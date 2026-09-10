//! Arrow-based parallel parquet writer using column-level parallelism.
//!
//! This module wires parquet::arrow::arrow_writer::ArrowColumnWriter to enable
//! true parallel column serialization for a single output parquet file.
//!
//! It is designed to be fed Arrow RecordBatches. Integration from Postgres rows
//! to Arrow is a follow-up step.

use std::io::Write;
use std::sync::Arc;

use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{SchemaDescriptor, TypePtr};

use crossbeam_channel::{bounded, Receiver, Sender};
use std::thread::JoinHandle;

use crate::parquet_writer::{WriterSettings, WriterStats};

// Arrow imports (arrow 56 splits crates; the `arrow` meta crate re-exports)
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use parquet::arrow::arrow_writer::{
	compute_leaves, get_column_writers, ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn,
};

type ColSender = Sender<ArrowLeafColumn>;
type ColumnJoinHandle = JoinHandle<Result<ArrowColumnWriter, String>>;

/// Consumes ArrowLeafColumn units from a channel and serializes them using an ArrowColumnWriter.
fn column_serializer_task(rx: Receiver<ArrowLeafColumn>, mut writer: ArrowColumnWriter) -> Result<ArrowColumnWriter, String> {
	for col in rx.iter() {
		writer.write(&col).map_err(|e| format!("Column write failed: {}", e))?;
	}
	Ok(writer)
}

/// Spawn per-column parallel serialization tasks for a row group and return their handles and senders.
fn spawn_column_parallel_row_group_writer(
	arrow_schema: Arc<ArrowSchema>,
	parquet_root: TypePtr,
	writer_props: WriterPropertiesPtr,
	channel_buffer: usize,
) -> Result<(Vec<ColumnJoinHandle>, Vec<ColSender>), String> {
	let schema_desc = SchemaDescriptor::new(parquet_root);
	let col_writers = get_column_writers(&schema_desc, &writer_props, &arrow_schema)
		.map_err(|e| format!("Failed to get column writers: {}", e))?;
	// Debug: number of leaf writers

	let mut handles = Vec::with_capacity(col_writers.len());
	let mut senders = Vec::with_capacity(col_writers.len());
	for writer in col_writers.into_iter() {
		let (tx, rx) = bounded::<ArrowLeafColumn>(channel_buffer);
		senders.push(tx);
		handles.push(std::thread::spawn(move || column_serializer_task(rx, writer)));
	}

	Ok((handles, senders))
}

/// Send arrays from a RecordBatch to their corresponding column writer channels.
fn send_arrays_to_col_writers(
	col_array_channels: &[ColSender],
	rb: &RecordBatch,
	arrow_schema: &Arc<ArrowSchema>,
) -> Result<(), String> {
	// Compute all leaf columns across all top-level fields in order
	let mut all_leaves = Vec::new();
	let mut per_field_counts: Vec<usize> = Vec::with_capacity(rb.num_columns());
	for (array, field) in rb.columns().iter().zip(arrow_schema.fields()) {
		let mut leaves = compute_leaves(field, array)
			.map_err(|e| format!("compute_leaves failed: {}", e))?;
		per_field_counts.push(leaves.len());
		all_leaves.append(&mut leaves);
	}
	// Debug: per-field leaf counts can be useful during schema mismatches
	if all_leaves.len() != col_array_channels.len() {
		return Err(format!(
			"Leaf/channel count mismatch: leaves={}, channels={}",
			all_leaves.len(),
			col_array_channels.len()
		));
	}
	for (tx, leaf) in col_array_channels.iter().zip(all_leaves.into_iter()) {
		tx.send(leaf)
			.map_err(|_| "Failed to send array to column writer".to_string())?;
	}
	Ok(())
}

/// Close all column writer tasks and collect encoded chunks.
fn finalize_column_writers(
	mut handles: Vec<ColumnJoinHandle>,
) -> Result<Vec<ArrowColumnChunk>, String> {
	let mut chunks = Vec::with_capacity(handles.len());
	for handle in handles.drain(..) {
		let writer = handle.join().map_err(|_| "Column thread panicked".to_string())??;
		let chunk = writer
			.close()
			.map_err(|e| format!("Closing column writer failed: {}", e))?;
		chunks.push(chunk);
	}
	Ok(chunks)
}

/// A writer that serializes Arrow RecordBatches into a Parquet file with column-level parallelism.
pub struct ArrowParallelWriter<W: Write + Send> {
	file_writer: SerializedFileWriter<W>,
	arrow_schema: Arc<ArrowSchema>,
	parquet_schema: TypePtr,
	writer_props: WriterPropertiesPtr,
	settings: WriterSettings,

	// current row group state
	cur_rowgroup_rows: usize,
	col_handles: Option<Vec<ColumnJoinHandle>>,
	col_senders: Option<Vec<ColSender>>,

	// stats
	stats: WriterStats,
}

impl<W: Write + Send + 'static> ArrowParallelWriter<W> {
	pub fn new(
		file_writer: SerializedFileWriter<W>,
		arrow_schema: Arc<ArrowSchema>,
		parquet_schema: TypePtr,
		writer_props: WriterPropertiesPtr,
		settings: WriterSettings,
	) -> Self {
		Self {
			file_writer,
			arrow_schema,
			parquet_schema,
			writer_props,
			settings,
			cur_rowgroup_rows: 0,
			col_handles: None,
			col_senders: None,
			stats: WriterStats::default(),
		}
	}

	/// Ensure per-column writer tasks are spawned for the current row group.
	fn ensure_row_group(&mut self) -> Result<(), String> {
		if self.col_handles.is_none() {
			let (handles, senders) = spawn_column_parallel_row_group_writer(
				self.arrow_schema.clone(),
				self.parquet_schema.clone(),
				self.writer_props.clone(),
				8, // small bounded buffer per column
			)?;
			self.col_handles = Some(handles);
			self.col_senders = Some(senders);
		}
		Ok(())
	}

	/// Write a RecordBatch into the current row group, spawning column-level parallelism.
	pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), String> {
		assert_eq!(batch.num_columns(), self.arrow_schema.fields().len());

		self.ensure_row_group()?;
		send_arrays_to_col_writers(
			self.col_senders.as_ref().unwrap(),
			batch,
			&self.arrow_schema,
		)?;

		self.cur_rowgroup_rows += batch.num_rows();
		self.stats.rows += batch.num_rows();

		// flush if row group row limit reached
		if self.cur_rowgroup_rows >= self.settings.row_group_row_limit {
			self.flush_row_group()?;
		}
		Ok(())
	}

	/// Finish the current row group and append to the parquet file.
	pub fn flush_row_group(&mut self) -> Result<(), String> {
		if self.col_handles.is_none() {
			return Ok(());
		}

		// close senders to signal end-of-stream
		if let Some(senders) = self.col_senders.take() {
			drop(senders);
		}

		// collect chunks
		let chunks = finalize_column_writers(self.col_handles.take().unwrap())?;

		// append chunks to a new row group
		let mut rg = self
			.file_writer
			.next_row_group()
			.map_err(|e| format!("Creating row group failed: {}", e))?;
		for chunk in chunks {
			chunk
				.append_to_row_group(&mut rg)
				.map_err(|e| format!("Appending column chunk failed: {}", e))?;
		}
		let md = rg
			.close()
			.map_err(|e| format!("Closing row group failed: {}", e))?;
		self.stats.groups += 1;
		self.stats.bytes_out += md.compressed_size() as usize;
		self.cur_rowgroup_rows = 0;
		Ok(())
	}

	/// Close writer and return stats.
	pub fn close(mut self) -> Result<WriterStats, String> {
		// finalize any open row group
		if self.col_handles.is_some() {
			self.flush_row_group()?;
		}
		self.file_writer
			.close()
			.map_err(|e| format!("Closing file failed: {}", e))?;
		Ok(self.stats)
	}
}

/// Helper for constructing ArrowParallelWriter from an existing SerializedFileWriter and Arrow schema.
pub fn create_arrow_parallel_writer<W: Write + Send + 'static>(
	file_writer: SerializedFileWriter<W>,
	arrow_schema: Arc<ArrowSchema>,
	parquet_schema: TypePtr,
	writer_props: WriterPropertiesPtr,
	settings: WriterSettings,
) -> ArrowParallelWriter<W> {
	ArrowParallelWriter::new(file_writer, arrow_schema, parquet_schema, writer_props, settings)
}

