//! Arrow-based parallel parquet writer using column-level parallelism.
//!
//! This module wires parquet::arrow::arrow_writer::ArrowColumnWriter to enable
//! parallel column serialization for a single output parquet file. Each leaf
//! column has one worker; row groups are written sequentially.

use std::io::Write;
use std::sync::Arc;

use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{SchemaDescriptor, TypePtr};

use crossbeam_channel::{bounded, Sender};
use std::thread::JoinHandle;

use crate::parquet_writer::{WriterSettings, WriterStats};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use parquet::arrow::arrow_writer::{
	compute_leaves, get_column_writers, ArrowColumnChunk, ArrowLeafColumn,
};

type ColSender = Sender<ArrowLeafColumn>;
struct RowGroup {
	senders: Vec<ColSender>,
	handles: Vec<JoinHandle<Result<ArrowColumnChunk, String>>>,
	rows: usize,
}

/// Spawn per-column parallel serialization tasks for a row group and return their handles and senders.
fn spawn_column_parallel_row_group_writer(
	arrow_schema: Arc<ArrowSchema>,
	parquet_root: TypePtr,
	writer_props: WriterPropertiesPtr,
	channel_buffer: usize,
) -> Result<RowGroup, String> {
	let schema_desc = SchemaDescriptor::new(parquet_root);
	let col_writers = get_column_writers(&schema_desc, &writer_props, &arrow_schema)
		.map_err(|e| format!("Failed to get column writers: {}", e))?;

	let mut handles = Vec::with_capacity(col_writers.len());
	let mut senders = Vec::with_capacity(col_writers.len());
	for mut writer in col_writers {
		let (tx, rx) = bounded::<ArrowLeafColumn>(channel_buffer);
		senders.push(tx);
		handles.push(std::thread::spawn(move || {
			for col in rx {
				writer.write(&col).map_err(|e| format!("Column write failed: {e}"))?;
			}
			// Closing may encode and compress buffered pages too.
			writer.close().map_err(|e| format!("Closing column writer failed: {e}"))
		}));
	}

	Ok(RowGroup { handles, senders, rows: 0 })
}

/// Send arrays from a RecordBatch to their corresponding column writer channels.
fn send_arrays_to_col_writers(
	col_array_channels: &[ColSender],
	rb: &RecordBatch,
	arrow_schema: &Arc<ArrowSchema>,
) -> Result<(), String> {
	// Compute all leaf columns across all top-level fields in order
	let mut all_leaves = Vec::new();
	for (array, field) in rb.columns().iter().zip(arrow_schema.fields()) {
		all_leaves.extend(compute_leaves(field, array)
			.map_err(|e| format!("compute_leaves failed: {}", e))?);
	}
	if all_leaves.len() != col_array_channels.len() {
		return Err(format!(
			"Leaf/channel count mismatch: leaves={}, channels={}",
			all_leaves.len(),
			col_array_channels.len()
		));
	}
	for (tx, leaf) in col_array_channels.iter().zip(all_leaves) {
		tx.send(leaf)
			.map_err(|_| "Failed to send array to column writer".to_string())?;
	}
	Ok(())
}

/// A writer that serializes Arrow RecordBatches into a Parquet file with column-level parallelism.
pub struct ArrowParallelWriter<W: Write + Send> {
	file_writer: SerializedFileWriter<W>,
	arrow_schema: Arc<ArrowSchema>,
	parquet_schema: TypePtr,
	writer_props: WriterPropertiesPtr,
	settings: WriterSettings,

	row_group: Option<RowGroup>,

	// stats
	stats: WriterStats,
}

impl<W: Write + Send> ArrowParallelWriter<W> {
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
			row_group: None,
			stats: WriterStats::default(),
		}
	}

	/// Ensure per-column writer tasks are spawned for the current row group.
	fn ensure_row_group(&mut self) -> Result<(), String> {
		if self.row_group.is_none() {
			self.row_group = Some(spawn_column_parallel_row_group_writer(
				self.arrow_schema.clone(),
				self.parquet_schema.clone(),
				self.writer_props.clone(),
				8, // small bounded buffer per column
			)?);
		}
		Ok(())
	}

	/// Write a RecordBatch into the current row group, spawning column-level parallelism.
	pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), String> {
		assert_eq!(batch.num_columns(), self.arrow_schema.fields().len());

		self.ensure_row_group()?;
		let row_group = self.row_group.as_mut().unwrap();
		send_arrays_to_col_writers(
			&row_group.senders,
			batch,
			&self.arrow_schema,
		)?;

		row_group.rows += batch.num_rows();
		self.stats.rows += batch.num_rows();

		// flush if row group row limit reached
		if row_group.rows >= self.settings.row_group_row_limit {
			self.flush_row_group()?;
		}
		Ok(())
	}

	/// Finish the current row group and append to the parquet file.
	pub fn flush_row_group(&mut self) -> Result<(), String> {
		let Some(row_group) = self.row_group.take() else {
			return Ok(());
		};
		// close senders to signal end-of-stream
		drop(row_group.senders);
		let results: Vec<_> = row_group.handles.into_iter().enumerate().map(|(i, handle)|
			handle.join().map_err(|_| format!("Column thread {i} panicked!"))?
		).collect();
		let chunks = results.into_iter().collect::<Result<Vec<_>, _>>()?;

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
		Ok(())
	}

	/// Close writer and return stats.
	pub fn close(mut self) -> Result<WriterStats, String> {
		self.flush_row_group()?;
		self.file_writer
			.close()
			.map_err(|e| format!("Closing file failed: {}", e))?;
		Ok(self.stats)
	}
}
