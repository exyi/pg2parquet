//! Arrow-based parallel parquet writer using column-level parallelism.
//!
//! This module wires parquet::arrow::arrow_writer::ArrowColumnWriter to enable
//! parallel column serialization for a single output parquet file. Each leaf
//! column has one task; row groups are written sequentially.

use std::io::Write;
use std::sync::Arc;

use parquet::file::writer::SerializedFileWriter;

use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;

use crate::parquet_writer::{WriterSettings, WriterStats};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use parquet::arrow::arrow_writer::{
	compute_leaves, ArrowColumnChunk, ArrowLeafColumn, ArrowRowGroupWriterFactory,
};

type ColSender = Sender<ArrowLeafColumn>;
struct RowGroup {
	senders: Vec<ColSender>,
	handles: Vec<JoinHandle<Result<ArrowColumnChunk, String>>>,
	rows: usize,
	bytes: usize,
}

/// Spawn per-column parallel serialization tasks for a row group and return their handles and senders.
fn spawn_column_parallel_row_group_writer<W: Write + Send>(
	file_writer: &SerializedFileWriter<W>,
	arrow_schema: Arc<ArrowSchema>,
	channel_buffer: usize,
) -> Result<RowGroup, String> {
	let row_group_index = file_writer.flushed_row_groups().len();
	let col_writers = ArrowRowGroupWriterFactory::new(file_writer, arrow_schema)
		.create_column_writers(row_group_index)
		.map_err(|e| format!("Failed to get column writers: {}", e))?;

	let mut handles = Vec::with_capacity(col_writers.len());
	let mut senders = Vec::with_capacity(col_writers.len());
	for mut writer in col_writers {
		let (tx, mut rx) = channel::<ArrowLeafColumn>(channel_buffer);
		senders.push(tx);
		handles.push(tokio::spawn(async move {
			while let Some(col) = rx.recv().await {
				writer.write(&col).map_err(|e| format!("Column write failed: {e}"))?;
			}
			// Closing may encode and compress buffered pages too.
			writer.close().map_err(|e| format!("Closing column writer failed: {e}"))
		}));
	}

	Ok(RowGroup { handles, senders, rows: 0, bytes: 0 })
}

/// Send arrays from a RecordBatch to their corresponding column writer channels.
async fn send_arrays_to_col_writers(
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
		tx.send(leaf).await
			.map_err(|_| "Failed to send array to column writer".to_string())?;
	}
	Ok(())
}

/// A writer that serializes Arrow RecordBatches into a Parquet file with column-level parallelism.
pub struct ArrowParallelWriter<W: Write + Send> {
	file_writer: SerializedFileWriter<W>,
	arrow_schema: Arc<ArrowSchema>,
	settings: WriterSettings,

	row_group: Option<RowGroup>,

	// stats
	stats: WriterStats,
}

impl<W: Write + Send> ArrowParallelWriter<W> {
	pub fn new(
		file_writer: SerializedFileWriter<W>,
		arrow_schema: Arc<ArrowSchema>,
		settings: WriterSettings,
	) -> Self {
		Self {
			file_writer,
			arrow_schema,
			settings,
			row_group: None,
			stats: WriterStats::default(),
		}
	}

	/// Ensure per-column writer tasks are spawned for the current row group.
	fn ensure_row_group(&mut self) -> Result<(), String> {
		if self.row_group.is_none() {
			self.row_group = Some(spawn_column_parallel_row_group_writer(
				&self.file_writer,
				self.arrow_schema.clone(),
				8, // small bounded buffer per column
			)?);
		}
		Ok(())
	}

	/// Write a RecordBatch into the current row group, spawning column-level parallelism.
	pub async fn write_batch(&mut self, batch: &RecordBatch, batch_bytes: usize) -> Result<(), String> {
		assert_eq!(batch.num_columns(), self.arrow_schema.fields().len());

		self.ensure_row_group()?;
		let new_row_group = self.row_group.as_ref().is_some_and(|row_group| {
			row_group.rows > 0 && (
				row_group.rows + batch.num_rows() > self.settings.row_group_row_limit ||
				row_group.bytes + batch_bytes > self.settings.row_group_byte_limit
			)
		});
		if new_row_group {
			self.flush_row_group().await?;
			self.ensure_row_group()?;
		}

		let row_group = self.row_group.as_mut().unwrap();
		send_arrays_to_col_writers(
			&row_group.senders,
			batch,
			&self.arrow_schema,
		).await?;

		row_group.rows += batch.num_rows();
		row_group.bytes += batch_bytes;
		self.stats.rows += batch.num_rows();
		self.stats.bytes += batch_bytes as u64;

		Ok(())
	}

	/// Finish the current row group and append to the parquet file.
	pub async fn flush_row_group(&mut self) -> Result<(), String> {
		let Some(row_group) = self.row_group.take() else {
			return Ok(());
		};
		// close senders to signal end-of-stream
		drop(row_group.senders);
		let mut chunks = Vec::with_capacity(row_group.handles.len());
		for (i, handle) in row_group.handles.into_iter().enumerate() {
			chunks.push(handle.await
				.map_err(|e| format!("Column task {i} failed: {e}"))??);
		}

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
	pub async fn close(mut self) -> Result<WriterStats, String> {
		self.flush_row_group().await?;
		self.file_writer
			.close()
			.map_err(|e| format!("Closing file failed: {}", e))?;
		Ok(self.stats)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::{ArrayRef, Int32Array};
	use arrow::datatypes::{DataType, Field};
	use parquet::file::properties::WriterProperties;
	use parquet::schema::parser::parse_message_type;
	use std::io::Cursor;

	#[tokio::test]
	async fn byte_limit_starts_a_new_row_group_before_it_is_exceeded() {
		let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new("value", DataType::Int32, false)]));
		let parquet_schema = Arc::new(parse_message_type("message schema { REQUIRED INT32 value; }").unwrap());
		let props = Arc::new(WriterProperties::builder().build());
		let file_writer = SerializedFileWriter::new(
			Cursor::new(Vec::new()),
			parquet_schema.clone(),
			props.clone(),
		).unwrap();
		let mut writer = ArrowParallelWriter::new(
			file_writer,
			arrow_schema.clone(),
			WriterSettings::new(15, usize::MAX),
		);

		for values in [[1, 2], [3, 4]] {
			let batch = RecordBatch::try_new(
				arrow_schema.clone(),
				vec![Arc::new(Int32Array::from(values.to_vec())) as ArrayRef],
			).unwrap();
			writer.write_batch(&batch, 10).await.unwrap();
		}

		let stats = writer.close().await.unwrap();
		assert_eq!(stats.rows, 4);
		assert_eq!(stats.bytes, 20);
		assert_eq!(stats.groups, 2);
	}
}
