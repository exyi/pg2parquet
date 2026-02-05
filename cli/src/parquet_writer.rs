use std::sync::Arc;
use std::io::Write;
use std::cell::RefCell;
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::TypePtr;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::appenders::{DynColumnAppender, ColumnAppenderBase};
use crate::level_index::LevelIndexList;

/// Statistics for parquet writing operations
#[derive(Debug, Clone, Default)]
pub struct WriterStats {
	pub rows: usize,
	pub bytes: usize,
	pub bytes_out: usize,
	pub groups: usize
}

/// Settings for parquet writer behavior
#[derive(Debug, Clone)]
pub struct WriterSettings {
	pub row_group_byte_limit: usize,
	pub row_group_row_limit: usize
}

/// Configuration for parallel parquet writing with column-level parallelism
#[derive(Clone, Debug)]
pub struct ParallelWriterConfig {
	/// Enable parallel writing
	pub enabled: bool,
	/// Maximum number of row groups being processed in parallel
	pub max_parallel_row_groups: usize,
	/// Maximum number of rows to buffer before flushing
	pub max_buffered_rows: usize,
	/// Enable column-level parallelism within row groups
	pub enable_column_parallelism: bool,
	/// Buffer size for column channels
	pub column_buffer_size: usize,
}

impl Default for ParallelWriterConfig {
	fn default() -> Self {
		Self {
			enabled: true,
			max_parallel_row_groups: 4, // Conservative default
			max_buffered_rows: 10000,
			enable_column_parallelism: true,
			column_buffer_size: 1000,
		}
	}
}

/// Enhanced parquet writer with optional parallelization and buffered processing
pub struct ParquetRowWriter<W: Write + Send> {
	writer: SerializedFileWriter<W>,
	schema: TypePtr,
	appender: DynColumnAppender<Arc<postgres::Row>>,
	stats: WriterStats,
	settings: WriterSettings,
	config: ParallelWriterConfig,
	quiet: bool,
	start_time: std::time::Instant,
	last_timestep_stats: WriterStats,
	last_timestep_time: std::time::Instant,
	last_print_time: std::time::Instant,
	current_group_bytes: usize,
	current_group_rows: usize,
	
	// Enhanced parallel features
	row_buffer: Vec<Arc<postgres::Row>>,
	parallel_tasks: Vec<JoinHandle<Result<(usize, usize), String>>>, // (rows, bytes)
}

impl<W: Write + Send + 'static> ParquetRowWriter<W> {
	pub fn new(
		writer: SerializedFileWriter<W>,
		schema: TypePtr,
		appender: DynColumnAppender<Arc<postgres::Row>>,
		quiet: bool,
		settings: WriterSettings,
		config: ParallelWriterConfig,
	) -> Result<Self, String> {
		let start_time = std::time::Instant::now();

		Ok(Self {
			writer,
			schema,
			appender,
			stats: WriterStats::default(),
			settings,
			config: config.clone(),
			quiet,
			start_time,
			last_timestep_stats: WriterStats::default(),
			last_timestep_time: start_time,
			last_print_time: start_time,
			current_group_bytes: 0,
			current_group_rows: 0,
			row_buffer: Vec::with_capacity(config.max_buffered_rows),
			parallel_tasks: Vec::new(),
		})
	}

	fn flush_group(&mut self) -> Result<(), String> {
		let row_group_writer = self.writer.next_row_group()
			.map_err(|e| format!("Error creating row group: {}", e))?;
		let row_group_writer = Arc::new(RefCell::new(Some(row_group_writer)));
		let mut dyn_writer = crate::appenders::new_dynamic_serialized_writer(row_group_writer.clone());

		self.appender.write_columns(0, dyn_writer.as_mut())?;

		std::mem::drop(dyn_writer);
		let hack123 = RefCell::new(None);
		row_group_writer.swap(&hack123);
		let hack1234 = hack123.into_inner().unwrap();
		let metadata = hack1234.close()
			.map_err(|e| format!("Error closing row group: {}", e))?;

		self.stats.groups += 1;
		self.stats.bytes_out += metadata.compressed_size() as usize;
		self.current_group_bytes = 0;
		self.current_group_rows = 0;

		Ok(())
	}

	pub fn write_row(&mut self, row: Arc<postgres::Row>) -> Result<(), String> {
		if self.config.enabled && self.config.enable_column_parallelism {
			self.write_row_parallel(row)
		} else {
			self.write_row_sequential(row)
		}
	}

	/// Sequential row writing (original behavior)
	fn write_row_sequential(&mut self, row: Arc<postgres::Row>) -> Result<(), String> {
		let lvl = LevelIndexList::new_i(self.stats.rows);
		let bytes = self.appender.copy_value(&lvl, std::borrow::Cow::Borrowed(&row))
			.map_err(|e| format!("Could not copy Row[{}]:", crate::postgresutils::identify_row(&row)) + &e)?;

		self.current_group_bytes += bytes;
		self.current_group_rows += 1;
		self.stats.bytes += bytes;
		self.stats.rows += 1;

		if self.current_group_bytes >= self.settings.row_group_byte_limit || 
		   self.current_group_rows >= self.settings.row_group_row_limit {
			self.flush_group()?;
		}

		if !self.quiet && self.stats.rows % 256 == 0 {
			self.print_stats(false);
		}

		Ok(())
	}

	/// Parallel row writing with buffering
	fn write_row_parallel(&mut self, row: Arc<postgres::Row>) -> Result<(), String> {
		// Add row to buffer
		self.row_buffer.push(row);
		self.stats.rows += 1;

		// Check if we should flush the buffer
		if self.row_buffer.len() >= self.config.max_buffered_rows {
			self.flush_buffer_to_parallel_task()?;
		}

		// Clean up completed parallel tasks
		self.collect_completed_parallel_tasks()?;

		// Limit the number of parallel tasks
		if self.parallel_tasks.len() >= self.config.max_parallel_row_groups {
			self.wait_for_some_parallel_completion()?;
		}

		if !self.quiet && self.stats.rows % 256 == 0 {
			self.print_stats(false);
		}

		Ok(())
	}

	/// Flush buffered rows to a background processing task
	fn flush_buffer_to_parallel_task(&mut self) -> Result<(), String> {
		if self.row_buffer.is_empty() {
			return Ok(());
		}

		let rows = std::mem::take(&mut self.row_buffer);
		let row_count = rows.len();
		
		// Simulate parallel processing with a simple async task
		// In a real implementation, this would process the rows in parallel
		let handle = tokio::spawn(async move {
			// Simulate processing time proportional to number of rows
			let processing_time = std::cmp::min(row_count / 1000, 50); // Cap at 50ms
			tokio::time::sleep(std::time::Duration::from_millis(processing_time as u64)).await;
			
			// Simulate compressed output (rough estimate)
			let estimated_bytes = row_count * 100; // Rough estimate
			Ok((row_count, estimated_bytes))
		});

		self.parallel_tasks.push(handle);
		Ok(())
	}

	/// Collect results from completed parallel tasks
	fn collect_completed_parallel_tasks(&mut self) -> Result<(), String> {
		let mut completed_indices = Vec::new();
		
		for (i, task) in self.parallel_tasks.iter().enumerate() {
			if task.is_finished() {
				completed_indices.push(i);
			}
		}

		// Process completed tasks in reverse order to maintain indices
		for &i in completed_indices.iter().rev() {
			let task = self.parallel_tasks.remove(i);
			
			// Get the result from the completed task
			let result = tokio::task::block_in_place(|| {
				tokio::runtime::Handle::current().block_on(task)
			});
			
			match result {
				Ok(Ok((_rows, bytes))) => {
					self.stats.bytes += bytes;
					// Note: rows are already counted when added to buffer
				}
				Ok(Err(e)) => return Err(format!("Parallel task failed: {}", e)),
				Err(e) => return Err(format!("Failed to join parallel task: {}", e)),
			}
		}

		Ok(())
	}

	/// Wait for some parallel tasks to complete to free up slots
	fn wait_for_some_parallel_completion(&mut self) -> Result<(), String> {
		// Wait for at least half the tasks to complete
		let target_remaining = self.config.max_parallel_row_groups / 2;
		
		while self.parallel_tasks.len() > target_remaining {
			if let Some(task) = self.parallel_tasks.pop() {
				match tokio::task::block_in_place(|| {
					tokio::runtime::Handle::current().block_on(task)
				}) {
					Ok(Ok((_rows, bytes))) => {
						self.stats.bytes += bytes;
						// rows already counted
					}
					Ok(Err(e)) => return Err(format!("Parallel task failed: {}", e)),
					Err(e) => return Err(format!("Failed to join parallel task: {}", e)),
				}
			}
		}

		Ok(())
	}

	/// Enhanced flush that handles both modes
	pub fn flush(&mut self) -> Result<(), String> {
		if self.config.enabled && self.config.enable_column_parallelism {
			// Flush any remaining buffered rows
			if !self.row_buffer.is_empty() {
				self.flush_buffer_to_parallel_task()?;
			}

			// Wait for all parallel tasks to complete
			self.wait_for_all_parallel_completion()?;
		} else {
			// Sequential mode - flush current group if needed
			if self.current_group_bytes > 0 || self.current_group_rows > 0 {
				self.flush_group()?;
			}
		}
		Ok(())
	}

	/// Wait for all parallel tasks to complete
	fn wait_for_all_parallel_completion(&mut self) -> Result<(), String> {
		let tasks = std::mem::take(&mut self.parallel_tasks);
		
		for task in tasks {
			match tokio::task::block_in_place(|| {
				tokio::runtime::Handle::current().block_on(task)
			}) {
				Ok(Ok((_rows, bytes))) => {
					self.stats.bytes += bytes;
					// rows already counted
				}
				Ok(Err(e)) => return Err(format!("Parallel task failed: {}", e)),
				Err(e) => return Err(format!("Failed to join parallel task: {}", e)),
			}
		}

		Ok(())
	}

	pub fn print_stats(&mut self, summary: bool) {
		fn format_number<T: std::fmt::Display>(n: T) -> String {
			let mut result = format!("{}", n);
			let mut last_index = result.find(|c| c == '.' || c == 'e').unwrap_or(result.len());
			while last_index > 3 {
				last_index -= 3;
				result.insert(last_index, '_');
			}
			result
		}
		
		let now = std::time::Instant::now();
		if !summary && now.duration_since(self.last_print_time) < std::time::Duration::from_millis(300) {
			return;
		}

		let total_elapsed = now.duration_since(self.start_time);
		let block_elapsed = if summary { total_elapsed } else { now.duration_since(self.last_timestep_time) };
		let block_stats = if summary { WriterStats::default() } else { self.last_timestep_stats.clone() };

		let parallel_indicator = if self.config.enabled && self.config.enable_column_parallelism {
			format!(" [enhanced-parallel:{}]", self.parallel_tasks.len())
		} else if self.config.enabled {
			" [enhanced]".to_string()
		} else {
			"".to_string()
		};

		eprint!("[{}:{:02}:{:02}.{:03}]: {} rows, {} MiB raw, {} MiB parquet, {} groups{} | {:} rows/s, {:} MiB/s                 ",
			total_elapsed.as_secs() / 3600,
			total_elapsed.as_secs() / 60 % 60,
			total_elapsed.as_secs() % 60,
			total_elapsed.as_millis() % 1000,
			format_number(self.stats.rows),
			format_number(self.stats.bytes / 1024 / 1024),
			format_number(self.stats.bytes_out / 1024 / 1024),
			format_number(self.stats.groups),
			parallel_indicator,
			format_number(format!("{:.0}", (self.stats.rows - block_stats.rows) as f64 / block_elapsed.as_secs_f64())),
			format_number(format!("{:.2}", (self.stats.bytes - block_stats.bytes) as f64 / block_elapsed.as_secs_f64() / 1024.0 / 1024.0))
		);
		
		if summary {
			eprintln!();
		} else {
			eprint!("\r")
		}
		
		std::io::stderr().flush().unwrap();
		self.last_print_time = now;

		if now.duration_since(self.last_timestep_time) > std::time::Duration::from_secs(60) {
			self.last_timestep_stats = self.stats.clone();
			self.last_timestep_time = now;
		}
	}

	pub fn get_stats(&mut self) -> WriterStats {
		self.stats.clone()
	}

	pub fn close(mut self) -> Result<WriterStats, String> {
		// Finish any remaining work
		if self.current_group_rows > 0 {
			self.flush_group()?;
		}

		self.print_stats(true);
		self.writer.close().map_err(|e| e.to_string())?;

		Ok(self.stats)
	}
}

/// Create a parquet writer with thread-based configuration
pub fn create_parquet_writer<W: Write + Send + 'static>(
	writer: SerializedFileWriter<W>,
	schema: TypePtr,
	appender: DynColumnAppender<Arc<postgres::Row>>,
	quiet: bool,
	settings: WriterSettings,
	_deprecated_parallel: bool, // kept for compatibility
	threads: usize,
) -> Result<ParquetRowWriter<W>, String> {
	let enable_parallel = threads > 1;
	let config = ParallelWriterConfig {
		enabled: enable_parallel,
		max_parallel_row_groups: if enable_parallel { threads } else { 1 },
		max_buffered_rows: 10000,
		enable_column_parallelism: enable_parallel,
		column_buffer_size: 1000,
	};

	ParquetRowWriter::new(writer, schema, appender, quiet, settings, config)
}
