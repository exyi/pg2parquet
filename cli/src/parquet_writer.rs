use std::{borrow::Cow, cell::RefCell, fmt::Display, io::Write, mem, os, rc::Rc, sync::Arc, usize};

use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};

use crate::{level_index::LevelIndexList, postgresutils::identify_row, pg_custom_types::PgAbstractRow, appenders::{new_dynamic_serialized_writer, Arcell, DynColumnAppender}};


#[derive(Debug, Clone, Default)]
pub struct WriterStats {
	pub rows: usize,
	pub bytes: usize,
	pub bytes_out: usize,
	pub groups: usize
}

#[derive(Debug, Clone)]
pub struct WriterSettings {
	pub row_group_byte_limit: usize,
	pub row_group_row_limit: usize
}

pub struct ParquetRowWriter<W: Write + Send> {
	writer: SerializedFileWriter<W>,
	schema: parquet::schema::types::TypePtr,
	// row_group_writer: SerializedRowGroupWriter<'a, W>,
	appender: DynColumnAppender<Arc<postgres::Row>>,
	stats: WriterStats,
	last_timestep_stats: WriterStats,
	last_timestep_time: std::time::Instant,
	start_time: std::time::Instant,
	last_print_time: std::time::Instant,
	quiet: bool,
	settings: WriterSettings,
	current_group_bytes: usize,
	current_group_rows: usize
}

impl <W: Write + Send> ParquetRowWriter<W> {
	pub fn new(
		writer: SerializedFileWriter<W>,
		schema: parquet::schema::types::TypePtr,
		appender: DynColumnAppender<Arc<postgres::Row>>,
		quiet: bool,
		settings: WriterSettings
	) -> parquet::errors::Result<Self> {
		// let mut row_group_writer = writer.next_row_group()?;
		let start_time = std::time::Instant::now();
		Ok(ParquetRowWriter {
			writer,
			schema,
			// row_group_writer,
			appender,
			stats: WriterStats::default(),
			last_timestep_stats: WriterStats::default(),
			last_timestep_time: start_time,
			last_print_time: start_time,
			start_time,
			quiet,
			settings,
			current_group_bytes: 0,
			current_group_rows: 0
		})
	}

	fn flush_group(&mut self) -> Result<(), String> {
		let row_group_writer = self.writer.next_row_group().map_err(|e| format!("Error creating row group: {}", e))?;
		let row_group_writer: Arcell<_> = Arc::new(RefCell::new(Some(row_group_writer)));
		let mut dyn_writer = new_dynamic_serialized_writer(row_group_writer.clone());

		self.appender.write_columns(0, dyn_writer.as_mut())?;

		mem::drop(dyn_writer);
		let hack123 = RefCell::new(None);
		row_group_writer.swap(&hack123);
		let hack1234 = hack123.into_inner().unwrap();
		let metadata = hack1234.close().map_err(|e| format!("Error closing row group: {}", e))?;

		self.stats.groups += 1;
		self.stats.bytes_out += metadata.compressed_size() as usize;
		self.current_group_bytes = 0;
		self.current_group_rows = 0;

		Ok(())
	}

	pub fn write_row(&mut self, row: Arc<postgres::Row>) -> Result<(), String> {
		let lvl = LevelIndexList::new_i(self.stats.rows);
		let bytes = self.appender.copy_value(&lvl, Cow::Borrowed(&row))
			.map_err(|e| format!("Could not copy Row[{}]:", identify_row(&row)) + &e)?;

		self.current_group_bytes += bytes;
		self.current_group_rows += 1;
		self.stats.bytes += bytes;
		self.stats.rows += 1;

		if self.current_group_bytes >= self.settings.row_group_byte_limit || self.current_group_rows >= self.settings.row_group_row_limit {
			self.flush_group()?;
		}

		if !self.quiet && self.stats.rows % 256 == 0 {
			self.print_stats(false);
		}

		Ok(())
	}

	pub fn print_stats(&mut self, summary: bool) {
		fn format_number<T: Display>(n: T) -> String {
			let mut result = format!("{}", n);
			// let mut last_index = result.len() - 1;
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

		eprint!("[{}:{:02}:{:02}.{:03}]: {} rows, {} MiB raw, {} MiB parquet, {} groups | {:} rows/s, {:} MiB/s                 ",
			total_elapsed.as_secs() / 3600,
			total_elapsed.as_secs() / 60 % 60,
			total_elapsed.as_secs() % 60,
			total_elapsed.as_millis() % 1000,
			format_number(self.stats.rows),
			format_number(self.stats.bytes / 1024 / 1024),
			format_number(self.stats.bytes_out / 1024 / 1024),
			format_number(self.stats.groups),
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

	pub fn get_stats(&mut self) -> WriterStats { self.stats.clone() }

	pub fn close(mut self) -> Result<WriterStats, String> {
		self.flush_group().map_err(|e| e)?;

		self.print_stats(true);

		// self.row_group_writer.close().map_err(|e| e.to_string())?;
		self.writer.close().map_err(|e| e.to_string())?;

		Ok(self.stats)
	}
}
