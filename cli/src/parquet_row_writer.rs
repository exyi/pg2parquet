use std::{io::Write, cell::RefCell, sync::Arc, mem, borrow::Cow, rc::Rc};

use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};

use crate::{column_pg_copier::{new_dynamic_serialized_writer, Arcell}, level_index::LevelIndexList, postgresutils::identify_row, column_appender::DynColumnAppender, pg_custom_types::PgAbstractRow, postgres_cloner::DynRowCopier};


#[derive(Debug, Clone)]
pub struct WriterStats {
	pub rows: usize,
	pub bytes: usize,
	pub groups: usize
}

#[derive(Debug, Clone)]
pub struct WriterSettings {
	pub row_group_byte_limit: usize,
	pub row_group_row_limit: usize
}


pub trait ParquetRowWriter {
	fn write_row(&mut self, row: Arc<postgres::Row>) -> Result<(), String>;

	fn get_stats(&mut self) -> WriterStats;

	fn close(self) -> Result<WriterStats, String>;
}

pub struct ParquetRowWriterImpl<W: Write> {
	writer: SerializedFileWriter<W>,
	schema: parquet::schema::types::TypePtr,
	// row_group_writer: SerializedRowGroupWriter<'a, W>,
	copier: DynRowCopier<postgres::Row>,
	stats: WriterStats,
	settings: WriterSettings,
	current_group_bytes: usize,
	current_group_rows: usize
}

impl <W: Write> ParquetRowWriterImpl<W> {
	pub fn new(
		writer: SerializedFileWriter<W>,
		schema: parquet::schema::types::TypePtr,
		copier: DynRowCopier<postgres::Row>,
		settings: WriterSettings
	) -> parquet::errors::Result<Self> {
		// let mut row_group_writer = writer.next_row_group()?;
		Ok(ParquetRowWriterImpl {
			writer,
			schema,
			// row_group_writer,
			copier,
			stats: WriterStats { rows: 0, bytes: 0, groups: 0 },
			settings,
			current_group_bytes: 0,
			current_group_rows: 0
		})
	}

	fn flush_group(&mut self) -> Result<(), String> {
		let row_group_writer = self.writer.next_row_group().map_err(|e| format!("Error creating row group: {}", e))?;
		let row_group_writer: Arcell<_> = Arc::new(RefCell::new(Some(row_group_writer)));
		let mut dyn_writer = new_dynamic_serialized_writer(row_group_writer.clone());

		self.copier.write_columns(0, dyn_writer.as_mut())?;

		mem::drop(dyn_writer);
		let hack123 = RefCell::new(None);
		row_group_writer.swap(&hack123);
		let hack1234 = hack123.into_inner().unwrap();
		hack1234.close().map_err(|e| format!("Error closing row group: {}", e))?;

		self.stats.groups += 1;
		self.current_group_bytes = 0;
		self.current_group_rows = 0;

		Ok(())
	}
}

impl<W: Write> ParquetRowWriter for ParquetRowWriterImpl<W> {
	fn write_row(&mut self, row: Arc<postgres::Row>) -> Result<(), String> {
		let lvl = LevelIndexList::new_i(self.stats.rows);
		let bytes = self.copier.copy_value(&lvl, Cow::Borrowed(&row))
			.map_err(|e| format!("Could not copy Row[{}]:", identify_row(&row)) + &e)?;

		self.current_group_bytes += bytes;
		self.current_group_rows += 1;
		self.stats.bytes += bytes;
		self.stats.rows += 1;

		if self.current_group_bytes >= self.settings.row_group_byte_limit || self.current_group_rows >= self.settings.row_group_row_limit {
			self.flush_group()?;
		}

		Ok(())
	}

	fn get_stats(&mut self) -> WriterStats { self.stats.clone() }

	fn close(mut self) -> Result<WriterStats, String> {
		self.flush_group().map_err(|e| e)?;
		// self.row_group_writer.close().map_err(|e| e.to_string())?;
		self.writer.close().map_err(|e| e.to_string())?;

		Ok(self.stats)
	}
}
