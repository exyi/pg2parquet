use std::{marker::PhantomData, fs::File, io::Write, cell::RefCell, sync::Arc};

use parquet::{file::writer::{SerializedColumnWriter, SerializedRowGroupWriter}, errors::ParquetError};
use postgres::types::FromSql;

use crate::{column_appender::*, level_index::*, pg_custom_types::PgAbstractRow};

pub type Arcell<T> = Arc<RefCell<T>>;

pub trait DynamicSerializedWriter {
	fn next_column(&mut self, callback: &mut dyn FnMut(SerializedColumnWriter<'_>) -> ()) -> parquet::errors::Result<bool>;
}

struct DynamicSerializedWriterImpl<'a, W: Write> {
	writer: Arcell<Option<SerializedRowGroupWriter<'a, W>>>
}
impl<'a, 'b, W: Write> DynamicSerializedWriter for DynamicSerializedWriterImpl<'a, W> {
	fn next_column(&mut self, callback: &mut dyn FnMut(SerializedColumnWriter<'_>) -> ()) -> parquet::errors::Result<bool> {
		let mut writer = self.writer.borrow_mut();
		let writer2 = writer.as_mut().unwrap();
		let col = writer2.next_column()?;
		match col {
			None => Ok(false),
			Some(col) => {
				callback(col);
				Ok(true)
			}
		}
	}
}

pub fn new_dynamic_serialized_writer<'a, W: Write>(hovno: Arcell<Option<SerializedRowGroupWriter<'a, W>>>) -> Box<dyn DynamicSerializedWriter + 'a> {
	Box::new(DynamicSerializedWriterImpl::<'a, W> { writer: hovno })
}


pub trait ColumnCopier<TReader> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: &TReader) -> Result<usize, String>;
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String>;

	// fn write_columns<'b, W: Write>(&mut self, writer: &mut SerializedRowGroupWriter<'b, W>) -> Result<(), ParquetError> where Self: Sized;
	fn write_columns<'b>(&mut self, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String>;
	// fn write_columns_file<'b>(&mut self, writer: &mut SerializedRowGroupWriter<'b, File>) -> Result<(), ParquetError> where Self: Sized;
}

pub fn copier_write_columns<'a, 'b: 'a, W: Write, TReader, C: ColumnCopier<TReader>>(
	s: &mut C, writer: Arcell<Option<SerializedRowGroupWriter<'a, W>>>
) -> Result<(), String> {
	let mut dynamic_writer = new_dynamic_serialized_writer(writer);
	s.write_columns(dynamic_writer.as_mut())
}


pub struct BasicPgColumnCopier<TPg, TAppender>
	where TAppender: ColumnAppender<TPg> {
	column_i: usize,
	column: TAppender,
	_dummy: PhantomData<TPg>
}

impl<TPg, TAppender> BasicPgColumnCopier<TPg, TAppender>
	where TAppender: ColumnAppender<TPg> {
	pub fn new(column_i: usize, appender: TAppender) -> Self {
		BasicPgColumnCopier {
			column_i,
			column: appender,
			_dummy: PhantomData
		}
	}
}

impl<TPg, TAppender, TRow: PgAbstractRow> ColumnCopier<TRow> for BasicPgColumnCopier<TPg, TAppender>
	where TPg: for<'a> FromSql<'a>, TAppender: ColumnAppender<TPg> {

	fn copy_value<'a>(&mut self, repetition_index: &LevelIndexList, reader: &'a TRow) -> Result<usize, String> {
		debug_assert_eq!(repetition_index.level, self.column.max_rl());

		let v = reader.ab_get::<Option<TPg>>(self.column_i);

		self.column.copy_value_opt(repetition_index, v)
	}

	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		debug_assert_eq!(repetition_index.level, self.column.max_rl());
		self.column.write_null(repetition_index, level)
	}

	fn write_columns<'b>(&mut self, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		self.column.write_columns(self.column_i, next_col)
	}
}

pub struct MergedColumnCopier<TReader> {
	columns: Vec<Box<dyn ColumnCopier<TReader>>>,
	max_dl: i16,
	max_rl: i16
}

impl<'a, TReader> MergedColumnCopier<TReader> {
	pub fn new(columns: Vec<Box<dyn ColumnCopier<TReader>>>, max_dl: i16, max_rl: i16) -> Self {
		MergedColumnCopier { columns, max_dl, max_rl }
	}
}

impl<'a, TReader> ColumnCopier<TReader> for MergedColumnCopier<TReader> {

	fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: &TReader) -> Result<usize, String> {
		let mut total = 0;
		for c in self.columns.iter_mut() {
			total += c.copy_value(repetition_index, reader)?;
		}
		Ok(total)
	}

	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		let mut total = 0;
		for c in self.columns.iter_mut() {
			total += c.write_null(repetition_index, level)?;
		}
		Ok(total)
	}

	fn write_columns<'b>(&mut self, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		for c in self.columns.iter_mut() {
			c.write_columns(next_col)?;
		}
		Ok(())
	}
}

impl<TReader> Clone for MergedColumnCopier<TReader> {
	fn clone(&self) -> Self {
		todo!();
		// MergedColumnCopier {
		// 	columns: self.columns.iter().map(|c| c.clone()).collect()
		// }
	}
}

impl<TReader> ColumnAppender<TReader> for MergedColumnCopier<TReader> {
	fn write_columns(&mut self, _column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		ColumnCopier::write_columns(self, next_col)
	}

	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: TReader) -> Result<usize, String> {
		ColumnCopier::copy_value(self, repetition_index, &value)
    }

	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		ColumnCopier::write_null(self, repetition_index, level)
    }

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }
}
