use std::{marker::PhantomData, fs::File, io::Write, cell::RefCell, sync::Arc, borrow::Cow};

use parquet::{file::writer::{SerializedColumnWriter, SerializedRowGroupWriter}, errors::ParquetError};
use postgres::types::FromSql;

use crate::{column_appender::*, level_index::*, pg_custom_types::PgAbstractRow, postgres_cloner::DynRowCopier};

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

pub fn copier_write_columns<'a, 'b: 'a, W: Write, TReader, C: ColumnAppender<TReader>>(
	s: &mut C, column_i: usize, writer: Arcell<Option<SerializedRowGroupWriter<'a, W>>>
) -> Result<(), String> {
	let mut dynamic_writer = new_dynamic_serialized_writer(writer);
	s.write_columns(column_i, dynamic_writer.as_mut())
}

pub struct BasicPgRowColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	column_i: usize,
	appender: TInner,
	_dummy: PhantomData<TPg>
}

impl<TPg, TInner> BasicPgRowColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	pub fn new(column_i: usize, appender: TInner) -> Self {
		BasicPgRowColumnAppender {
			column_i,
			appender,
			_dummy: PhantomData
		}
	}
}

impl<TPg, TInner> ColumnAppenderBase for BasicPgRowColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		// println!("write_null: level: {}, max_rl: {}", level, self.column.max_rl());
		debug_assert_eq!(repetition_index.level, self.appender.max_rl());
		self.appender.write_null(repetition_index, level)
	}

	fn write_columns(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		// debug_assert_eq!(column_i, self.column_i);
		self.appender.write_columns(column_i, next_col)
	}

	fn max_dl(&self) -> i16 { self.appender.max_dl() }

	fn max_rl(&self) -> i16 { self.appender.max_rl() }
}

impl<'b, TPg, TAppender, TRow: PgAbstractRow> ColumnAppender<Cow<'b, Arc<TRow>>> for BasicPgRowColumnAppender<TPg, TAppender>
	where TPg: for<'a> FromSql<'a>, TAppender: ColumnAppender<TPg> {

	fn copy_value<'a>(&mut self, repetition_index: &LevelIndexList, reader: Cow<'a, Arc<TRow>>) -> Result<usize, String> {
		debug_assert_eq!(repetition_index.level, self.appender.max_rl());

		let v = reader.ab_get::<Option<TPg>>(self.column_i);

		self.appender.copy_value_opt(repetition_index, v)
	}
}

pub struct MergedColumnAppender<TReader> {
	columns: Vec<DynRowCopier<TReader>>,
	max_dl: i16,
	max_rl: i16
}

impl<TReader> MergedColumnAppender<TReader> {
	pub fn new(columns: Vec<DynRowCopier<TReader>>, max_dl: i16, max_rl: i16) -> Self {
		MergedColumnAppender { columns, max_dl, max_rl }
	}
}

impl<TReader> ColumnAppenderBase for MergedColumnAppender<TReader> {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		let mut total = 0;
		for c in self.columns.iter_mut() {
			total += c.write_null(repetition_index, level)?;
		}
		Ok(total)
	}

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		for (i, c) in self.columns.iter_mut().enumerate() {
			c.write_columns(i, next_col)?;
		}
		Ok(())
	}

	fn max_dl(&self) -> i16 {
		self.max_dl
	}

	fn max_rl(&self) -> i16 {
		self.max_rl
	}
}

impl<'a, TReader> ColumnAppender<Cow<'a, Arc<TReader>>> for MergedColumnAppender<TReader> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: Cow<'a, Arc<TReader>>) -> Result<usize, String> {
		let mut total = 0;
		let reader_r = reader.as_ref();
		for c in self.columns.iter_mut() {
			total += c.copy_value(repetition_index, Cow::Borrowed(reader_r))?;
		}
		Ok(total)
	}
}
