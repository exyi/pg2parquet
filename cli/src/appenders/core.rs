use std::{borrow::Cow, sync::Arc, cell::RefCell, io::Write};

use parquet::file::writer::{SerializedColumnWriter, SerializedRowGroupWriter};

use crate::level_index::LevelIndexList;

pub trait ColumnAppenderBase {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String>;

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String>;

	fn max_dl(&self) -> i16;
	fn max_rl(&self) -> i16;
}

pub trait ColumnAppender<TPg: Clone>: ColumnAppenderBase {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String>;
	fn copy_value_opt(&mut self, repetition_index: &LevelIndexList, value: Cow<Option<TPg>>) -> Result<usize, String> {
		match value {
			Cow::Owned(Some(value)) => self.copy_value(repetition_index, Cow::<TPg>::Owned(value)),
			Cow::Borrowed(Some(value)) => self.copy_value(repetition_index, Cow::Borrowed(value)),
			Cow::Owned(None) | Cow::Borrowed(None) => {
				assert_ne!(self.max_dl(), 0);
				self.write_null(repetition_index, self.max_dl() - 1)
			},
		}
	}
}

pub type DynColumnAppender<T> = Box<dyn ColumnAppender<T>>;

impl<T> ColumnAppenderBase for DynColumnAppender<T> {
    fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
        self.as_mut().write_null(repetition_index, level)
    }

    fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
        self.as_mut().write_columns(column_i, next_col)
    }

    fn max_dl(&self) -> i16 {
        self.as_ref().max_dl()
    }

    fn max_rl(&self) -> i16 {
        self.as_ref().max_rl()
    }
}

impl<T: Clone> ColumnAppender<T> for DynColumnAppender<T> {
    fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<T>) -> Result<usize, String> {
        self.as_mut().copy_value(repetition_index, value)
    }
}

pub type Arcell<T> = Arc<RefCell<T>>;

/// Helper trait for ColumnAppender to allow dynamic dispatch of creating new columns
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

pub fn new_dynamic_serialized_writer<'a, W: Write>(writer: Arcell<Option<SerializedRowGroupWriter<'a, W>>>) -> Box<dyn DynamicSerializedWriter + 'a> {
	Box::new(DynamicSerializedWriterImpl::<'a, W> { writer })
}
