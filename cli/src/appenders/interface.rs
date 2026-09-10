use std::borrow::Cow;
// Arrow exports: allow appenders to materialize Arrow arrays for parallel compression path
use arrow::array::ArrayRef;
use arrow::datatypes::DataType as ArrowDataType;

use crate::level_index::LevelIndexList;

pub trait ColumnAppenderBase {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String>;

	fn max_dl(&self) -> i16;
	fn max_rl(&self) -> i16;

	// Export this column as an Arrow Array matching the target type.
	// Default implementation returns an error; concrete appenders should override when supported.
	fn to_arrow_array(&mut self, _target: &ArrowDataType) -> Result<ArrayRef, String>;

	// No-op by default; some appenders may ignore Arrow entirely.

	// Optional: clear buffered values after a flush
	fn clear(&mut self) {}
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

    fn max_dl(&self) -> i16 {
        self.as_ref().max_dl()
    }

    fn max_rl(&self) -> i16 {
        self.as_ref().max_rl()
    }

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		self.as_mut().to_arrow_array(target)
	}
}

impl<T: Clone> ColumnAppender<T> for DynColumnAppender<T> {
    fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<T>) -> Result<usize, String> {
        self.as_mut().copy_value(repetition_index, value)
    }
}
