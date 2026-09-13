use std::borrow::Cow;
use arrow::array::ArrayRef;

/// Appends return an approximate buffered byte count, including validity overhead.
/// Nulls may also be placeholders beneath a null parent struct.
pub trait ColumnAppenderBase {
	fn write_null(&mut self) -> Result<usize, String>;
	/// Finishes the buffered batch and resets the appender for reuse.
	fn to_arrow_array(&mut self) -> Result<ArrayRef, String>;
}

pub trait ColumnAppender<TPg: Clone>: ColumnAppenderBase {
	fn copy_value(&mut self, value: Cow<TPg>) -> Result<usize, String>;
	fn copy_value_opt(&mut self, value: Cow<Option<TPg>>) -> Result<usize, String> {
		match value {
			Cow::Owned(Some(value)) => self.copy_value(Cow::<TPg>::Owned(value)),
			Cow::Borrowed(Some(value)) => self.copy_value(Cow::Borrowed(value)),
			Cow::Owned(None) | Cow::Borrowed(None) => {
				self.write_null()
			},
		}
	}
}

pub type DynColumnAppender<T> = Box<dyn ColumnAppender<T>>;

impl<T> ColumnAppenderBase for DynColumnAppender<T> {
    fn write_null(&mut self) -> Result<usize, String> {
        self.as_mut().write_null()
    }

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		self.as_mut().to_arrow_array()
	}
}

impl<T: Clone> ColumnAppender<T> for DynColumnAppender<T> {
    fn copy_value(&mut self, value: Cow<T>) -> Result<usize, String> {
        self.as_mut().copy_value(value)
    }
}
