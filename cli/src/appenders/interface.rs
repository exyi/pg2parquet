use arrow::array::ArrayRef;

/// Appends return an approximate buffered byte count, excluding validity bitmap bits.
/// Nulls may also be placeholders beneath a null parent struct.
pub trait ColumnAppenderBase {
	fn write_null(&mut self) -> Result<usize, String>;
	/// Finishes the buffered batch and resets the appender for reuse.
	fn to_arrow_array(&mut self) -> Result<ArrayRef, String>;
}

pub trait ColumnAppender<TPg: ?Sized>: ColumnAppenderBase {
	fn copy_value(&mut self, value: &TPg) -> Result<usize, String>;
	fn copy_value_opt(&mut self, value: &Option<TPg>) -> Result<usize, String>
	where
		TPg: Sized,
	{
		match value {
			Some(value) => self.copy_value(value),
			None => self.write_null(),
		}
	}
}

pub type DynColumnAppender<T> = Box<dyn ColumnAppender<T> + Send>;

impl<T: ?Sized> ColumnAppenderBase for DynColumnAppender<T> {
    fn write_null(&mut self) -> Result<usize, String> {
        self.as_mut().write_null()
    }

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		self.as_mut().to_arrow_array()
	}
}

impl<T: ?Sized> ColumnAppender<T> for DynColumnAppender<T> {
    fn copy_value(&mut self, value: &T) -> Result<usize, String> {
        self.as_mut().copy_value(value)
    }
}
