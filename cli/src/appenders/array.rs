use std::marker::PhantomData;

use super::{ColumnAppender, ColumnAppenderBase};
// Arrow support for exporting LIST arrays via inner appender
use std::sync::Arc;
use arrow::array::{ArrayRef, ListArray, NullBufferBuilder, OffsetBufferBuilder};
use arrow::datatypes::Field as ArrowField;
use arrow::buffer::{OffsetBuffer, NullBuffer};

pub struct ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	inner: TInner,
	element_name: String,
	allow_null: bool,
	allow_element_null: bool,
	// Track per-list metadata using Arrow builders
	offsets_builder: OffsetBufferBuilder<i32>,
	valid_builder: NullBufferBuilder,
	_dummy: PhantomData<TPg>,
}

impl<TPg, TInner> ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	pub fn new(inner: TInner, allow_null: bool, allow_element_null: bool) -> Self {
		ArrayColumnAppender {
			inner, allow_null, allow_element_null, element_name: "element".into(),
			offsets_builder: OffsetBufferBuilder::new(1024),
			valid_builder: NullBufferBuilder::new(1024),
			_dummy: PhantomData
		}
	}
	pub fn with_element_name(mut self, name: impl Into<String>) -> Self {
		self.element_name = name.into();
		self
	}
}

impl<TPg, TInner> ColumnAppenderBase for ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	fn write_null(&mut self) -> Result<usize, String> {
		self.offsets_builder.push_length(0);
		self.valid_builder.append_null();
		Ok(4)
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		let child_arr = self.inner.to_arrow_array()?;
		let child_field_ref = Arc::new(ArrowField::new(&self.element_name, child_arr.data_type().clone(), self.allow_element_null));
		let list_nulls: Option<NullBuffer> = if self.allow_null { self.valid_builder.finish() } else { None };
		let offsets_buf: OffsetBuffer<i32> = self.offsets_builder.finish_cloned();
		let list = ListArray::try_new(child_field_ref.clone(), offsets_buf, child_arr, list_nulls)
			.map_err(|e| format!("ListArray::try_new({child_field_ref:?}) failed: {}", e.to_string()))?;

		self.offsets_builder = OffsetBufferBuilder::new(0);
		self.valid_builder = NullBufferBuilder::new(0);

		Ok(Arc::new(list) as ArrayRef)
	}
}

pub trait Nullable<T> {
	fn as_option(&self) -> Option<&T>;
}

impl<T> Nullable<T> for Option<T> {
	fn as_option(&self) -> Option<&T> { self.as_ref() }
}

impl<T> Nullable<T> for T {
	fn as_option(&self) -> Option<&T> { Some(self) }
}

impl<TPg, TInner, TArray, TItem> ColumnAppender<TArray> for ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg>,
		  for<'a> &'a TArray: IntoIterator<Item = &'a TItem>,
		  TItem: Nullable<TPg> {
	fn copy_value(&mut self, array: &TArray) -> Result<usize, String> {
		let mut bytes_written = 0;

		let mut element_count = 0;

		for value in array {
			match value.as_option() {
				Some(value) => bytes_written += self.inner.copy_value(value)?,
				None if self.allow_element_null => bytes_written += self.inner.write_null()?,
				None => {
					debug_assert!(false, "null element in a required list");
					continue;
				}
			}
			element_count += 1;
		}

		self.offsets_builder.push_length(element_count);
		self.valid_builder.append_non_null();
		Ok(bytes_written + 4)
	}
}
