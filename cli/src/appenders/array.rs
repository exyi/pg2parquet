use std::{marker::PhantomData, borrow::Cow};

use crate::level_index::LevelIndexList;

use super::{ColumnAppender, ColumnAppenderBase};
// Arrow support for exporting LIST arrays via inner appender
use std::sync::Arc;
use arrow::array::{ArrayRef, ListArray, ListBuilder, NullBufferBuilder, OffsetBufferBuilder};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::buffer::{OffsetBuffer, NullBuffer};
// Use arrow's compute concat to avoid bringing in extra crates
use arrow::compute::concat as arrow_concat;

pub struct ArrayColumnAppender<TPg: Clone, TInner>
	where TInner: ColumnAppender<TPg> {
	inner: TInner,
	dl: i16,
	rl: i16,
	allow_null: bool,
	allow_element_null: bool,
	// Track per-list metadata using Arrow builders
	offsets_builder: OffsetBufferBuilder<i32>,
	valid_builder: NullBufferBuilder,
	_dummy: PhantomData<TPg>,
	// dummy2: PhantomData<TPg>,
}

impl<TPg: Clone, TInner> ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	pub fn new(inner: TInner, allow_null: bool, allow_element_null: bool, dl: i16, rl: i16) -> Self {
		if inner.max_rl() != rl + 1 {
			panic!("Cannot create {}, repetition levels {} must be one less than inner repetition levels {}", std::any::type_name::<Self>(), rl, inner.max_rl());
		}
		if inner.max_dl() != dl + 1 + allow_element_null as i16 {
			panic!("Cannot create {}, definition levels {} must be {} less than inner definition levels {}", std::any::type_name::<Self>(), dl, 1 + allow_element_null as i16, inner.max_dl());
		}
		if dl < allow_null as i16 {
			panic!("Cannot create {}, definition levels {} must be positive", std::any::type_name::<Self>(), dl);
		}
		if rl < 0 {
			panic!("Cannot create {}, repetition levels {} must be positive", std::any::type_name::<Self>(), rl);
		}

		ArrayColumnAppender {
			inner, allow_null, allow_element_null, dl, rl,
			offsets_builder: OffsetBufferBuilder::new(1024),
			valid_builder: NullBufferBuilder::new(1024),
			_dummy: PhantomData
		}
	}
}

impl<TPg: Clone, TInner> ColumnAppenderBase for ArrayColumnAppender<TPg, TInner> 
	where TInner: ColumnAppender<TPg> {
	fn write_null(&mut self, _repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		assert!(level <= self.dl);
		self.offsets_builder.push_length(0);
		self.valid_builder.append_null();
		Ok(4)
	}

	fn max_dl(&self) -> i16 { self.dl }
	fn max_rl(&self) -> i16 {
		debug_assert!(self.inner.max_rl() > 0);
		self.inner.max_rl() - 1
	}

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		// println!("Getting Arrow array from {} {}: {target}", std::any::type_name::<TPg>(), std::any::type_name::<TInner>());
		let ArrowDataType::List(child_field_ref) = target else {
			return Err(panic!("Arrow list export: unexpected target type {target:?}"))
		};
		let child_arr = self.inner.to_arrow_array(child_field_ref.data_type())?;
		let rows = self.valid_builder.len();
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
	const IS_NULLABLE: bool;
	fn is_null(&self) -> bool;
	fn as_option(self) -> Option<T>;
}

impl<T> Nullable<T> for Option<T> {
	const IS_NULLABLE: bool = true;
	fn is_null(&self) -> bool { self.is_none() }
	fn as_option(self) -> Option<T> { self }
}

impl<T> Nullable<T> for T {
	const IS_NULLABLE: bool = false;
	fn is_null(&self) -> bool { false }
	fn as_option(self) -> Option<T> { Some(self) }
}

impl<'a, TPg: Clone, TInner, TArray: Clone, TItem> ColumnAppender<TArray> for ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg>,
		  TArray: IntoIterator<Item = TItem> + Clone,
		  TItem: Nullable<TPg> {

	fn copy_value(&mut self, repetition_index: &LevelIndexList, array: Cow<TArray>) -> Result<usize, String> {
		let mut bytes_written = 0;

		let mut nested_ri = repetition_index.new_child();

		for value in array.into_owned().into_iter() { // TODO: avoid copying
			if TItem::IS_NULLABLE && self.allow_element_null {
				bytes_written += self.inner.copy_value_opt(&nested_ri, Cow::Owned(value.as_option()))?;
				nested_ri.inc();
			} else {
				match value.as_option() {
					Some(value) => {
						bytes_written += self.inner.copy_value(&nested_ri, Cow::Owned(value))?;
						nested_ri.inc();
					},
					None => { debug_assert!(false) }// skip
				}
			}
		}

		self.offsets_builder.push_length(nested_ri.index as usize);
		self.valid_builder.append_non_null();
		Ok(bytes_written)
	}

	fn copy_value_opt(&mut self, repetition_index: &LevelIndexList, value: Cow<Option<TArray>>) -> Result<usize, String> {
		match value {
			Cow::Owned(Some(value)) => self.copy_value(repetition_index, Cow::<TArray>::Owned(value)),
			Cow::Borrowed(Some(value)) => self.copy_value(repetition_index, Cow::Borrowed(value)),
			Cow::Owned(None) | Cow::Borrowed(None) => {
				self.offsets_builder.push_length(0);
				self.valid_builder.append_null();
				Ok(4)
			},
		}
	}
}
