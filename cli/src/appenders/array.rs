use std::{marker::PhantomData, borrow::Cow};

use crate::level_index::LevelIndexList;

use super::{ColumnAppender, ColumnAppenderBase};
// Arrow support for exporting LIST arrays via inner appender
use std::sync::Arc;
use arrow::array::{ArrayRef, ListArray, OffsetBufferBuilder, NullBufferBuilder};
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

		ArrayColumnAppender { inner, allow_null, allow_element_null, dl, rl, offsets_builder: OffsetBufferBuilder::new(0), valid_builder: NullBufferBuilder::new(0), _dummy: PhantomData }
	}
}

impl<TPg: Clone, TInner> ColumnAppenderBase for ArrayColumnAppender<TPg, TInner> 
	where TInner: ColumnAppender<TPg> {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		assert!(level <= self.dl);

		// We may get called at two interesting levels:
		// - level == self.dl: explicit null list row => record a null list
		// - level == self.dl - 1: parent was null, but we must still record a row to keep alignment
		if level == self.dl {
			// Null list row
			self.offsets_builder.push_length(0usize);
			self.valid_builder.append_null();
			if !self.allow_element_null { return Ok(0); }
		} else if level == self.dl - 1 {
			// Parent container is null. Record a placeholder list row.
			self.offsets_builder.push_length(0usize);
			// If this list itself is nullable, mark invalid; otherwise treat as valid empty list.
			if self.allow_null { self.valid_builder.append_null(); } else { self.valid_builder.append_non_null(); }
			if !self.allow_element_null { return Ok(0); }
		}

		let nested_ri = repetition_index.new_child();
		self.inner.write_null(&nested_ri, level)
	}

	fn max_dl(&self) -> i16 { self.dl }
	fn max_rl(&self) -> i16 {
		debug_assert!(self.inner.max_rl() > 0);
		self.inner.max_rl() - 1
	}

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		// Only support flat LIST where inner has max_rl = self.max_rl()+1 and we can export its values array.
		// We expect target to be List(field) or LargeList(field) matching the inner element type.
		// We currently do not handle nested lists-of-lists specially; rely on inner.
		let (child_field_ref, is_large) = match target {
			ArrowDataType::List(f) => (f.clone(), false),
			ArrowDataType::LargeList(f) => (f.clone(), true),
			_ => return Err("Arrow LIST export: unsupported target type".to_string()),
		};
		if is_large { return Err("LargeList is not supported".to_string()); }
		// We need value array and offsets. Our inner appender currently buffers values with dls/rls.
		// If inner cannot produce Arrow, bail out and let caller fallback.
		let child_arr = self.inner.to_arrow_array(child_field_ref.data_type())?;
		let rows = self.valid_builder.len();
		// Finish builders to get final buffers
		let offsets_buf: OffsetBuffer<i32> = self.offsets_builder.finish_cloned();
		let list_nulls: Option<NullBuffer> = if self.allow_null { self.valid_builder.finish() } else { None };

		// Next, slice out only the spans of real elements from the original child array and concatenate them.
		// Track the running position inside the original child array including any sentinels (only when elements are nullable).
		let mut pos: i64 = 0;
		let mut slices: Vec<ArrayRef> = Vec::new();
		let offs: &[i32] = offsets_buf.as_ref();
		for i in 0..rows {
			let len_i = (offs[i+1] - offs[i]) as usize;
			let is_valid = match &list_nulls { Some(nb) => nb.is_valid(i), None => true };
			if !is_valid || len_i == 0 {
				// For null/empty lists, advance past a sentinel only when inner allowed element nulls
				if self.allow_element_null { pos += 1; }
			} else {
				// take the contiguous span of length `len` starting at current pos
				let slice = child_arr.slice(pos as usize, len_i);
				slices.push(slice);
				pos += len_i as i64;
			}
		}
		// Build compact values array
		let values: ArrayRef = if slices.is_empty() {
			// construct an empty array of the correct type by slicing zero
			child_arr.slice(0, 0)
		} else {
			let mut slice_refs: Vec<&dyn arrow::array::Array> = Vec::with_capacity(slices.len());
			for s in slices.iter() { slice_refs.push(s.as_ref()); }
			arrow_concat(&slice_refs).map_err(|e| e.to_string())?
		};
		// Construct ListArray
		let list = ListArray::try_new(child_field_ref, offsets_buf, values, list_nulls)
			.map_err(|e| e.to_string())?;

		// Reset builders/state for next batch
		self.offsets_builder = OffsetBufferBuilder::new(0);
		self.valid_builder = NullBufferBuilder::new(0);

		Ok(Arc::new(list) as ArrayRef)
	}
}

pub trait Nullable<T> {
	const IS_NULLABLE: bool;
	fn as_option(self) -> Option<T>;
}

impl<T> Nullable<T> for Option<T> {
	const IS_NULLABLE: bool = true;
	fn as_option(self) -> Option<T> { self }
}

impl<T> Nullable<T> for T {
	const IS_NULLABLE: bool = false;
	fn as_option(self) -> Option<T> { Some(self) }
}

impl<'a, TPg: Clone, TInner, TArray: Clone, TItem> ColumnAppender<TArray> for ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg>,
		  TArray: IntoIterator<Item = TItem> + Clone,
		  TItem: Nullable<TPg> {

	fn copy_value(&mut self, repetition_index: &LevelIndexList, array: Cow<TArray>) -> Result<usize, String> {
		let mut bytes_written = 0;

		let mut nested_ri = repetition_index.new_child();

		for (_index, value) in array.into_owned().into_iter().enumerate() {
			if TItem::IS_NULLABLE && self.allow_element_null {
				bytes_written += self.inner.copy_value_opt(&nested_ri, Cow::Owned(value.as_option()))?;
				nested_ri.inc();
			} else {
				match value.as_option() {
					Some(value) => {
						bytes_written += self.inner.copy_value(&nested_ri, Cow::Owned(value))?;
						nested_ri.inc();
					},
					None => { }// skip
				}
			}
		}

		if nested_ri.index == 0 {
			if self.allow_element_null {
				bytes_written += self.inner.write_null(&nested_ri, self.dl)?;
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
				// Null at this list level: if list is nullable => record null; otherwise treat as valid empty list
				let nested_ri = repetition_index.new_child();
				self.offsets_builder.push_length(0usize);
				if self.allow_null {
					self.valid_builder.append_null();
					if self.allow_element_null { self.inner.write_null(&nested_ri, self.dl - 1) } else { Ok(0) }
				} else {
					// required list cannot be null: emit as empty valid list without child sentinel
					self.valid_builder.append_non_null();
					Ok(0)
				}
			},
		}
	}
}
