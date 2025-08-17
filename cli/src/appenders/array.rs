use std::{marker::PhantomData, borrow::Cow};

use crate::level_index::LevelIndexList;

use super::{ColumnAppender, ColumnAppenderBase, DynamicSerializedWriter};

pub struct ArrayColumnAppender<TPg: Clone, TInner>
	where TInner: ColumnAppender<TPg> {
	inner: TInner,
	dl: i16,
	rl: i16,
	allow_null: bool,
	allow_element_null: bool,
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

		ArrayColumnAppender { inner, allow_null, allow_element_null, dl, rl, _dummy: PhantomData }
	}
}

impl<TPg: Clone, TInner> ColumnAppenderBase for ArrayColumnAppender<TPg, TInner> 
	where TInner: ColumnAppender<TPg> {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		assert!(level <= self.dl);

		let nested_ri = repetition_index.new_child();
		self.inner.write_null(&nested_ri, level)
	}

	fn max_dl(&self) -> i16 { self.dl }
	fn max_rl(&self) -> i16 {
		debug_assert!(self.inner.max_rl() > 0);
		self.inner.max_rl() - 1
	}

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		self.inner.write_columns(column_i, next_col)
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
			// empty array is written as null at DL=1
			bytes_written += self.inner.write_null(&nested_ri, self.dl)?;
		}
		Ok(bytes_written)
	}

	fn copy_value_opt(&mut self, repetition_index: &LevelIndexList, value: Cow<Option<TArray>>) -> Result<usize, String> {
		match value {
			Cow::Owned(Some(value)) => self.copy_value(repetition_index, Cow::<TArray>::Owned(value)),
			Cow::Borrowed(Some(value)) => self.copy_value(repetition_index, Cow::Borrowed(value)),
			Cow::Owned(None) | Cow::Borrowed(None) => {
				// if !self.allow_null, this writes an empty array
				let nested_ri = repetition_index.new_child();
				self.inner.write_null(&nested_ri, self.dl - self.allow_null as i16)
			},
		}
	}
}
