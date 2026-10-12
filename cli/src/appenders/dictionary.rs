use std::{marker::PhantomData, sync::Arc};
use arrow::{array::{ArrayBuilder, ArrayRef, BinaryBuilder, BinaryDictionaryBuilder, UInt16Builder, UInt16DictionaryArray}, datatypes::UInt32Type};
use super::{ColumnAppender, ColumnAppenderBase};

pub struct DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	builder: BinaryDictionaryBuilder<UInt32Type>,
	max_key: Option<u32>,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	pub fn new(conversion: FConversion) -> Self {
		Self {
			builder: BinaryDictionaryBuilder::new(),
			conversion,
			max_key: None,
			_pg: PhantomData,
		}
	}
}

impl<TPg, FConversion> ColumnAppenderBase for DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	fn write_null(&mut self) -> Result<usize, String> {
		self.builder.append_null();
		Ok(4)
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		let array = Arc::new(self.builder.finish()) as ArrayRef;
		self.max_key = None;
		Ok(array)
	}
}

impl<TPg, FConversion> ColumnAppender<TPg> for DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	fn copy_value(&mut self, value: &TPg) -> Result<usize, String> {
		let bytes = (self.conversion)(value);
		let byte_size = bytes.len();
		let ix = self.builder.append(bytes).unwrap();
		if self.max_key.is_none_or(|max_key| ix > max_key) {
			self.max_key = Some(ix);
			Ok(byte_size + 4 + 4)
		} else {
			Ok(4)
		}
	}
}

pub struct SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	keys_array: ArrayRef,
	builder: UInt16Builder,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	pub fn new(keys: Vec<Vec<u8>>, conversion: FConversion) -> Self {
		let mut keys_arr = BinaryBuilder::new();
		for k in &keys {
			keys_arr.append_value(k);
		}
		Self {
			keys_array: Arc::new(keys_arr.finish()),
			builder: UInt16Builder::new(),
			conversion,
			_pg: PhantomData,
		}
	}
}

impl<TPg, FConversion> ColumnAppenderBase for SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	fn write_null(&mut self) -> Result<usize, String> {
		self.builder.append_null();
		Ok(2)
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		let indexes = self.builder.finish();
		let array = UInt16DictionaryArray::new(indexes, self.keys_array.clone());
		Ok(Arc::new(array))
	}
}

impl<TPg, FConversion> ColumnAppender<TPg> for SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	fn copy_value(&mut self, value: &TPg) -> Result<usize, String> {
		let case = (self.conversion)(value);
		self.builder.append_value(case);
		Ok(2)
	}
}
