use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use arrow::{array::{ArrayRef, BinaryArray, BinaryBuilder, BinaryDictionaryBuilder, DictionaryArray, GenericBinaryArray, Int8Builder, Int16BufferBuilder, Int16Builder, StringDictionaryBuilder, UInt16Builder, UInt16DictionaryArray}, datatypes::{ByteArrayType, DataType, Int32Type, UInt32Type}};

use crate::{appenders::{ColumnAppender, ColumnAppenderBase}, level_index::LevelIndexList};

pub struct DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	max_dl: i16,
	max_rl: i16,
	builder: BinaryDictionaryBuilder<UInt32Type>,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		Self {
			max_dl,
			max_rl,
			builder: BinaryDictionaryBuilder::new(),
			conversion,
			_pg: PhantomData,
		}
	}
}

impl<TPg: Clone, FConversion> ColumnAppenderBase for DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	fn write_null(&mut self, _repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &arrow::datatypes::DataType) -> Result<ArrayRef, String> {
		let array = Arc::new(self.builder.finish()) as ArrayRef;
		Ok(array)
	}
}

impl<TPg: Clone, FConversion> ColumnAppender<TPg> for DictionaryColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	fn copy_value(&mut self, _repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let value_ref = value.as_ref();
		let bytes = (self.conversion)(value_ref);
		let byte_size = bytes.len();
		self.builder.append_value(bytes);
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}



pub struct SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	max_dl: i16,
	max_rl: i16,
	keys_array: ArrayRef,
	builder: UInt16Builder,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	pub fn new(max_dl: i16, max_rl: i16, keys: Vec<Vec<u8>>, conversion: FConversion) -> Self {
		let mut keys_arr = BinaryBuilder::new();
		for k in &keys {
			keys_arr.append_value(k);
		}
		Self {
			max_dl,
			max_rl,
			keys_array: Arc::new(keys_arr.finish()),
			builder: UInt16Builder::new(),
			conversion,
			_pg: PhantomData,
		}
	}
}

impl<TPg: Clone, FConversion> ColumnAppenderBase for SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	fn write_null(&mut self, _repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &arrow::datatypes::DataType) -> Result<ArrayRef, String> {
		let indexes = self.builder.finish();
		let array = UInt16DictionaryArray::new(indexes, self.keys_array.clone());
		Ok(Arc::new(array))
	}
}

impl<TPg: Clone, FConversion> ColumnAppender<TPg> for SmallEnumColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> u16,
{
	fn copy_value(&mut self, _repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let value_ref = value.as_ref();
		let case = (self.conversion)(value_ref);
		self.builder.append_value(case);
		Ok(2 + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

