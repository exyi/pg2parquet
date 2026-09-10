use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use arrow::array::{ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::DataType as ArrowDataType;

use parquet::basic;
use parquet::data_type::{DataType, ByteArray as PqByteArray, ByteArrayType, FixedLenByteArray as PqFixedLenByteArray, FixedLenByteArrayType};

use crate::{
	level_index::{LevelIndexList, LevelIndexState},
	myfrom::MyFrom,
};

use super::{real_memory_size::RealMemorySize, ColumnAppender, ColumnAppenderBase, DynColumnAppender};

// pub trait ArrowBuilderBridge: Sized {
// 	type Native: Copy + RealMemorySize;
//
// 	fn with_capacity(capacity: usize) -> Self;
// 	fn append_value(&mut self, value: Self::Native);
// 	fn append_null(&mut self);
// 	fn finish(self) -> ArrayRef;
// 	fn arrow_type() -> ArrowDataType;
// }
//
// macro_rules! impl_arrow_builder_bridge {
// 	($builder:ty, $native:ty, $arrow_dt:expr) => {
// 		impl ArrowBuilderBridge for $builder {
// 			type Native = $native;
//
// 			fn with_capacity(capacity: usize) -> Self {
// 				<$builder>::with_capacity(capacity)
// 			}
//
// 			fn append_value(&mut self, value: Self::Native) {
// 				self.append_value(value)
// 			}
//
// 			fn append_null(&mut self) {
// 				self.append_null()
// 			}
//
// 			fn finish(mut self) -> ArrayRef {
// 				Arc::new(<$builder>::finish(&mut self)) as ArrayRef
// 			}
//
// 			fn arrow_type() -> ArrowDataType {
// 				$arrow_dt
// 			}
// 		}
// 	};
// }

// impl_arrow_builder_bridge!(BooleanBuilder, bool, ArrowDataType::Boolean);
// impl_arrow_builder_bridge!(Int32Builder, i32, ArrowDataType::Int32);
// impl_arrow_builder_bridge!(Int64Builder, i64, ArrowDataType::Int64);
// impl_arrow_builder_bridge!(Float32Builder, f32, ArrowDataType::Float32);
// impl_arrow_builder_bridge!(Float64Builder, f64, ArrowDataType::Float64);

pub trait PrimitiveParquetType: DataType
	where Self::T: Clone + RealMemorySize
{
	type Builder: ArrayBuilder + Extend<Option<Self::T>> + Default;
}

impl PrimitiveParquetType for parquet::data_type::BoolType {
	type Builder = BooleanBuilder;
}
impl PrimitiveParquetType for parquet::data_type::Int32Type {
	type Builder = Int32Builder;
}
impl PrimitiveParquetType for parquet::data_type::Int64Type {
	type Builder = Int64Builder;
}
impl PrimitiveParquetType for parquet::data_type::FloatType {
	type Builder = Float32Builder;
}
impl PrimitiveParquetType for parquet::data_type::DoubleType {
	type Builder = Float64Builder;
}
impl PrimitiveParquetType for parquet::data_type::ByteArrayType {
	type Builder = BinaryBuilder;
}

pub struct PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	max_dl: i16,
	max_rl: i16,
	builder: TPq::Builder,
	repetition_index: LevelIndexState,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
	_pq: PhantomData<TPq>,
}

pub fn new_autoconv_generic_appender<TPg, TPq>(max_dl: i16, max_rl: i16) -> PrimitiveColumnAppender<TPg, TPq, impl Fn(TPg) -> TPq::T>
	where
		TPq: PrimitiveParquetType,
		TPq::T: Clone + RealMemorySize + MyFrom<TPg>,
{
	PrimitiveColumnAppender::new(max_dl, max_rl, |value: TPg| MyFrom::my_from(value))
}

impl<TPg, TPq, FConversion> PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		PrimitiveColumnAppender {
			max_dl,
			max_rl,
			builder: TPq::Builder::default(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion,
			_pg: PhantomData,
			_pq: PhantomData,
		}
	}

	pub fn convert(&self, value: TPg) -> TPq::T {
		(self.conversion)(value)
	}
}

impl<TPg, TPq, FConversion> ColumnAppenderBase for PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	fn write_null(&mut self, _repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		if self.max_dl == 0 {
			return Err("Cannot write null to required column".to_string());
		}
		debug_assert!(level < self.max_dl);
		self.builder.extend([None]);
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 {
		self.max_dl
	}

	fn max_rl(&self) -> i16 {
		self.max_rl
	}

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		let array = self.builder.finish();
		if target != array.data_type() { // TODO: remove ArrowDataType
			return Err(format!(
				"Arrow export mismatch: expected {:?}, received {:?}",
				array.data_type(),
				target
			));
		}
		Ok(array)
	}
}

impl<TPg: Clone, TPq, FConversion> ColumnAppender<TPg> for PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	fn copy_value(&mut self, _repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let pq_value = self.convert(value.into_owned());
		let byte_size = pq_value.real_memory_size();
		self.builder.extend([Some(pq_value)]);
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

pub struct ByteArrayColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	max_dl: i16,
	max_rl: i16,
	builder: BinaryBuilder,
	repetition_index: LevelIndexState,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> ByteArrayColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		Self {
			max_dl,
			max_rl,
			builder: BinaryBuilder::new(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion,
			_pg: PhantomData,
		}
	}
}

impl<TPg: Clone, FConversion> ColumnAppenderBase for ByteArrayColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	fn write_null(&mut self, _repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		if self.max_dl == 0 {
			return Err("Cannot write null to required column".to_string());
		}
		debug_assert!(level < self.max_dl);
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		if target != &ArrowDataType::Binary {
			return Err("Arrow BYTE_ARRAY export: expected Binary".to_string());
		}
		let array = Arc::new(self.builder.finish()) as ArrayRef;
		Ok(array)
	}
}

impl<TPg: Clone, FConversion> ColumnAppender<TPg> for ByteArrayColumnAppender<TPg, FConversion>
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

pub struct FixedLenBinaryColumnAppender<const N: usize, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	max_dl: i16,
	max_rl: i16,
	builder: FixedSizeBinaryBuilder,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<const N: usize, TPg, FConversion> FixedLenBinaryColumnAppender<N, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		Self {
			max_dl,
			max_rl,
			builder: FixedSizeBinaryBuilder::new(N.try_into().unwrap()),
			conversion,
			_pg: PhantomData,
		}
	}
}

impl<const N: usize, TPg: Clone, FConversion> ColumnAppenderBase for FixedLenBinaryColumnAppender<N, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	fn write_null(&mut self, _repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		if self.max_dl == 0 {
			return Err("Cannot write null to required column".to_string());
		}
		// debug_assert_eq!(level, self.max_dl);
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		if target != &ArrowDataType::FixedSizeBinary(N as i32) {
			return Err("Arrow FIXED_LEN_BYTE_ARRAY export: expected FixedSizeBinary".to_string());
		}
		let array = Arc::new(self.builder.finish()) as ArrayRef;
		Ok(array)
	}
}

impl<const N: usize, TPg: Clone, FConversion> ColumnAppender<TPg> for FixedLenBinaryColumnAppender<N, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	fn copy_value(&mut self, _repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		match (self.conversion)(value.as_ref()) {
			None => self.builder.append_null(),
			Some(bytes) => self.builder.append_value(&bytes).unwrap()
		}
		Ok(N + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

pub fn new_autoconv_byte_array_binary_appender<TPg>(max_dl: i16, max_rl: i16) -> PrimitiveColumnAppender<TPg, ByteArrayType, impl Fn(TPg) -> PqByteArray>
	where
		TPg: Clone,
		PqByteArray: MyFrom<TPg>,
{
	PrimitiveColumnAppender::new(max_dl, max_rl, |value: TPg| MyFrom::my_from(value))
}
