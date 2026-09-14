use std::{marker::PhantomData, sync::Arc};
use arrow::array::{ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder};
use parquet::data_type::{DataType, ByteArray as PqByteArray, ByteArrayType};
use crate::myfrom::MyFrom;
use super::{real_memory_size::RealMemorySize, ColumnAppender, ColumnAppenderBase};

pub trait PrimitiveParquetType: DataType
	where Self::T: Clone + RealMemorySize
{
	type Builder: ArrayBuilder + Extend<Option<Self::T>> + Default;
	const NULL_VALUE_SIZE: usize;
	const VALUE_OVERHEAD: usize = 0;
}

impl PrimitiveParquetType for parquet::data_type::BoolType {
	type Builder = BooleanBuilder;
	const NULL_VALUE_SIZE: usize = 1;
}
impl PrimitiveParquetType for parquet::data_type::Int32Type {
	type Builder = Int32Builder;
	const NULL_VALUE_SIZE: usize = 4;
}
impl PrimitiveParquetType for parquet::data_type::Int64Type {
	type Builder = Int64Builder;
	const NULL_VALUE_SIZE: usize = 8;
}
impl PrimitiveParquetType for parquet::data_type::FloatType {
	type Builder = Float32Builder;
	const NULL_VALUE_SIZE: usize = 4;
}
impl PrimitiveParquetType for parquet::data_type::DoubleType {
	type Builder = Float64Builder;
	const NULL_VALUE_SIZE: usize = 8;
}
impl PrimitiveParquetType for parquet::data_type::ByteArrayType {
	type Builder = BinaryBuilder;
	const NULL_VALUE_SIZE: usize = 4;
	const VALUE_OVERHEAD: usize = 4;
}

pub struct PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(&TPg) -> TPq::T,
{
	builder: TPq::Builder,

	conversion: FConversion,
	_pg: PhantomData<TPg>,
	_pq: PhantomData<TPq>,
}

pub fn new_autoconv_generic_appender<TPg, TPq>() -> PrimitiveColumnAppender<TPg, TPq, impl Fn(&TPg) -> TPq::T>
	where
		TPg: Clone,
		TPq: PrimitiveParquetType,
		TPq::T: Clone + RealMemorySize + MyFrom<TPg>,
{
	PrimitiveColumnAppender::new(|value: &TPg| MyFrom::my_from((*value).clone()))
}

impl<TPg, TPq, FConversion> PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(&TPg) -> TPq::T,
{
	pub fn new(conversion: FConversion) -> Self {
		PrimitiveColumnAppender {
			builder: TPq::Builder::default(),

			conversion,
			_pg: PhantomData,
			_pq: PhantomData,
		}
	}

	pub fn convert(&self, value: &TPg) -> TPq::T {
		(self.conversion)(value)
	}
}

impl<TPg, TPq, FConversion> ColumnAppenderBase for PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(&TPg) -> TPq::T,
{
	fn write_null(&mut self) -> Result<usize, String> {
		self.builder.extend([None]);
		Ok(TPq::NULL_VALUE_SIZE)
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		let array = self.builder.finish();
		Ok(array)
	}
}

impl<TPg, TPq, FConversion> ColumnAppender<TPg> for PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: RealMemorySize,
		FConversion: Fn(&TPg) -> TPq::T,
{
	fn copy_value(&mut self, value: &TPg) -> Result<usize, String> {
		let pq_value = self.convert(value);
		let byte_size = pq_value.real_memory_size();
		self.builder.extend([Some(pq_value)]);
		Ok(byte_size + TPq::VALUE_OVERHEAD)
	}
}

pub struct ByteArrayColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	builder: BinaryBuilder,

	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> ByteArrayColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	pub fn new(conversion: FConversion) -> Self {
		Self {
			builder: BinaryBuilder::new(),

			conversion,
			_pg: PhantomData,
		}
	}
}

impl<TPg, FConversion> ColumnAppenderBase for ByteArrayColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	fn write_null(&mut self) -> Result<usize, String> {
		self.builder.append_null();
		Ok(4)
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		let array = Arc::new(self.builder.finish()) as ArrayRef;
		Ok(array)
	}
}

impl<TPg, FConversion> ColumnAppender<TPg> for ByteArrayColumnAppender<TPg, FConversion>
	where FConversion: for <'a> Fn(&'a TPg) -> &'a[u8],
{
	fn copy_value(&mut self, value: &TPg) -> Result<usize, String> {
		let bytes = (self.conversion)(value);
		let byte_size = bytes.len();
		self.builder.append_value(bytes);
		Ok(byte_size + 4)
	}
}

pub struct FixedLenBinaryColumnAppender<const N: usize, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	builder: FixedSizeBinaryBuilder,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<const N: usize, TPg, FConversion> FixedLenBinaryColumnAppender<N, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	pub fn new(conversion: FConversion) -> Self {
		Self {
			builder: FixedSizeBinaryBuilder::new(N.try_into().unwrap()),
			conversion,
			_pg: PhantomData,
		}
	}
}

impl<const N: usize, TPg, FConversion> ColumnAppenderBase for FixedLenBinaryColumnAppender<N, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	fn write_null(&mut self) -> Result<usize, String> {
		self.builder.append_null();
		Ok(N)
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		let array = Arc::new(self.builder.finish()) as ArrayRef;
		Ok(array)
	}
}

impl<const N: usize, TPg, FConversion> ColumnAppender<TPg> for FixedLenBinaryColumnAppender<N, TPg, FConversion>
	where FConversion: Fn(&TPg) -> Option<[u8; N]>,
{
	fn copy_value(&mut self, value: &TPg) -> Result<usize, String> {
		match (self.conversion)(value) {
			None => self.builder.append_null(),
			Some(bytes) => self.builder.append_value(&bytes).unwrap()
		}
		Ok(N)
	}
}

pub fn new_autoconv_byte_array_binary_appender<TPg>() -> PrimitiveColumnAppender<TPg, ByteArrayType, impl Fn(&TPg) -> PqByteArray>
	where
		TPg: Clone,
		PqByteArray: MyFrom<TPg>,
{
	PrimitiveColumnAppender::new(|value: &TPg| MyFrom::my_from((*value).clone()))
}
