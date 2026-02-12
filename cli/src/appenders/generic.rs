use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use arrow::array::{ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::DataType as ArrowDataType;

use parquet::basic;
use parquet::data_type::{DataType, ByteArray as PqByteArray, ByteArrayType, FixedLenByteArray as PqFixedLenByteArray, FixedLenByteArrayType};

use crate::{
	level_index::{LevelIndexList, LevelIndexState},
	myfrom::MyFrom,
};

use super::{real_memory_size::RealMemorySize, ColumnAppender, ColumnAppenderBase, DynColumnAppender};

pub trait ArrowBuilderBridge: Sized {
	type Native: Copy + RealMemorySize;

	fn with_capacity(capacity: usize) -> Self;
	fn append_value(&mut self, value: Self::Native);
	fn append_null(&mut self);
	fn finish(self) -> ArrayRef;
	fn arrow_type() -> ArrowDataType;
}

macro_rules! impl_arrow_builder_bridge {
	($builder:ty, $native:ty, $arrow_dt:expr) => {
		impl ArrowBuilderBridge for $builder {
			type Native = $native;

			fn with_capacity(capacity: usize) -> Self {
				<$builder>::with_capacity(capacity)
			}

			fn append_value(&mut self, value: Self::Native) {
				self.append_value(value)
			}

			fn append_null(&mut self) {
				self.append_null()
			}

			fn finish(mut self) -> ArrayRef {
				Arc::new(<$builder>::finish(&mut self)) as ArrayRef
			}

			fn arrow_type() -> ArrowDataType {
				$arrow_dt
			}
		}
	};
}

impl_arrow_builder_bridge!(BooleanBuilder, bool, ArrowDataType::Boolean);
impl_arrow_builder_bridge!(Int32Builder, i32, ArrowDataType::Int32);
impl_arrow_builder_bridge!(Int64Builder, i64, ArrowDataType::Int64);
impl_arrow_builder_bridge!(Float32Builder, f32, ArrowDataType::Float32);
impl_arrow_builder_bridge!(Float64Builder, f64, ArrowDataType::Float64);

pub trait PrimitiveParquetType: DataType
	where Self::T: Copy + RealMemorySize
{
	type Builder: ArrowBuilderBridge<Native = Self::T>;
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

pub struct PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: Copy + RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	max_dl: i16,
	max_rl: i16,
	builder: TPq::Builder,
	dls: Vec<i16>,
	rls: Vec<i16>,
	repetition_index: LevelIndexState,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
	_pq: PhantomData<TPq>,
}

pub fn new_autoconv_generic_appender<TPg, TPq>(max_dl: i16, max_rl: i16) -> PrimitiveColumnAppender<TPg, TPq, impl Fn(TPg) -> TPq::T>
	where
		TPq: PrimitiveParquetType,
		TPq::T: Copy + RealMemorySize + MyFrom<TPg>,
{
	PrimitiveColumnAppender::new(max_dl, max_rl, |value: TPg| MyFrom::my_from(value))
}

impl<TPg, TPq, FConversion> PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: Copy + RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		if max_dl < 0 || max_rl < 0 {
			panic!(
				"Cannot create {} with max_dl={}, max_rl={}",
				std::any::type_name::<Self>(),
				max_dl,
				max_rl
			);
		}
		PrimitiveColumnAppender {
			max_dl,
			max_rl,
			builder: TPq::Builder::with_capacity(0),
			dls: Vec::new(),
			rls: Vec::new(),
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
		TPq::T: Copy + RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		if self.max_dl == 0 {
			return Err("Cannot write null to required column".to_string());
		}
		debug_assert!(level < self.max_dl);
		self.dls.push(level);
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 {
		self.max_dl
	}

	fn max_rl(&self) -> i16 {
		self.max_rl
	}

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		let expected = <TPq::Builder as ArrowBuilderBridge>::arrow_type();
		if target != &expected {
			return Err(format!(
				"Arrow export mismatch: expected {:?}, received {:?}",
				expected,
				target
			));
		}
		let builder = std::mem::replace(&mut self.builder, TPq::Builder::with_capacity(0));
		let array = builder.finish();
		self.dls.clear();
		self.rls.clear();
		Ok(array)
	}

	fn clear(&mut self) {
		self.builder = TPq::Builder::with_capacity(0);
		self.dls.clear();
		self.rls.clear();
	}
}

impl<TPg: Clone, TPq, FConversion> ColumnAppender<TPg> for PrimitiveColumnAppender<TPg, TPq, FConversion>
	where
		TPq: PrimitiveParquetType,
		TPq::T: Copy + RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T,
{
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let pq_value = self.convert(value.into_owned());
		let byte_size = pq_value.real_memory_size();
		self.builder.append_value(pq_value);
		if self.max_dl > 0 {
			self.dls.push(self.max_dl);
		}
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryArrowExport {
	Binary,
	Utf8,
	FixedSizeBinary(i32),
}

pub struct ByteArrayBinaryColumnAppender<TPg, FConversion>
	where
		TPg: Clone,
		FConversion: Fn(TPg) -> PqByteArray,
{
	max_dl: i16,
	max_rl: i16,
	builder: BinaryBuilder,
	dls: Vec<i16>,
	rls: Vec<i16>,
	repetition_index: LevelIndexState,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> ByteArrayBinaryColumnAppender<TPg, FConversion>
	where
		TPg: Clone,
		FConversion: Fn(TPg) -> PqByteArray,
{
	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		if max_dl < 0 || max_rl < 0 {
			panic!(
				"Cannot create {} with max_dl={}, max_rl={}",
				std::any::type_name::<Self>(),
				max_dl,
				max_rl
			);
		}
		Self {
			max_dl,
			max_rl,
			builder: BinaryBuilder::new(),
			dls: Vec::new(),
			rls: Vec::new(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion,
			_pg: PhantomData,
		}
	}

	fn convert(&self, value: TPg) -> PqByteArray {
		(self.conversion)(value)
	}
}

impl<TPg: Clone, FConversion> ColumnAppenderBase for ByteArrayBinaryColumnAppender<TPg, FConversion>
	where
		FConversion: Fn(TPg) -> PqByteArray,
{
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		if self.max_dl == 0 {
			return Err("Cannot write null to required column".to_string());
		}
		debug_assert!(level < self.max_dl);
		self.dls.push(level);
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		if target != &ArrowDataType::Binary {
			return Err("Arrow BYTE_ARRAY export: expected Binary".to_string());
		}
		let mut builder = std::mem::replace(&mut self.builder, BinaryBuilder::new());
		let array = Arc::new(builder.finish()) as ArrayRef;
		self.dls.clear();
		self.rls.clear();
		Ok(array)
	}

	fn clear(&mut self) {
		self.builder = BinaryBuilder::new();
		self.dls.clear();
		self.rls.clear();
	}
}

impl<TPg: Clone, FConversion> ColumnAppender<TPg> for ByteArrayBinaryColumnAppender<TPg, FConversion>
	where
		FConversion: Fn(TPg) -> PqByteArray,
{
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let pq_value = self.convert(value.into_owned());
		let byte_size = pq_value.real_memory_size();
		self.builder.append_value(pq_value.data());
		if self.max_dl > 0 {
			self.dls.push(self.max_dl);
		}
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

pub struct ByteArrayUtf8ColumnAppender<TPg, FConversion>
	where
		TPg: Clone,
		FConversion: Fn(TPg) -> PqByteArray,
{
	max_dl: i16,
	max_rl: i16,
	builder: StringBuilder,
	dls: Vec<i16>,
	rls: Vec<i16>,
	repetition_index: LevelIndexState,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> ByteArrayUtf8ColumnAppender<TPg, FConversion>
	where
		TPg: Clone,
		FConversion: Fn(TPg) -> PqByteArray,
{
	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		if max_dl < 0 || max_rl < 0 {
			panic!(
				"Cannot create {} with max_dl={}, max_rl={}",
				std::any::type_name::<Self>(),
				max_dl,
				max_rl
			);
		}
		Self {
			max_dl,
			max_rl,
			builder: StringBuilder::new(),
			dls: Vec::new(),
			rls: Vec::new(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion,
			_pg: PhantomData,
		}
	}

	fn convert(&self, value: TPg) -> PqByteArray {
		(self.conversion)(value)
	}
}

impl<TPg: Clone, FConversion> ColumnAppenderBase for ByteArrayUtf8ColumnAppender<TPg, FConversion>
	where
		FConversion: Fn(TPg) -> PqByteArray,
{
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		if self.max_dl == 0 {
			return Err("Cannot write null to required column".to_string());
		}
		debug_assert!(level < self.max_dl);
		self.dls.push(level);
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		if target != &ArrowDataType::Utf8 {
			return Err("Arrow BYTE_ARRAY export: expected Utf8".to_string());
		}
		let mut builder = std::mem::replace(&mut self.builder, StringBuilder::new());
		let array = Arc::new(builder.finish()) as ArrayRef;
		self.dls.clear();
		self.rls.clear();
		Ok(array)
	}

	fn clear(&mut self) {
		self.builder = StringBuilder::new();
		self.dls.clear();
		self.rls.clear();
	}
}

impl<TPg: Clone, FConversion> ColumnAppender<TPg> for ByteArrayUtf8ColumnAppender<TPg, FConversion>
	where
		FConversion: Fn(TPg) -> PqByteArray,
{
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let pq_value = self.convert(value.into_owned());
		let byte_size = pq_value.real_memory_size();
		match std::str::from_utf8(pq_value.data()) {
			Ok(s) => {
				self.builder.append_value(s);
				if self.max_dl > 0 {
					self.dls.push(self.max_dl);
				}
			}
			Err(_) => {
				if self.max_dl == 0 {
					return Err("Invalid UTF-8 in required column".to_string());
				}
				self.builder.append_null();
				self.dls.push(self.max_dl - 1);
			}
		}
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

pub struct FixedLenBinaryColumnAppender<TPg, FConversion>
	where
		TPg: Clone,
		FConversion: Fn(TPg) -> PqFixedLenByteArray,
{
	max_dl: i16,
	max_rl: i16,
	len: i32,
	builder: FixedSizeBinaryBuilder,
	dls: Vec<i16>,
	rls: Vec<i16>,
	repetition_index: LevelIndexState,
	conversion: FConversion,
	_pg: PhantomData<TPg>,
}

impl<TPg, FConversion> FixedLenBinaryColumnAppender<TPg, FConversion>
	where
		TPg: Clone,
		FConversion: Fn(TPg) -> PqFixedLenByteArray,
{
	pub fn new(max_dl: i16, max_rl: i16, len: i32, conversion: FConversion) -> Self {
		if max_dl < 0 || max_rl < 0 {
			panic!(
				"Cannot create {} with max_dl={}, max_rl={}",
				std::any::type_name::<Self>(),
				max_dl,
				max_rl
			);
		}
		Self {
			max_dl,
			max_rl,
			len,
			builder: FixedSizeBinaryBuilder::new(len),
			dls: Vec::new(),
			rls: Vec::new(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion,
			_pg: PhantomData,
		}
	}

	fn convert(&self, value: TPg) -> PqFixedLenByteArray {
		(self.conversion)(value)
	}
}

impl<TPg: Clone, FConversion> ColumnAppenderBase for FixedLenBinaryColumnAppender<TPg, FConversion>
	where
		FConversion: Fn(TPg) -> PqFixedLenByteArray,
{
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		if self.max_dl == 0 {
			return Err("Cannot write null to required column".to_string());
		}
		debug_assert!(level < self.max_dl);
		self.dls.push(level);
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		self.builder.append_null();
		Ok(2 + if self.max_rl > 0 { 2 } else { 0 })
	}

	fn max_dl(&self) -> i16 { self.max_dl }

	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		if target != &ArrowDataType::FixedSizeBinary(self.len) {
			return Err("Arrow FIXED_LEN_BYTE_ARRAY export: expected FixedSizeBinary".to_string());
		}
		let mut builder = std::mem::replace(&mut self.builder, FixedSizeBinaryBuilder::new(self.len));
		let array = Arc::new(builder.finish()) as ArrayRef;
		self.dls.clear();
		self.rls.clear();
		Ok(array)
	}

	fn clear(&mut self) {
		self.builder = FixedSizeBinaryBuilder::new(self.len);
		self.dls.clear();
		self.rls.clear();
	}
}

impl<TPg: Clone, FConversion> ColumnAppender<TPg> for FixedLenBinaryColumnAppender<TPg, FConversion>
	where
		FConversion: Fn(TPg) -> PqFixedLenByteArray,
{
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let pq_value = self.convert(value.into_owned());
		let byte_size = pq_value.real_memory_size();
		let bytes = pq_value.data();
		if bytes.len() != self.len as usize {
			if self.max_dl == 0 {
				return Err("FixedLenBinary value has wrong length".to_string());
			}
			self.builder.append_null();
			self.dls.push(self.max_dl - 1);
		} else {
			let _ = self.builder.append_value(bytes);
			if self.max_dl > 0 {
				self.dls.push(self.max_dl);
			}
		}
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
		}
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

pub fn new_autoconv_byte_array_binary_appender<TPg>(max_dl: i16, max_rl: i16) -> ByteArrayBinaryColumnAppender<TPg, impl Fn(TPg) -> PqByteArray>
	where
		TPg: Clone,
		PqByteArray: MyFrom<TPg>,
{
	ByteArrayBinaryColumnAppender::new(max_dl, max_rl, |value: TPg| MyFrom::my_from(value))
}

pub fn new_autoconv_byte_array_utf8_appender<TPg>(max_dl: i16, max_rl: i16) -> ByteArrayUtf8ColumnAppender<TPg, impl Fn(TPg) -> PqByteArray>
	where
		TPg: Clone,
		PqByteArray: MyFrom<TPg>,
{
	ByteArrayUtf8ColumnAppender::new(max_dl, max_rl, |value: TPg| MyFrom::my_from(value))
}

pub fn new_autoconv_fixed_len_binary_appender<TPg>(max_dl: i16, max_rl: i16, len: i32) -> FixedLenBinaryColumnAppender<TPg, impl Fn(TPg) -> PqFixedLenByteArray>
	where
		TPg: Clone,
		PqFixedLenByteArray: MyFrom<TPg>,
{
	FixedLenBinaryColumnAppender::new(max_dl, max_rl, len, |value: TPg| MyFrom::my_from(value))
}

pub trait BinaryAppenderFactory<TPg: Clone>: DataType {
	fn build_appender<FConversion>(
		max_dl: i16,
		max_rl: i16,
		conversion: FConversion,
		arrow: BinaryArrowExport,
	) -> Result<DynColumnAppender<TPg>, String>
	where
		FConversion: Fn(TPg) -> Self::T + 'static;
}

impl<TPg: Clone + 'static> BinaryAppenderFactory<TPg> for ByteArrayType {
	fn build_appender<FConversion>(
		max_dl: i16,
		max_rl: i16,
		conversion: FConversion,
		arrow: BinaryArrowExport,
	) -> Result<DynColumnAppender<TPg>, String>
	where
		FConversion: Fn(TPg) -> PqByteArray + 'static,
	{
		match arrow {
			BinaryArrowExport::Binary => {
				let conv = conversion;
				Ok(Box::new(ByteArrayBinaryColumnAppender::new(max_dl, max_rl, conv)) as DynColumnAppender<TPg>)
			}
			BinaryArrowExport::Utf8 => {
				let conv = conversion;
				Ok(Box::new(ByteArrayUtf8ColumnAppender::new(max_dl, max_rl, conv)) as DynColumnAppender<TPg>)
			}
			BinaryArrowExport::FixedSizeBinary(_) => Err("Cannot export BYTE_ARRAY as FixedSizeBinary".to_string()),
		}
	}
}

impl<TPg: Clone + 'static> BinaryAppenderFactory<TPg> for FixedLenByteArrayType {
	fn build_appender<FConversion>(
		max_dl: i16,
		max_rl: i16,
		conversion: FConversion,
		arrow: BinaryArrowExport,
	) -> Result<DynColumnAppender<TPg>, String>
	where
		FConversion: Fn(TPg) -> PqFixedLenByteArray + 'static,
	{
		match arrow {
			BinaryArrowExport::FixedSizeBinary(len) => {
				let conv = conversion;
				Ok(Box::new(FixedLenBinaryColumnAppender::new(max_dl, max_rl, len, conv)) as DynColumnAppender<TPg>)
			}
			_ => Err("FIXED_LEN_BYTE_ARRAY export requires FixedSizeBinary target".to_string()),
		}
	}
}

pub fn new_binary_value_appender<TPg, TPq, FConversion>(
	max_dl: i16,
	max_rl: i16,
	conversion: FConversion,
	arrow: BinaryArrowExport,
) -> Result<DynColumnAppender<TPg>, String>
	where
		TPg: Clone + 'static,
		TPq: BinaryAppenderFactory<TPg>,
		TPq::T: RealMemorySize,
		FConversion: Fn(TPg) -> TPq::T + 'static,
{
	<TPq as BinaryAppenderFactory<TPg>>::build_appender(max_dl, max_rl, conversion, arrow)
}
