use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

use parquet::data_type::{DataType, AsBytes, SliceAsBytes};
use parquet::{errors::ParquetError, file::writer::SerializedColumnWriter};
use parquet::column::writer::{GenericColumnWriter, Level};
use postgres::types::FromSql;

use crate::column_pg_copier::DynamicSerializedWriter;
use crate::level_index::*;
use crate::myfrom::MyFrom;

pub fn generic_column_appender_new_myfrom<TPg, TPq>(max_dl: i16, max_rl: i16) -> GenericColumnAppender<TPg, TPq, impl Fn(TPg) -> TPq::T>
	where TPq::T: RealMemorySize, TPq: DataType, TPq::T: MyFrom<TPg> {
	GenericColumnAppender::new(max_dl, max_rl, |x| TPq::T::my_from(x))
}

pub trait ColumnAppender<TPg>: Clone {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: TPg) -> Result<usize, String>;
	fn copy_value_opt(&mut self, repetition_index: &LevelIndexList, value: Option<TPg>) -> Result<usize, String> {
		match value {
			Some(value) => self.copy_value(repetition_index, value),
			None => self.write_null(repetition_index, self.max_dl() - 1),
		}
	}
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String>;

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String>;

	fn max_dl(&self) -> i16;
	fn max_rl(&self) -> i16;
}

pub struct GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {
	max_dl: i16,
	max_rl: i16,
	column: Vec<TPq::T>,
	dls: Vec<i16>,
	rls: Vec<i16>,
	dummy: PhantomData<TPg>,
	dummy2: PhantomData<TPq>,
	repetition_index: LevelIndexState,
	conversion: Arc<FConversion>,
}

impl<TPg, TPq, FConversion> GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {

	pub fn new(max_dl: i16, max_rl: i16, conversion: FConversion) -> Self {
		GenericColumnAppender {
			max_dl, max_rl,
			column: Vec::new(),
			dummy: PhantomData,
			dummy2: PhantomData,
			dls: Vec::new(),
			rls: Vec::new(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion: Arc::new(conversion),
		}
	}

	pub fn element_size(&self) -> usize {
		size_of::<TPq>() + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2
	}

	pub fn convert(&self, value: TPg) -> TPq::T {
		(self.conversion)(value)
	}

	fn write_column(&mut self, writer: &mut SerializedColumnWriter) -> Result<(), ParquetError> {
		let dls = if self.max_dl > 0 { Some(self.dls.as_slice()) } else { None };
		let rls = if self.max_rl > 0 { Some(self.rls.as_slice()) } else { None };

		// if self.max_rl > 0 {
		// 	println!("Writing values: {:?}", self.column);
		// 	println!("           RLS: {:?}", self.rls);
		// 	println!("           DLS: {:?}", self.dls);
		// }
		
		let typed = writer.typed::<TPq>();
		let _num_written = typed.write_batch(&self.column, dls, rls)?;

		self.column.clear();
		self.dls.clear();
		self.rls.clear();

		Ok(())
	}
}

impl<TPg, TPq, FConversion> Clone for GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {
	fn clone(&self) -> Self {
		GenericColumnAppender {
			max_dl: self.max_dl,
			max_rl: self.max_rl,
			column: self.column.clone(),
			dummy: PhantomData,
			dummy2: PhantomData,
			dls: self.dls.clone(),
			rls: self.rls.clone(),
			repetition_index: self.repetition_index.clone(),
			conversion: self.conversion.clone(),
		}
	}
}

impl<TPg, TPq, FConversion> GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize + MyFrom<TPg>, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {

	pub fn new_mfrom(max_dl: i16, max_rl: i16) -> GenericColumnAppender<TPg, TPq, impl Fn(TPg) -> TPq::T> {
		GenericColumnAppender::new(max_dl, max_rl, |x| TPq::T::my_from(x))
	}
}


impl<TPg, TPq, FConversion> ColumnAppender<TPg> for GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: TPg) -> Result<usize, String> {
		let pq_value = self.convert(value);
		let byte_size = pq_value.real_memory_size();
		self.column.push(pq_value);
		if self.max_dl > 0 {
			self.dls.push(self.max_dl);
		}
		if self.max_rl > 0 {
			// let self_ri = self.repetition_index.clone();
			let rl = self.repetition_index.copy_and_diff(repetition_index);

			// println!("Appending {:?} with RL: {}, {:?} {:?}", self.column.last().unwrap(),  rl, self_ri, repetition_index);
			self.rls.push(rl);
		}
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		let mut error = None;
		let c = next_col.next_column(&mut |mut column| {
			let result = self.write_column(&mut column);
			error = result.err();
			let result2 = column.close();

			error = error.clone().or(result2.err());
			
		}).map_err(|e| format!("Could not create column[{}]: {}", column_i, e))?;

		if error.is_some() {
			return Err(format!("Couldn't write data of column[{}]: {}", column_i, error.unwrap()));
		}

		if !c {
			return Err("Not enough columns".to_string());
		}

		Ok(())
	}


	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		debug_assert!(level < self.max_dl);

		// self.column.push(self.default.clone());

		self.dls.push(level);
		if self.max_rl > 0 {
			// let self_ri = self.repetition_index.clone();
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			// println!("Appending NULL with RL: {}, {:?} {:?}",  rl, self_ri, repetition_index);
			self.rls.push(rl);
			Ok(4)
		} else {
			Ok(2)
		}
	}

	fn max_dl(&self) -> i16 { self.max_dl }
	fn max_rl(&self) -> i16 { self.max_rl }
}

pub struct ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	inner: TInner,
	dummy: PhantomData<TPg>,
	// dummy2: PhantomData<TPg>,
}

impl<TPg, TInner> ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	pub fn new(inner: TInner) -> Self {
		ArrayColumnAppender { inner, dummy: PhantomData }
	}

	// fn max_dl(&self) -> i16 { self.inner.max_dl() }
}

impl<TPg, TInner> Clone for ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	fn clone(&self) -> Self {
		ArrayColumnAppender { inner: self.inner.clone(), dummy: PhantomData }
	}
}

// impl<TPg, TPq, FConversion> ArrayColumnAppender<TPg, GenericColumnAppender<TPg, TPq, TDataType>>
// 	where TPq: Default + From<TPg> + RealMemorySize, TDataType: DataType<T = TPq> {
// 	fn new_generic(max_dl: i16, max_rl: i16) -> Self
// 		where TPq: Default, TDataType: DataType<T = TPq>, TPq: From<TPg> {
// 		ArrayColumnAppender::new(GenericColumnAppender::<TPg, TPq, TDataType>::new(max_dl, max_rl + 1))
// 	}
// }

impl<'a, TPg, TInner, TArray> ColumnAppender<TArray> for ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg>,
		  TArray: IntoIterator<Item = Option<TPg>> {

	fn copy_value(&mut self, repetition_index: &LevelIndexList, array: TArray) -> Result<usize, String> {
		let mut bytes_written = 0;

		let mut nested_ri = repetition_index.new_child();

		for (_index, value) in array.into_iter().enumerate() {
			bytes_written += self.inner.copy_value_opt(&nested_ri, value)?;

			nested_ri.inc();
		}

		// at least one element has to be there
		if nested_ri.index == 0 {
			bytes_written += self.inner.write_null(&nested_ri, self.inner.max_dl() - 1)?;
		}
		Ok(bytes_written)
	}

	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		debug_assert!(level < self.inner.max_dl());

		let nested_ri = repetition_index.new_child();
		if level == self.inner.max_dl() - 1 {
			self.inner.write_null(&nested_ri, level)
		} else {
			self.inner.write_null(&nested_ri, level)
		}
	}

	fn max_dl(&self) -> i16 { self.inner.max_dl() }
	fn max_rl(&self) -> i16 { self.inner.max_rl() - 1 }

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		self.inner.write_columns(column_i, next_col)
	}
}

pub trait RealMemorySize {
	fn real_memory_size(&self) -> usize;
}

impl RealMemorySize for bool {
	fn real_memory_size(&self) -> usize { 1 }
}
impl RealMemorySize for i8 {
	fn real_memory_size(&self) -> usize { 1 }
}
impl RealMemorySize for u8 {
	fn real_memory_size(&self) -> usize { 1 }
}
impl RealMemorySize for i16 {
	fn real_memory_size(&self) -> usize { 2 }
}
impl RealMemorySize for u16 {
	fn real_memory_size(&self) -> usize { 2 }
}
impl RealMemorySize for i32 {
	fn real_memory_size(&self) -> usize { 4 }
}
impl RealMemorySize for u32 {
	fn real_memory_size(&self) -> usize { 4 }
}
impl RealMemorySize for i64 {
	fn real_memory_size(&self) -> usize { 8 }
}
impl RealMemorySize for u64 {
	fn real_memory_size(&self) -> usize { 8 }
}
impl RealMemorySize for f32 {
	fn real_memory_size(&self) -> usize { 4 }
}
impl RealMemorySize for f64 {
	fn real_memory_size(&self) -> usize { 8 }
}
impl RealMemorySize for [u8] {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for Vec<u8> {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for str {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for parquet::data_type::Decimal {
	fn real_memory_size(&self) -> usize { self.data().len() }
}
impl RealMemorySize for parquet::data_type::ByteArray {
	fn real_memory_size(&self) -> usize { self.data().len() }
}
impl RealMemorySize for parquet::data_type::FixedLenByteArray {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for parquet::data_type::Int96 {
	fn real_memory_size(&self) -> usize { 12 }
}

