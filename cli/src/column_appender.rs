use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

use parquet::data_type::{DataType, AsBytes, SliceAsBytes, ByteArray};
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

pub trait ColumnAppenderBase {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String>;

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String>;

	fn max_dl(&self) -> i16;
	fn max_rl(&self) -> i16;
}

pub trait ColumnAppender<TPg: Clone>: ColumnAppenderBase {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String>;
	fn copy_value_opt(&mut self, repetition_index: &LevelIndexList, value: Cow<Option<TPg>>) -> Result<usize, String> {
		match value {
			Cow::Owned(Some(value)) => self.copy_value(repetition_index, Cow::<TPg>::Owned(value)),
			Cow::Borrowed(Some(value)) => self.copy_value(repetition_index, Cow::Borrowed(value)),
			Cow::Owned(None) | Cow::Borrowed(None) => {
				assert_ne!(self.max_dl(), 0);
				self.write_null(repetition_index, self.max_dl() - 1)
			},
		}
	}
}

pub type DynColumnAppender<T> = Box<dyn ColumnAppender<T>>;

impl<T> ColumnAppenderBase for DynColumnAppender<T> {
    fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
        self.as_mut().write_null(repetition_index, level)
    }

    fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
        self.as_mut().write_columns(column_i, next_col)
    }

    fn max_dl(&self) -> i16 {
        self.as_ref().max_dl()
    }

    fn max_rl(&self) -> i16 {
        self.as_ref().max_rl()
    }
}

impl<T: Clone> ColumnAppender<T> for DynColumnAppender<T> {
    fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<T>) -> Result<usize, String> {
        self.as_mut().copy_value(repetition_index, value)
    }
}

// impl<'a, T, X: ColumnAppender<Cow<'a, T>>> ColumnAppender<Cow<'a, Arc<T>>> for X {
//     fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<'a, Arc<T>>) -> Result<usize, String> {
//         let nvalue = Cow::Borrowed(value.as_ref());
// 		self.as_mut().copy_value(repetition_index, nvalue)
//     }
// }

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
		if max_dl < 0 || max_rl < 0 {
			panic!("Cannot create {} with max_dl={}, max_rl={}", std::any::type_name::<Self>(), max_dl, max_rl);
		}
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

impl<TPg, TPq, FConversion> ColumnAppenderBase for GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {

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

impl<TPg: Clone, TPq, FConversion> ColumnAppender<TPg> for GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let pq_value = self.convert(value.into_owned());
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
}

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
			panic!("Cannot create {}, definition levels {} must be {} less than inner definition levels {}", std::any::type_name::<Self>(), dl, if allow_element_null { "one" } else { "two" }, inner.max_dl());
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

impl<'a, TPg: Clone, TInner, TArray: Clone> ColumnAppender<TArray> for ArrayColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg>,
		  TArray: IntoIterator<Item = Option<TPg>> + Clone {

	fn copy_value(&mut self, repetition_index: &LevelIndexList, array: Cow<TArray>) -> Result<usize, String> {
		let mut bytes_written = 0;

		let mut nested_ri = repetition_index.new_child();

		for (_index, value) in array.into_owned().into_iter().enumerate() {
			match value {
				Some(value) => {
					bytes_written += self.inner.copy_value(&nested_ri, Cow::Owned(value))?;
					nested_ri.inc();
				},
				None => {
					if self.allow_element_null {
						debug_assert_eq!(self.dl + 1, self.inner.max_dl() - 1);
						bytes_written += self.inner.write_null(&nested_ri, self.dl + 1)?;
						nested_ri.inc();
					} else {
						// skip
					}
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
				let nested_ri = repetition_index.new_child();
				self.inner.write_null(&nested_ri, self.dl - self.allow_null as i16)
			},
		}
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

