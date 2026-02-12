use std::{marker::PhantomData, sync::Arc, borrow::Cow};
use std::any::TypeId;

use parquet::data_type::DataType;
use parquet::data_type::{ByteArray as PqByteArray, FixedLenByteArray as PqFixedLenByteArray};

use crate::{level_index::{LevelIndexState, LevelIndexList}, myfrom::MyFrom};

use super::{real_memory_size::RealMemorySize, ColumnAppenderBase, ColumnAppender};
// Arrow exports for parallel compression path
use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, Float32Builder, Float64Builder, BinaryBuilder, StringBuilder, FixedSizeBinaryBuilder};
use arrow::datatypes::DataType as ArrowDataType;


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
	conversion: FConversion,
}

pub fn new_autoconv_generic_appender<TPg, TPq: DataType>(
	max_dl: i16, max_rl: i16,
) -> GenericColumnAppender<TPg, TPq, impl Fn(TPg) -> TPq::T>
	where TPq::T: Clone + RealMemorySize, TPq::T: MyFrom<TPg> {
	GenericColumnAppender::new(max_dl, max_rl, |value: TPg| MyFrom::my_from(value))
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
			conversion,
		}
	}

	pub fn convert(&self, value: TPg) -> TPq::T {
		(self.conversion)(value)
	}
}

impl<TPg, TPq, FConversion> ColumnAppenderBase for GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {

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

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		// Support both flat and repeated contexts; rows derive from definition levels when nullable

        let make_nulls = self.max_dl > 0;
        let rows = if make_nulls { self.dls.len() } else { self.column.len() };

		// Helper to drive nullable iteration
		macro_rules! build_nullable {
			($builder:ident, $append:expr) => {{
				let mut b = $builder::with_capacity(rows);
				let mut val_idx = 0usize;
				for i in 0..rows {
					let is_valid = !make_nulls || self.dls[i] == self.max_dl;
					if is_valid {
						$append(&mut b, &self.column[val_idx]);
						val_idx += 1;
					} else {
						b.append_null();
					}
				}
				let arr = Arc::new(b.finish()) as ArrayRef;
				// Clear buffers after building array
				self.column.clear();
				self.dls.clear();
				self.rls.clear();
				Ok(arr)
			}};
		}

		// Dispatch based on parquet physical type carried by TPq
		let t_id = TypeId::of::<TPq>();
		if t_id == TypeId::of::<parquet::data_type::BoolType>() {
			match target {
				ArrowDataType::Boolean => {
					return build_nullable!(BooleanBuilder, |b: &mut BooleanBuilder, v: &TPq::T| {
						// SAFETY: when TPq == BoolType, TPq::T == bool
						let vv = unsafe { *(v as *const _ as *const bool) };
						b.append_value(vv);
					});
				}
				_ => return Err("Arrow BOOLEAN export: unsupported target type".to_string())
			}
		}
		if t_id == TypeId::of::<parquet::data_type::Int32Type>() {
			match target {
				ArrowDataType::Int32 => {
					return build_nullable!(Int32Builder, |b: &mut Int32Builder, v: &TPq::T| {
						let vv = unsafe { *(v as *const _ as *const i32) };
						b.append_value(vv);
					});
				}
				_ => return Err("Arrow INT32 export: unsupported target type".to_string())
			}
		}
		if t_id == TypeId::of::<parquet::data_type::Int64Type>() {
			match target {
				ArrowDataType::Int64 => {
					return build_nullable!(Int64Builder, |b: &mut Int64Builder, v: &TPq::T| {
						let vv = unsafe { *(v as *const _ as *const i64) };
						b.append_value(vv);
					});
				}
				_ => return Err("Arrow INT64 export: unsupported target type".to_string())
			}
		}
		if t_id == TypeId::of::<parquet::data_type::ByteArrayType>() {
			match target {
				ArrowDataType::Utf8 => {
					let total_bytes: usize = self.column.iter().map(|v| v.real_memory_size()).sum();
					let mut b = StringBuilder::with_capacity(rows, total_bytes);
					let mut val_idx = 0usize;
					for i in 0..rows {
						let is_valid = !make_nulls || self.dls[i] == self.max_dl;
						if is_valid {
							let v: &PqByteArray = unsafe { &*( &self.column[val_idx] as *const _ as *const PqByteArray) };
							let bytes = v.data();
							match std::str::from_utf8(bytes) {
								Ok(s) => b.append_value(s),
								Err(_) => b.append_null(),
							}
							val_idx += 1;
						} else {
							b.append_null();
						}
					}
					let arr = Arc::new(b.finish()) as ArrayRef;
					self.column.clear(); self.dls.clear(); self.rls.clear();
					return Ok(arr);
				}
				ArrowDataType::Binary => {
					let total_bytes: usize = self.column.iter().map(|v| v.real_memory_size()).sum();
					let mut b = BinaryBuilder::with_capacity(rows, total_bytes);
					let mut val_idx = 0usize;
					for i in 0..rows {
						let is_valid = !make_nulls || self.dls[i] == self.max_dl;
						if is_valid {
							let v: &PqByteArray = unsafe { &*( &self.column[val_idx] as *const _ as *const PqByteArray) };
							b.append_value(v.data());
							val_idx += 1;
						} else {
							b.append_null();
						}
					}
					let arr = Arc::new(b.finish()) as ArrayRef;
					self.column.clear(); self.dls.clear(); self.rls.clear();
					return Ok(arr);
				}
				_ => return Err("Arrow BYTE_ARRAY export: unsupported target type".to_string())
			}
		}
		if t_id == TypeId::of::<parquet::data_type::FixedLenByteArrayType>() {
			match target {
				ArrowDataType::FixedSizeBinary(len) => {
					let mut b = FixedSizeBinaryBuilder::new(*len as i32);
					let mut val_idx = 0usize;
					for i in 0..rows {
						let is_valid = !make_nulls || self.dls[i] == self.max_dl;
						if is_valid {
							let v: &PqFixedLenByteArray = unsafe { &*( &self.column[val_idx] as *const _ as *const PqFixedLenByteArray) };
							let bytes = v.data();
							if bytes.len() == (*len as usize) { let _ = b.append_value(bytes); } else { let _ = b.append_null(); }
							val_idx += 1;
						} else { let _ = b.append_null(); }
					}
					let arr = Arc::new(b.finish()) as ArrayRef;
					self.column.clear(); self.dls.clear(); self.rls.clear();
					return Ok(arr);
				}
				ArrowDataType::Binary => {
					let total_bytes: usize = self.column.iter().map(|v| v.real_memory_size()).sum();
					let mut b = BinaryBuilder::with_capacity(rows, total_bytes);
					let mut val_idx = 0usize;
					for i in 0..rows {
						let is_valid = !make_nulls || self.dls[i] == self.max_dl;
						if is_valid { 
							let v: &PqFixedLenByteArray = unsafe { &*( &self.column[val_idx] as *const _ as *const PqFixedLenByteArray) };
							b.append_value(v.data());
							val_idx += 1; 
						} else { b.append_null(); }
					}
					let arr = Arc::new(b.finish()) as ArrayRef;
					self.column.clear(); self.dls.clear(); self.rls.clear();
					return Ok(arr);
				}
				_ => return Err("Arrow FIXED_LEN_BYTE_ARRAY export: unsupported target type".to_string())
			}
		}
		if t_id == TypeId::of::<parquet::data_type::FloatType>() {
			match target {
				ArrowDataType::Float32 => {
					return build_nullable!(Float32Builder, |b: &mut Float32Builder, v: &TPq::T| {
						let vv = unsafe { *(v as *const _ as *const f32) };
						b.append_value(vv);
					});
				}
				_ => return Err("Arrow FLOAT export: unsupported target type".to_string())
			}
		}
		if t_id == TypeId::of::<parquet::data_type::DoubleType>() {
			match target {
				ArrowDataType::Float64 => {
					return build_nullable!(Float64Builder, |b: &mut Float64Builder, v: &TPq::T| {
						let vv = unsafe { *(v as *const _ as *const f64) };
						b.append_value(vv);
					});
				}
				_ => return Err("Arrow DOUBLE export: unsupported target type".to_string())
			}
		}
		Err("Arrow export: unsupported parquet physical type".to_string())
	}


	fn clear(&mut self) {
		self.column.clear();
		self.dls.clear();
		self.rls.clear();
	}

	// to_arrow_array: not implemented here; defaults to None
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
