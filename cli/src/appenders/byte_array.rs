use std::{borrow::Cow, io::Read, marker::PhantomData, sync::Arc};

use byteorder::{ReadBytesExt, ByteOrder, BigEndian};
use parquet::data_type::ByteArrayType;

use crate::{level_index::{LevelIndexState, LevelIndexList}, myfrom::MyFrom, pg_custom_types::{PgAnyRef, PgAbstractRow}};

use super::{real_memory_size::RealMemorySize, ColumnAppenderBase, ColumnAppender};
use arrow::array::{ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType as ArrowDataType;


pub struct ByteArrayColumnAppender<TPg, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> {
	max_dl: i16,
	max_rl: i16,
	// Build Arrow Binary directly; avoids maintaining custom buffers
	builder: BinaryBuilder,
	// Temporary scratch buffer for conversion closure
	scratch: Vec<u8>,
	repetition_index: LevelIndexState,
	conversion: FCopyTo,
	_dummy: PhantomData<TPg>,
}

impl<TPg, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> ByteArrayColumnAppender<TPg, FCopyTo> {
	pub fn new(max_dl: i16, max_rl: i16, f_copy: FCopyTo) -> Self {
		if max_dl < 0 || max_rl < 0 {
			panic!("Cannot create {} with max_dl={}, max_rl={}", std::any::type_name::<Self>(), max_dl, max_rl);
		}
		ByteArrayColumnAppender {
			max_dl, max_rl,
			builder: BinaryBuilder::new(),
			scratch: Vec::new(),
			_dummy: PhantomData,
			repetition_index: LevelIndexState::new(max_rl),
			conversion: f_copy,
		}
	}

	pub fn append(&mut self, value: &TPg) -> usize {
		// Prepare scratch and convert value into it
		self.scratch.clear();
		if let Some(len) = (self.conversion)(value, &mut self.scratch) {
			debug_assert_eq!(len, self.scratch.len());
			self.builder.append_value(&self.scratch);

			len + 2 * (self.max_dl > 0) as usize + 2 * (self.max_rl > 0) as usize
		} else {
			self.write_null(self.max_dl - 1)
		}
	}

	fn write_null(&mut self, level: i16) -> usize {
		debug_assert!(level < self.max_dl);

		if self.max_rl > 0 {
			4
		} else {
			2
		}
	}

}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> ColumnAppenderBase for ByteArrayColumnAppender<TPg, FCopyTo> {

	fn write_null(&mut self, _repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		Ok(self.write_null(level))
	}

	fn max_dl(&self) -> i16 { self.max_dl }
	fn max_rl(&self) -> i16 { self.max_rl }

	fn to_arrow_array(&mut self, target: &ArrowDataType) -> Result<ArrayRef, String> {
		match target {
			ArrowDataType::Binary => {
				let arr = Arc::new(self.builder.finish()) as ArrayRef;
				Ok(arr)
			}
			_ => Err("Arrow BYTE_ARRAY export: unsupported target type".to_string())
		}
	}
}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> ColumnAppender<TPg> for ByteArrayColumnAppender<TPg, FCopyTo> {
	fn copy_value(&mut self, _repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let byte_size = self.append(value.as_ref());
		
		Ok(byte_size)
	}
}


/// Directly appends the bytes of the Postgres wire representation
/// Works for TEXT (and similar), BYTES, JSON (not JSONB!!)
pub fn create_pg_raw_appender<TRow: PgAbstractRow + Clone>(max_dl: i16, max_rl: i16, column_index: usize) -> impl ColumnAppender<TRow> {
	let a = ByteArrayColumnAppender::new(max_dl, max_rl, move |row: &TRow, buffer: &mut Vec<u8>| {
		if let Some(value) = row.ab_get::<Option<PgAnyRef>>(column_index) {
			buffer.extend_from_slice(value.value);
			Some(value.value.len())
		} else {
			None
		}
	});
	a
}

pub fn create_jsonb_appender<TRow: PgAbstractRow + Clone>(max_dl: i16, max_rl: i16, column_index: usize) -> impl ColumnAppender<TRow> {
	let a = ByteArrayColumnAppender::new(max_dl, max_rl, move |row: &TRow, buffer: &mut Vec<u8>| {
		if let Some(value) = row.ab_get::<Option<PgAnyRef>>(column_index) {

			debug_assert_eq!(value.ty, postgres::types::Type::JSONB);
			let mut data = value.value;
			let version = data.read_i32::<BigEndian>().unwrap();
			assert_eq!(version, 1);
			buffer.extend_from_slice(data);
			Some(value.value.len())
		} else {
			None
		}
	});
	a
}
// pub fn create_string_appender<TRow: PgAbstractRow>(max_dl: i16, max_rl: i16, column_index: usize) -> impl ColumnAppender<Arc<TRow>> {
// 	create_pg_raw_appender::<TRow>(max_dl, max_rl, column_index)
// }
