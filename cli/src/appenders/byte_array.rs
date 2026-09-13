use std::{io::Read, marker::PhantomData, sync::Arc};
use byteorder::{ReadBytesExt, BigEndian};
use crate::pg_custom_types::{PgAnyRef, PgAbstractRow};
use super::{ColumnAppenderBase, ColumnAppender};
use arrow::array::{ArrayRef, BinaryBuilder};

pub struct ByteArrayColumnAppender<TPg, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> {
	// Build Arrow Binary directly; avoids maintaining custom buffers
	builder: BinaryBuilder,
	// Temporary scratch buffer for conversion closure
	scratch: Vec<u8>,

	conversion: FCopyTo,
	_dummy: PhantomData<TPg>,
}

impl<TPg, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> ByteArrayColumnAppender<TPg, FCopyTo> {
	pub fn new(f_copy: FCopyTo) -> Self {
		ByteArrayColumnAppender {
			builder: BinaryBuilder::new(),
			scratch: Vec::new(),
			_dummy: PhantomData,

			conversion: f_copy,
		}
	}

	pub fn append(&mut self, value: &TPg) -> usize {
		// Prepare scratch and convert value into it
		self.scratch.clear();
		if let Some(len) = (self.conversion)(value, &mut self.scratch) {
			debug_assert_eq!(len, self.scratch.len());
			self.builder.append_value(&self.scratch);

			len + 1
		} else {
			self.append_null()
		}
	}

	fn append_null(&mut self) -> usize {
		self.builder.append_null();
		1
	}

}

impl<TPg, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> ColumnAppenderBase for ByteArrayColumnAppender<TPg, FCopyTo> {
	fn write_null(&mut self) -> Result<usize, String> {
		Ok(self.append_null())
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		Ok(Arc::new(self.builder.finish()))
	}
}

impl<TPg, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> Option<usize>> ColumnAppender<TPg> for ByteArrayColumnAppender<TPg, FCopyTo> {
	fn copy_value(&mut self, value: &TPg) -> Result<usize, String> {
		let byte_size = self.append(value);

		Ok(byte_size)
	}
}

/// Directly appends the bytes of the Postgres wire representation
/// Works for TEXT (and similar), BYTES, JSON (not JSONB!!)
pub fn create_pg_raw_appender<TRow: PgAbstractRow>(column_index: usize) -> impl ColumnAppender<TRow> {
	let a = ByteArrayColumnAppender::new(move |row: &TRow, buffer: &mut Vec<u8>| {
		if let Some(value) = row.ab_get::<Option<PgAnyRef>>(column_index) {
			buffer.extend_from_slice(value.value);
			Some(value.value.len())
		} else {
			None
		}
	});
	a
}

pub fn create_jsonb_appender<TRow: PgAbstractRow>(column_index: usize) -> impl ColumnAppender<TRow> {
	let a = ByteArrayColumnAppender::new(move |row: &TRow, buffer: &mut Vec<u8>| {
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
// }
