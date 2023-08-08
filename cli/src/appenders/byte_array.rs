use std::{marker::PhantomData, sync::Arc, borrow::Cow};

use bytes::Bytes;
use parquet::{data_type::{DataType, ByteArray, FixedLenByteArray, ByteArrayType}, file::writer::SerializedColumnWriter, errors::ParquetError};

use crate::{level_index::{LevelIndexState, LevelIndexList}, myfrom::MyFrom, pg_custom_types::{PgAnyRef, PgAbstractRow}};

use super::{real_memory_size::RealMemorySize, ColumnAppenderBase, ColumnAppender, DynamicSerializedWriter};


pub struct ByteArrayColumnAppender<TPg: Clone, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> usize> {
	max_dl: i16,
	max_rl: i16,
	byte_buffer: Vec<u8>,
	offsets: Vec<usize>,
	dls: Vec<i16>,
	rls: Vec<i16>,
	repetition_index: LevelIndexState,
	conversion: FCopyTo,
	_dummy: PhantomData<TPg>,
}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> usize> ByteArrayColumnAppender<TPg, FCopyTo> {
	pub fn new(max_dl: i16, max_rl: i16, f_copy: FCopyTo) -> Self {
		if max_dl < 0 || max_rl < 0 {
			panic!("Cannot create {} with max_dl={}, max_rl={}", std::any::type_name::<Self>(), max_dl, max_rl);
		}
		ByteArrayColumnAppender {
			max_dl, max_rl,
			byte_buffer: Vec::new(),
			offsets: Vec::new(),
			_dummy: PhantomData,
			dls: Vec::new(),
			rls: Vec::new(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion: f_copy,
		}
	}

	pub fn append(&mut self, value: &TPg) -> usize {
		let index = self.byte_buffer.len();
		let len = (self.conversion)(value, &mut self.byte_buffer);
		debug_assert_eq!(index + len, self.byte_buffer.len());
		self.offsets.push(index);
		len
	}

	fn write_column(&mut self, writer: &mut SerializedColumnWriter) -> Result<(), ParquetError> {
		let dls = if self.max_dl > 0 { Some(self.dls.as_slice()) } else { None };
		let rls = if self.max_rl > 0 { Some(self.rls.as_slice()) } else { None };

		let writer_t = writer.typed::<ByteArrayType>();

		if self.offsets.len() == 0 {
			writer_t.write_batch(&[], dls, rls)?;
			return Ok(());
		}

		// if self.max_rl > 0 {
		// 	println!("Writing values: {:?}", self.column);
		// 	println!("           RLS: {:?}", self.rls);
		// 	println!("           DLS: {:?}", self.dls);
		// }
		let mut byte_array = Vec::new();
		std::mem::swap(&mut self.byte_buffer, &mut byte_array);
		let byte_array = Bytes::from(byte_array);

		let mut column: Vec<ByteArray> = vec![ByteArray::new(); self.offsets.len()];
		for ((&offset, &next), out) in self.offsets.iter().zip(self.offsets.iter().skip(1)).zip(column.iter_mut()) {
			let b: Bytes = byte_array.slice(offset..next);
			let b_array: ByteArray = ByteArray::from(b);
			*out = b_array;
		}
		column[self.offsets.len()-1] = ByteArray::from(byte_array.slice(*self.offsets.last().unwrap()..));

		let _num_written = writer_t.write_batch(&column, dls, rls)?;

		self.offsets.clear();
		self.byte_buffer.reserve(byte_array.len() / 4);
		assert_eq!(0, self.byte_buffer.len());
		self.dls.clear();
		self.rls.clear();

		Ok(())
	}
}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> usize> ColumnAppenderBase for ByteArrayColumnAppender<TPg, FCopyTo> {

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		let mut error = None;
		let c = next_col.next_column(&mut |mut column| {
			let result = self.write_column(&mut column);
			let error1 = result.err();
			let result2 = column.close();

			error = error1.or(result2.err());
			
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

		self.dls.push(level);
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
			Ok(4)
		} else {
			Ok(2)
		}
	}

	fn max_dl(&self) -> i16 { self.max_dl }
	fn max_rl(&self) -> i16 { self.max_rl }
}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut Vec<u8>) -> usize> ColumnAppender<TPg> for ByteArrayColumnAppender<TPg, FCopyTo> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let byte_size = self.append(value.as_ref());
		if self.max_dl > 0 {
			self.dls.push(self.max_dl);
		}
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);

			// println!("Appending {:?} with RL: {}, {:?} {:?}", self.column.last().unwrap(),  rl, self_ri, repetition_index);
			self.rls.push(rl);
		}
		Ok(byte_size + (self.max_dl > 0) as usize * 2 + (self.max_rl > 0) as usize * 2)
	}
}

// pub struct PostgresStringAppender<Inner: for<'a> ColumnAppender<PgAnyRef<'a>>> {
// 	inner: Inner
// }

// impl Col

pub fn create_string_appender<TRow: PgAbstractRow>(max_dl: i16, max_rl: i16, column_index: usize) -> impl ColumnAppender<Arc<TRow>> {
	let a = ByteArrayColumnAppender::new(max_dl, max_rl, move |row: &Arc<TRow>, buffer: &mut Vec<u8>| {
		let value: PgAnyRef = row.ab_get(column_index);
		debug_assert!(value.ty == postgres::types::Type::TEXT);
		buffer.extend_from_slice(value.value);
		value.value.len()
	});
	a
}
