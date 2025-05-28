use std::{marker::PhantomData, sync::Arc, borrow::Cow};

use byteorder::{ReadBytesExt, ByteOrder, BigEndian};
use bytes::{Bytes, BufMut};
use parquet::{data_type::{ByteArray, ByteArrayType, DataType, FixedLenByteArray, FixedLenByteArrayType}, errors::ParquetError, file::writer::SerializedColumnWriter};

use crate::{appenders::byte_array, level_index::{LevelIndexList, LevelIndexState}, myfrom::MyFrom, pg_custom_types::{PgAbstractRow, PgAnyRef}};

use super::{real_memory_size::RealMemorySize, ColumnAppenderBase, ColumnAppender, DynamicSerializedWriter};

pub struct FixedByteArrayColumnAppender<TPg, FCopyTo: Fn(&TPg, &mut [u8]) -> Option<usize>> {
	max_dl: i16,
	max_rl: i16,
	nullable: bool,
	length: usize,
	byte_buffer: Vec<u8>,
	dls: Vec<i16>,
	rls: Vec<i16>,
	repetition_index: LevelIndexState,
	conversion: FCopyTo,
	_dummy: PhantomData<TPg>,
}

impl<TPg, FCopyTo: Fn(&TPg, &mut [u8]) -> Option<usize>> FixedByteArrayColumnAppender<TPg, FCopyTo> {
	pub fn new(max_dl: i16, max_rl: i16, length: usize, nullable: bool, f_copy: FCopyTo) -> Self {
		if max_dl < 0 || max_rl < 0 {
			panic!("Cannot create {} with max_dl={}, max_rl={}", std::any::type_name::<Self>(), max_dl, max_rl);
		}
		FixedByteArrayColumnAppender {
			max_dl, max_rl,
			byte_buffer: Vec::new(),
			length,
			nullable,
			_dummy: PhantomData,
			dls: Vec::new(),
			rls: Vec::new(),
			repetition_index: LevelIndexState::new(max_rl),
			conversion: f_copy,
		}
	}

	pub fn append(&mut self, repetition_index: &LevelIndexList, value: &TPg) -> usize {
		let index = self.byte_buffer.len();
		if let Some(len) = (self.conversion)(value, &mut self.byte_buffer) {
			debug_assert_eq!(index + len, self.byte_buffer.len());

			if self.max_dl > 0 {
				self.dls.push(self.max_dl);
			}
			if self.max_rl > 0 {
				let rl = self.repetition_index.copy_and_diff(repetition_index);
	
				// println!("Appending {:?} with RL: {}, {:?} {:?}", self.column.last().unwrap(),  rl, self_ri, repetition_index);
				self.rls.push(rl);
			}

			len + 2 * (self.max_dl > 0) as usize + 2 * (self.max_rl > 0) as usize
		} else {
			self.write_null(repetition_index, self.max_dl - 1)
		}
	}

	pub fn append_array(&mut self, repetition_index1: &LevelIndexList, values: &[TPg]) -> usize {
		let mut len = 0;
		for value in values {
			len += self.append(repetition_index1, value);
		}
		len
	}

	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> usize {
		debug_assert!(level < self.max_dl);

		self.dls.push(level);
		if self.max_rl > 0 {
			let rl = self.repetition_index.copy_and_diff(repetition_index);
			self.rls.push(rl);
			4
		} else {
			2
		}
	}

	fn write_column(&mut self, writer: &mut SerializedColumnWriter) -> Result<(), ParquetError> {
		let dls = if self.max_dl > 0 { Some(self.dls.as_slice()) } else { None };
		let rls = if self.max_rl > 0 { Some(self.rls.as_slice()) } else { None };

		let writer_t = writer.typed::<FixedLenByteArrayType>();

		if self.byte_buffer.len() == 0 {
			assert_eq!(0, self.byte_buffer.len());
			writer_t.write_batch(&[], dls, rls)?;
			self.dls.clear();
			self.rls.clear();
			return Ok(());
		}

		let mut byte_array = Vec::new();
		std::mem::swap(&mut self.byte_buffer, &mut byte_array);
		let byte_array = Bytes::from(byte_array);

		let mut column: Vec<FixedLenByteArray> = vec![FixedLenByteArray::default(); byte_array.len() / self.length];
		for i in 0..column.len() {
			let b: Bytes = byte_array.slice(i * self.length..(i + 1) * self.length);
			column[i] = FixedLenByteArray::from(ByteArray::from(b));
		}

		let _num_written = writer_t.write_batch(&column, dls, rls)?;
		let buffer_length = byte_array.len();
		std::mem::drop((column, byte_array));
		self.byte_buffer.reserve(buffer_length);

		assert_eq!(0, self.byte_buffer.len());
		self.dls.clear();
		self.rls.clear();

		Ok(())
	}
}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut [u8]) -> Option<usize>> ColumnAppenderBase for FixedByteArrayColumnAppender<TPg, FCopyTo> {

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
		Ok(self.write_null(repetition_index, level))
	}

	fn max_dl(&self) -> i16 { self.max_dl }
	fn max_rl(&self) -> i16 { self.max_rl }
}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut [u8]) -> Option<usize>> ColumnAppender<TPg> for FixedByteArrayColumnAppender<TPg, FCopyTo> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<TPg>) -> Result<usize, String> {
		let byte_size = self.append(repetition_index, value.as_ref());
		
		Ok(byte_size)
	}
}
