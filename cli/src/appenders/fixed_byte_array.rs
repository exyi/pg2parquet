use std::{marker::PhantomData, sync::Arc, borrow::Cow};


use crate::{appenders::byte_array, level_index::{LevelIndexList, LevelIndexState}, myfrom::MyFrom, pg_custom_types::{PgAbstractRow, PgAnyRef}};

use super::{real_memory_size::RealMemorySize, ColumnAppenderBase, ColumnAppender};

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

}

impl<TPg: Clone, FCopyTo: Fn(&TPg, &mut [u8]) -> Option<usize>> ColumnAppenderBase for FixedByteArrayColumnAppender<TPg, FCopyTo> {

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
