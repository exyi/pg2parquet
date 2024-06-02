use std::{marker::PhantomData, sync::Arc, borrow::Cow};

use parquet::{column::writer::ColumnWriter, data_type::DataType, errors::ParquetError, file::writer::SerializedColumnWriter, schema::types::ColumnDescriptor};

use crate::{level_index::{LevelIndexState, LevelIndexList}, myfrom::MyFrom};

use super::{real_memory_size::RealMemorySize, ColumnAppenderBase, ColumnAppender, DynamicSerializedWriter};


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

impl<TPg, TPq, FConversion> ColumnAppenderBase for GenericColumnAppender<TPg, TPq, FConversion>
	where TPq::T: Clone + RealMemorySize, TPq: DataType, FConversion: Fn(TPg) -> TPq::T {

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		let mut error = None;
		let mut col_descriptor: Option<(Arc<ColumnDescriptor>, u64, u64)> = None;
		let c = next_col.next_column(&mut |mut column| {
			let result = self.write_column(&mut column);
			col_descriptor = Some(get_column_descriptor(&mut column));
			let error1 = result.err();
			let result2 = column.close();

			error = error1.or(result2.err());
			
		}).map_err(|e| format!("Could not create column[{}]: {}", column_i, e))?;

		debug_assert!(col_descriptor.is_some());
		debug_assert_eq!(col_descriptor.as_ref().unwrap().0.max_def_level(), self.max_dl);
		debug_assert_eq!(col_descriptor.as_ref().unwrap().0.max_rep_level(), self.max_rl);

		if error.is_some() {
			let col_name = col_descriptor.map(|(desc, _, _)| desc.path().string()).unwrap_or_else(|| format!("column[{}]", column_i));
			return Err(format!("Couldn't write data of {}: {}", col_name, error.unwrap()));
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

fn get_column_descriptor(column: &mut SerializedColumnWriter) -> (Arc<ColumnDescriptor>, u64, u64) {
	match column.untyped() {
		ColumnWriter::BoolColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
		ColumnWriter::Int32ColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
		ColumnWriter::Int64ColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
		ColumnWriter::Int96ColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
		ColumnWriter::FloatColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
		ColumnWriter::DoubleColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
		ColumnWriter::ByteArrayColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
		ColumnWriter::FixedLenByteArrayColumnWriter(x) => (x.get_descriptor().clone(), x.get_total_rows_written(), x.get_total_bytes_written()),
	}
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
