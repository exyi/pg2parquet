use std::{sync::Arc, borrow::Cow, marker::PhantomData};

use crate::{level_index::LevelIndexList, myfrom::MyFrom};

use super::{ColumnAppenderBase, ColumnAppender, PreprocessExt, PreprocessAppender, new_autoconv_generic_appender, RealMemorySize, GenericColumnAppender, DynColumnAppender};
// Arrow: compose child arrays into StructArray for struct/group columns
use arrow::array::{ArrayRef, BinaryDictionaryBuilder, StructArray};
use arrow::datatypes::DataType as ArrowDataType;

pub struct DynamicMergedAppender<T> {
	columns: Vec<DynColumnAppender<T>>,
	max_dl: i16,
	max_rl: i16,
	// Track parent-level presence per row so we can set Struct nulls correctly in Arrow
	present: Vec<bool>,
}

impl<T> DynamicMergedAppender<T> {
	pub fn new(columns: Vec<DynColumnAppender<T>>, max_dl: i16, max_rl: i16) -> Self {
		DynamicMergedAppender { columns, max_dl, max_rl, present: Vec::new() }
	}
}

impl<T> ColumnAppenderBase for DynamicMergedAppender<T> {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		// Any null forwarded to this struct corresponds to a struct row placeholder
		self.present.push(false);
		let mut total = 0;
		for c in self.columns.iter_mut() {
			// For list children, force a null list row (so Arrow List validity matches parent struct null)
			// Heuristic: lists have max_rl() > 0
			total += c.write_null(repetition_index, level)?;
		}
		Ok(total)
	}

	fn max_dl(&self) -> i16 {
		self.max_dl
	}

	fn max_rl(&self) -> i16 {
		self.max_rl
	}

	fn to_arrow_array(&mut self, target: &arrow::datatypes::DataType) -> Result<arrow::array::ArrayRef, String> {
		// Only support Struct targets here; defer to children otherwise
		let ArrowDataType::Struct(fields) = target else { return Err("Arrow STRUCT export: unsupported target type".to_string()) };
		if fields.len() != self.columns.len() {
			return Err(format!("Struct field count mismatch: target has {}, appenders {}", fields.len(), self.columns.len()));
		}

		// Build child arrays in order
		let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
		for (i, col) in self.columns.iter_mut().enumerate() {
			let dt = fields[i].data_type();
			let arr = col.to_arrow_array(dt)?;
			child_arrays.push(arr);
		}
		// Derive struct-level validity from tracked presence
		let rows = if child_arrays.is_empty() { self.present.len() } else { child_arrays[0].len() };
		if rows != self.present.len() {
			return Err(format!("Struct row count mismatch: children={}, present={}", rows, self.present.len()));
		}
		let validity = if self.present.contains(&false) { Some(arrow::buffer::NullBuffer::from(self.present.as_slice())) } else { None };

		// Build a Fields list from the provided Struct data type for construction
		let arrow_fields = fields.clone();
		let sa = StructArray::try_new(arrow_fields, child_arrays, validity)
			.map_err(|e| e.to_string())?;
		// Clear presence after successful export to avoid leaking state across batches
		self.present.clear();
	Ok(std::sync::Arc::new(sa) as ArrayRef)
	}

	fn clear(&mut self) {
		for c in self.columns.iter_mut() { c.clear(); }
		self.present.clear();
	}
}

impl<T: Clone> ColumnAppender<T> for DynamicMergedAppender<T> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: Cow<T>) -> Result<usize, String> {
		// Record a non-null struct row
		self.present.push(true);
		let mut total = 0;
		let reader_r = reader.as_ref();
		for c in self.columns.iter_mut() {
			total += c.copy_value(repetition_index, Cow::Borrowed(reader_r))?;
		}
		Ok(total)
	}
}

pub fn new_static_merged_appender<T: Clone>(max_dl: i16, max_rl: i16) -> impl StaticMergedAppender<T> {
    StaticMergedAppenderNil { max_dl, max_rl }
}

pub trait StaticMergedAppender<T: Clone>: ColumnAppender<T> {
    fn add_appender<A: ColumnAppender<T>>(self, appender: A) -> StaticMergedAppenderImpl<T, A, Self>
        where Self: Sized {
        StaticMergedAppenderImpl { appender, next: self, _dummy: PhantomData }
    }
    fn add_appender_map<A: ColumnAppender<T2>, T2: Clone, F: Fn(Cow<T>) -> Cow<T2>>(self, appender: A, f: F) -> StaticMergedAppenderImpl<T, PreprocessAppender<T, T2, A, F>, Self>
        where Self: Sized {
        StaticMergedAppender::add_appender(self, appender.preprocess(f))
    }

    // fn add_primitive_column<TPq: parquet::data_type::DataType, T2: Clone, F: Fn(Cow<T>) -> Cow<T2>>(self, max_dl: i16, max_rl: i16, f: F) -> StaticMergedAppenderImpl<T, PreprocessAppender<T, T2, GenericColumnAppender<T2, TPq, impl Fn(TPg) -> TPq::T>, F>, Self>
    //     where Self: Sized,
    //           TPq::T : MyFrom<T2> + RealMemorySize {
    //     let appender = new_autoconv_generic_appender::<T2, TPq>(max_dl, max_rl);
    //     StaticMergedAppender::add_appender_map(self, appender, f)
    // }
}

pub struct StaticMergedAppenderImpl<T: Clone, TAppender: ColumnAppender<T>, Next: ColumnAppender<T>> {
    pub appender: TAppender,
    pub next: Next,
    pub _dummy: PhantomData<T>
}

impl<T: Clone, TAppender: ColumnAppender<T>, Next: ColumnAppender<T>> ColumnAppenderBase for StaticMergedAppenderImpl<T, TAppender, Next> {
    fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
        let x = self.next.write_null(repetition_index, level)?;
        let y = self.appender.write_null(repetition_index, level)?;
        Ok(x + y)
    }

    fn max_dl(&self) -> i16 {
        self.next.max_dl()
    }

    fn max_rl(&self) -> i16 {
        self.next.max_rl()
    }

    fn clear(&mut self) {
        self.next.clear();
        self.appender.clear();
    }
}

impl<T: Clone, TAppender: ColumnAppender<T>, Next: ColumnAppender<T>> ColumnAppender<T> for StaticMergedAppenderImpl<T, TAppender, Next> {
    fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: Cow<T>) -> Result<usize, String> {
        let x = self.next.copy_value(repetition_index, Cow::Borrowed(reader.as_ref()))?;
        let y = self.appender.copy_value(repetition_index, reader)?;
        Ok(x + y)
    }
}

impl<T: Clone, TAppender: ColumnAppender<T>, Next: ColumnAppender<T>> StaticMergedAppender<T> for StaticMergedAppenderImpl<T, TAppender, Next> {}

pub struct StaticMergedAppenderNil {
    pub max_dl: i16,
    pub max_rl: i16
}

impl ColumnAppenderBase for StaticMergedAppenderNil {
    fn write_null(&mut self, _repetition_index: &LevelIndexList, _level: i16) -> Result<usize, String> {
        Ok(0)
    }

    fn max_dl(&self) -> i16 { self.max_dl }

    fn max_rl(&self) -> i16 { self.max_rl }

    fn clear(&mut self) {}
}

impl<T: Clone> ColumnAppender<T> for StaticMergedAppenderNil {
    fn copy_value(&mut self, _repetition_index: &LevelIndexList, _reader: Cow<T>) -> Result<usize, String> {
        Ok(0)
    }
}

impl<T: Clone> StaticMergedAppender<T> for StaticMergedAppenderNil {}


// trait StaticMergedAppenderCore<T> {
//     fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: Cow<Arc<T>>) -> Result<usize, String>;
//     fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String>;
// }

// struct StaticMergedAppenderCoreImpl<T, TNext: 
