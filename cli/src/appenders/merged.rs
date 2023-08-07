use std::{sync::Arc, borrow::Cow, marker::PhantomData};

use crate::{postgres_cloner::DynRowAppender, level_index::LevelIndexList, myfrom::MyFrom};

use super::{ColumnAppenderBase, ColumnAppender, DynamicSerializedWriter, PreprocessExt, PreprocessAppender, new_autoconv_generic_appender, RealMemorySize, GenericColumnAppender};

pub struct DynamicMergedAppender<T> {
	columns: Vec<DynRowAppender<T>>,
	max_dl: i16,
	max_rl: i16
}

impl<T> DynamicMergedAppender<T> {
	pub fn new(columns: Vec<DynRowAppender<T>>, max_dl: i16, max_rl: i16) -> Self {
		DynamicMergedAppender { columns, max_dl, max_rl }
	}
}

impl<T> ColumnAppenderBase for DynamicMergedAppender<T> {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		let mut total = 0;
		for c in self.columns.iter_mut() {
			total += c.write_null(repetition_index, level)?;
		}
		Ok(total)
	}

	fn write_columns<'b>(&mut self, _column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		for (i, c) in self.columns.iter_mut().enumerate() {
			c.write_columns(i, next_col)?;
		}
		Ok(())
	}

	fn max_dl(&self) -> i16 {
		self.max_dl
	}

	fn max_rl(&self) -> i16 {
		self.max_rl
	}
}

impl<T> ColumnAppender<Arc<T>> for DynamicMergedAppender<T> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: Cow<Arc<T>>) -> Result<usize, String> {
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

    fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
        self.next.write_columns(column_i, next_col)?;
        self.appender.write_columns(column_i, next_col)?;
        Ok(())
    }

    fn max_dl(&self) -> i16 {
        self.next.max_dl()
    }

    fn max_rl(&self) -> i16 {
        self.next.max_rl()
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

    fn write_columns<'b>(&mut self, _column_i: usize, _next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
        Ok(())
    }

    fn max_dl(&self) -> i16 { self.max_dl }

    fn max_rl(&self) -> i16 { self.max_rl }
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
//     fn write_columns<'b>(&mut self, _column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String>;
// }

// struct StaticMergedAppenderCoreImpl<T, TNext: 
