use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use crate::level_index::LevelIndexList;

use super::{ColumnAppender, ColumnAppenderBase, DynamicSerializedWriter};

pub struct PreprocessAppender<T1: Clone, T2: Clone, Appender2: ColumnAppender<T2>, F: Fn(Cow<T1>) -> Cow<T2>> {
    appender: Appender2,
    f: F,
    _dummy: PhantomData<(T1, T2)>
}
impl<T1: Clone, T2: Clone, Appender2: ColumnAppender<T2>, F: Fn(Cow<T1>) -> Cow<T2>> PreprocessAppender<T1, T2, Appender2, F> {
    pub fn new(appender: Appender2, f: F) -> Self {
        PreprocessAppender { appender, f, _dummy: PhantomData }
    }
}
impl<T1: Clone, T2: Clone, Appender2: ColumnAppender<T2>, F: Fn(Cow<T1>) -> Cow<T2>> ColumnAppenderBase for PreprocessAppender<T1, T2, Appender2, F> {
    fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
        self.appender.write_null(repetition_index, level)
    }

    fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
        self.appender.write_columns(column_i, next_col)
    }

    fn max_dl(&self) -> i16 {
        self.appender.max_dl()
    }

    fn max_rl(&self) -> i16 {
        self.appender.max_rl()
    }
}
impl<T1: Clone, T2: Clone, Appender2: ColumnAppender<T2>, F: Fn(Cow<T1>) -> Cow<T2>> ColumnAppender<T1> for PreprocessAppender<T1, T2, Appender2, F> {
    fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<T1>) -> Result<usize, String> {
        self.appender.copy_value(repetition_index, (self.f)(value))
    }
}
pub trait PreprocessExt<T2: Clone, Appender2: ColumnAppender<T2>> {
    fn preprocess<T1: Clone, F: Fn(Cow<T1>) -> Cow<T2>>(self, f: F) -> PreprocessAppender<T1, T2, Appender2, F>;
}
impl<T2: Clone, Appender2: ColumnAppender<T2>> PreprocessExt<T2, Appender2> for Appender2 {
    fn preprocess<T1: Clone, F: Fn(Cow<T1>) -> Cow<T2>>(self, f: F) -> PreprocessAppender<T1, T2, Appender2, F> {
        PreprocessAppender::new(self, f)
    }
}

pub struct RcWrapperAppender<T, TInner: ColumnAppender<Arc<T>>> {
	pub inner: TInner,
	pub dummy: PhantomData<T>
}
impl<T, TInner: ColumnAppender<Arc<T>>> RcWrapperAppender<T, TInner> {
	pub fn new(inner: TInner) -> Self {
		Self { inner, dummy: PhantomData }
	}
}
impl<T, TInner: ColumnAppender<Arc<T>>> ColumnAppenderBase for RcWrapperAppender<T, TInner> {
	fn write_null(&mut self, repetition_index: &crate::level_index::LevelIndexList, level: i16) -> Result<usize, String> {
		self.inner.write_null(repetition_index, level)
	}

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn crate::appenders::DynamicSerializedWriter) -> Result<(), String> {
		self.inner.write_columns(column_i, next_col)
	}

	fn max_dl(&self) -> i16 { self.inner.max_dl() }

	fn max_rl(&self) -> i16 { self.inner.max_rl() }
}
impl<T: Clone, TInner: ColumnAppender<Arc<T>>> ColumnAppender<T> for RcWrapperAppender<T, TInner> {
	fn copy_value(&mut self, repetition_index: &crate::level_index::LevelIndexList, value: Cow<T>) -> Result<usize, String> {
		let arc = Arc::new(value.into_owned());
		let cow = Cow::Owned(arc);
		self.inner.copy_value(repetition_index, cow)
	}
}
