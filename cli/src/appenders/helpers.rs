use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use super::{ColumnAppender, ColumnAppenderBase};

pub struct UnwrapOptionAppender<T: Clone, Appender2: ColumnAppender<T>> {
    appender: Appender2,
    _dummy: PhantomData<T>
}
impl<T: Clone, Appender2: ColumnAppender<T>> UnwrapOptionAppender<T, Appender2> {
    pub fn new(appender: Appender2) -> Self {
        UnwrapOptionAppender { appender, _dummy: PhantomData }
    }
}
impl<T: Clone, Appender2: ColumnAppender<T>> ColumnAppenderBase for UnwrapOptionAppender<T, Appender2> {
    fn write_null(&mut self) -> Result<usize, String> {
        self.appender.write_null()
    }

    fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
        self.appender.to_arrow_array()
    }
}
impl<T: Clone, Appender2: ColumnAppender<T>> ColumnAppender<Option<T>> for UnwrapOptionAppender<T, Appender2> {
    fn copy_value(&mut self, value: Cow<Option<T>>) -> Result<usize, String> {
        self.appender.copy_value_opt(value)
    }
}

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
    fn write_null(&mut self) -> Result<usize, String> {
        self.appender.write_null()
    }

    fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
        self.appender.to_arrow_array()
    }
}
impl<T1: Clone, T2: Clone, Appender2: ColumnAppender<T2>, F: Fn(Cow<T1>) -> Cow<T2>> ColumnAppender<T1> for PreprocessAppender<T1, T2, Appender2, F> {
    fn copy_value(&mut self, value: Cow<T1>) -> Result<usize, String> {
        self.appender.copy_value((self.f)(value))
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
	fn write_null(&mut self) -> Result<usize, String> {
		self.inner.write_null()
	}

    fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
        self.inner.to_arrow_array()
    }
}
impl<T: Clone, TInner: ColumnAppender<Arc<T>>> ColumnAppender<T> for RcWrapperAppender<T, TInner> {
	fn copy_value(&mut self, value: Cow<T>) -> Result<usize, String> {
		let arc = Arc::new(value.into_owned());
		let cow = Cow::Owned(arc);
		self.inner.copy_value(cow)
	}
}

pub trait RcWrapperExt<TInner: ColumnAppender<Arc<T>>, T> {
    fn rc_wrapper(self) -> RcWrapperAppender<T, TInner>;
}
impl<TInner: ColumnAppender<Arc<T>>, T> RcWrapperExt<TInner, T> for TInner {
    fn rc_wrapper(self) -> RcWrapperAppender<T, TInner> {
        RcWrapperAppender::new(self)
    }
}
