use std::marker::PhantomData;

use super::{ColumnAppender, ColumnAppenderBase};

pub struct UnwrapOptionAppender<T, Appender2: ColumnAppender<T>> {
    appender: Appender2,
    _dummy: PhantomData<T>
}
impl<T, Appender2: ColumnAppender<T>> UnwrapOptionAppender<T, Appender2> {
    pub fn new(appender: Appender2) -> Self {
        UnwrapOptionAppender { appender, _dummy: PhantomData }
    }
}
impl<T, Appender2: ColumnAppender<T>> ColumnAppenderBase for UnwrapOptionAppender<T, Appender2> {
    fn write_null(&mut self) -> Result<usize, String> {
        self.appender.write_null()
    }

    fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
        self.appender.to_arrow_array()
    }
}
impl<T, Appender2: ColumnAppender<T>> ColumnAppender<Option<T>> for UnwrapOptionAppender<T, Appender2> {
    fn copy_value(&mut self, value: &Option<T>) -> Result<usize, String> {
        self.appender.copy_value_opt(value)
    }
}

pub struct PreprocessAppender<T1, T2, Appender2: ColumnAppender<T2>, F: for<'a> Fn(&'a T1) -> T2> {
    appender: Appender2,
    f: F,
    _dummy: PhantomData<(T1, T2)>
}
impl<T1, T2, Appender2: ColumnAppender<T2>, F: for<'a> Fn(&'a T1) -> T2> PreprocessAppender<T1, T2, Appender2, F> {
    pub fn new(appender: Appender2, f: F) -> Self {
        PreprocessAppender { appender, f, _dummy: PhantomData }
    }
}
impl<T1, T2, Appender2: ColumnAppender<T2>, F: for<'a> Fn(&'a T1) -> T2> ColumnAppenderBase for PreprocessAppender<T1, T2, Appender2, F> {
    fn write_null(&mut self) -> Result<usize, String> {
        self.appender.write_null()
    }

    fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
        self.appender.to_arrow_array()
    }
}
impl<T1, T2, Appender2: ColumnAppender<T2>, F: for<'a> Fn(&'a T1) -> T2> ColumnAppender<T1> for PreprocessAppender<T1, T2, Appender2, F> {
    fn copy_value(&mut self, value: &T1) -> Result<usize, String> {
        let value = (self.f)(value);
        self.appender.copy_value(&value)
    }
}

pub struct PreprocessRefAppender<T1, T2, Appender2: ColumnAppender<T2>, F: for<'a> Fn(&'a T1) -> &'a T2> {
    appender: Appender2,
    f: F,
    _dummy: PhantomData<(T1, T2)>
}
impl<T1, T2, Appender2: ColumnAppender<T2>, F: for<'a> Fn(&'a T1) -> &'a T2> ColumnAppenderBase for PreprocessRefAppender<T1, T2, Appender2, F> {
    fn write_null(&mut self) -> Result<usize, String> {
        self.appender.write_null()
    }

    fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
        self.appender.to_arrow_array()
    }
}
impl<T1, T2, Appender2: ColumnAppender<T2>, F: for<'a> Fn(&'a T1) -> &'a T2> ColumnAppender<T1> for PreprocessRefAppender<T1, T2, Appender2, F> {
    fn copy_value(&mut self, value: &T1) -> Result<usize, String> {
        self.appender.copy_value((self.f)(value))
    }
}

pub trait PreprocessExt<T2, Appender2: ColumnAppender<T2>> {
    fn preprocess<T1, F: for<'a> Fn(&'a T1) -> T2>(self, f: F) -> PreprocessAppender<T1, T2, Appender2, F>;
    fn preprocess_ref<T1, F: for<'a> Fn(&'a T1) -> &'a T2>(self, f: F) -> PreprocessRefAppender<T1, T2, Appender2, F>;
}
impl<T2, Appender2: ColumnAppender<T2>> PreprocessExt<T2, Appender2> for Appender2 {
    fn preprocess<T1, F: for<'a> Fn(&'a T1) -> T2>(self, f: F) -> PreprocessAppender<T1, T2, Appender2, F> {
        PreprocessAppender::new(self, f)
    }
    fn preprocess_ref<T1, F: for<'a> Fn(&'a T1) -> &'a T2>(self, f: F) -> PreprocessRefAppender<T1, T2, Appender2, F> {
        PreprocessRefAppender { appender: self, f, _dummy: PhantomData }
    }
}
