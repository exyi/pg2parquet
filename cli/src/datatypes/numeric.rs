use std::borrow::Cow;

use parquet::data_type::{ByteArray, ByteArrayType};
use pg_bigdecimal::{PgNumeric, BigDecimal, BigInt};

use crate::appenders::{GenericColumnAppender, ColumnAppender, ColumnAppenderBase, DynamicSerializedWriter, new_autoconv_generic_appender, PreprocessExt, PreprocessAppender, UnwrapOptionAppender};
use crate::level_index::LevelIndexList;
use crate::myfrom::MyFrom;


fn convert_decimal_to_bytes(d: &BigDecimal, scale: i32, precision: u32) -> Vec<u8> {
	let dd = d.with_prec(precision as u64).with_scale(scale as i64);
	let (int, exp) = dd.into_bigint_and_exponent();
	debug_assert_eq!(exp, scale as i64);
	int.to_signed_bytes_be()
}

pub fn convert_decimal_to_int<Int: TryFrom<BigInt>>(d: &BigDecimal, scale: i32, precision: u32) -> Option<Int>
	where Int::Error: std::fmt::Display {
	debug_assert!(precision <= 18);
	let dd = d.with_prec(precision as u64).with_scale(scale as i64);
	let (int, exp) = dd.into_bigint_and_exponent();
	debug_assert_eq!(exp, scale as i64);
	int.try_into().map_err(|err| {
		eprintln!("Error converting decimal number {}, the value is replaced by NULL: {}", d, err)
	}).ok()
}

pub fn new_decimal_bytes_appender(max_dl: i16, max_rl: i16, precision: u32, scale: i32) -> impl ColumnAppender<PgNumeric> {
	let inner: GenericColumnAppender<Vec<u8>, ByteArrayType, _> = new_autoconv_generic_appender(max_dl, max_rl);
	DecimalBytesAppender {
		inner,
		precision,
		scale,
	}
}

pub fn new_decimal_int_appender<Int: TryFrom<BigInt> + Clone, TPq: parquet::data_type::DataType>(max_dl: i16, max_rl: i16, precision: u32, scale: i32) -> impl ColumnAppender<PgNumeric>
	where Int::Error: std::fmt::Display,
		TPq::T: Clone + crate::appenders::RealMemorySize,
		TPq::T: MyFrom<Int>{
	let inner = UnwrapOptionAppender::new(new_autoconv_generic_appender::<Int, TPq>(max_dl, max_rl));
	PreprocessAppender::new(inner, move |value: Cow<PgNumeric>| {
		match &value.n {
			Some(n) => Cow::Owned(convert_decimal_to_int(n, scale, precision)),
			None => Cow::Owned(None),
		}
	})
}

#[derive(Clone)]
struct DecimalBytesAppender<TInner: ColumnAppender<Vec<u8>>> {
	inner: TInner,
	precision: u32,
	scale: i32,
}

impl<TInner: ColumnAppender<Vec<u8>>> ColumnAppenderBase for DecimalBytesAppender<TInner> {
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		self.inner.write_null(repetition_index, level)
	}
	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		self.inner.write_columns(column_i, next_col)
	}
	fn max_dl(&self) -> i16 { self.inner.max_dl() }
	fn max_rl(&self) -> i16 { self.inner.max_rl() }
}

impl<TInner: ColumnAppender<Vec<u8>>> ColumnAppender<PgNumeric> for DecimalBytesAppender<TInner> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<PgNumeric>) -> Result<usize, String> {
		let value = value.as_ref();
		let bytes = match &value.n {
			Some(n) => Some(convert_decimal_to_bytes(n, self.scale, self.precision)),
			None => None,
		};
		self.inner.copy_value_opt(repetition_index, Cow::Owned(bytes))
	}
}

// #[derive(Clone)]
// struct DecimalIntAppender<TInt: TryFrom<BigInt>, TInner: ColumnAppender<i64>>
// 	where TInt::Error: std::fmt::Display {
// 	inner: TInner,
// 	precision: u32,
// 	scale: i32,
// }

// impl<TInner: ColumnAppender<i64>> ColumnAppenderBase for DecimalIntAppender<TInner> {
// 	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
// 		self.inner.write_null(repetition_index, level)
// 	}
// 	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
// 		self.inner.write_columns(column_i, next_col)
// 	}
// 	fn max_dl(&self) -> i16 { self.inner.max_dl() }
// 	fn max_rl(&self) -> i16 { self.inner.max_rl() }
// }

// impl<TInner: ColumnAppender<i64>> ColumnAppender<PgNumeric> for DecimalIntAppender<TInner> {
// 	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: Cow<PgNumeric>) -> Result<usize, String> {
// 		let value = value.as_ref();
// 		let int = match &value.n {
// 			Some(n) => convert_decimal_to_int(n, self.scale, self.precision),
// 			None => None,
// 		};
// 		self.inner.copy_value_opt(repetition_index, Cow::Owned(int))
// 	}
// }
