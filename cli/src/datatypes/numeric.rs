use std::borrow::Cow;

use parquet::data_type::{ByteArray, ByteArrayType};
use pg_bigdecimal::{PgNumeric, BigDecimal, BigInt};
use bigdecimal::ToPrimitive;

use crate::appenders::{ByteArrayColumnAppender, ColumnAppender, ColumnAppenderBase, PreprocessAppender, PreprocessExt, PrimitiveParquetType, UnwrapOptionAppender, new_autoconv_byte_array_binary_appender, new_autoconv_generic_appender};
use crate::myfrom::MyFrom;
// Arrow export
use arrow::array::{ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType as ArrowDataType;


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

pub fn new_decimal_bytes_appender(precision: u32, scale: i32) -> impl ColumnAppender<PgNumeric> {
	let inner = ByteArrayColumnAppender::<Vec<u8>, _>::new(|v| &v);
	PreprocessAppender::new(UnwrapOptionAppender::new(inner), move |value: Cow<PgNumeric>| {
		match &value.n {
			Some(n) => Cow::Owned(Some(convert_decimal_to_bytes(n, scale, precision))),
			None => Cow::Owned(None)
		}
	})
}

pub fn new_decimal_int_appender<Int: TryFrom<BigInt> + Clone, TPq: PrimitiveParquetType>(precision: u32, scale: i32) -> impl ColumnAppender<PgNumeric>
	where Int::Error: std::fmt::Display,
		TPq::T: Clone + crate::appenders::RealMemorySize,
		TPq::T: MyFrom<Int>{
	let inner = UnwrapOptionAppender::new(new_autoconv_generic_appender::<Int, TPq>());
	PreprocessAppender::new(inner, move |value: Cow<PgNumeric>| {
		match &value.n {
			Some(n) => Cow::Owned(convert_decimal_to_int(n, scale, precision)),
			None => Cow::Owned(None),
		}
	})
}

impl MyFrom<PgNumeric> for f64 {
    fn my_from(t: PgNumeric) -> Self {
        match t.n {
			Some(n) => n.to_string().parse().unwrap(), // for some reason the to_f64 method works poorly (looses more precision)
			// Some(n) => n.to_f64().unwrap(),
			None => f64::NAN,
		}
    }
}
impl MyFrom<PgNumeric> for f32 {
    fn my_from(t: PgNumeric) -> Self {
        match t.n {
			Some(n) => n.to_string().parse().unwrap(),
			// Some(n) => n.to_f32().unwrap(),
			None => f32::NAN,
		}
    }
}
