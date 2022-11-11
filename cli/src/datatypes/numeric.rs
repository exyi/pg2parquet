use parquet::data_type::{ByteArray, ByteArrayType};
use pg_bigdecimal::{PgNumeric, BigDecimal};

use crate::{column_appender::{ColumnAppender, GenericColumnAppender}, level_index::LevelIndexList, column_pg_copier::DynamicSerializedWriter, myfrom::MyFrom};


fn convert_decimal_to_bytes(d: BigDecimal, scale: i32, precision: u32) -> Vec<u8> {
	let dd = d.with_prec(precision as u64).with_scale(scale as i64);
	let (int, exp) = dd.into_bigint_and_exponent();
	debug_assert_eq!(exp, scale as i64);
	int.to_signed_bytes_be()
}

pub fn new_decimal_bytes_appender(max_dl: i16, max_rl: i16, precision: u32, scale: i32) -> impl ColumnAppender<PgNumeric> {
	let inner: GenericColumnAppender<Vec<u8>, ByteArrayType, _> = GenericColumnAppender::new(max_dl, max_rl, |v| MyFrom::my_from(v));
	DecimalBytesAppender {
		inner,
		precision,
		scale,
	}
}

#[derive(Clone)]
struct DecimalBytesAppender<TInner: ColumnAppender<Vec<u8>>> {
	inner: TInner,
	precision: u32,
	scale: i32,
}

impl<TInner: ColumnAppender<Vec<u8>>> ColumnAppender<PgNumeric> for DecimalBytesAppender<TInner> {
	fn copy_value(&mut self, repetition_index: &LevelIndexList, value: PgNumeric) -> Result<usize, String> {
		let bytes = value.n.map(|n| convert_decimal_to_bytes(n, self.scale, self.precision));
		self.inner.copy_value_opt(repetition_index, bytes)
	}
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		self.inner.write_null(repetition_index, level)
	}
	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn DynamicSerializedWriter) -> Result<(), String> {
		self.inner.write_columns(column_i, next_col)
	}
	fn max_dl(&self) -> i16 { self.inner.max_dl() }
	fn max_rl(&self) -> i16 { self.inner.max_rl() }
}
