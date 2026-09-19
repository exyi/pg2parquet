use std::marker::PhantomData;

use postgres::types::FromSql;

use crate::{pg_custom_types::PgAbstractRow};

use super::{ColumnAppender, ColumnAppenderBase};

pub struct BasicPgRowColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	column_i: usize,
	appender: TInner,
	_dummy: PhantomData<TPg>
}

impl<TPg, TInner> BasicPgRowColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	pub fn new(column_i: usize, appender: TInner) -> Self {
		BasicPgRowColumnAppender {
			column_i,
			appender,
			_dummy: PhantomData
		}
	}
}

impl<TPg, TInner> ColumnAppenderBase for BasicPgRowColumnAppender<TPg, TInner>
	where TInner: ColumnAppender<TPg> {
	fn write_null(&mut self) -> Result<usize, String> {
		self.appender.write_null()
	}

	fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
		self.appender.to_arrow_array()
	}
}

impl<TPg, TAppender, TRow: PgAbstractRow> ColumnAppender<TRow> for BasicPgRowColumnAppender<TPg, TAppender>
	where TPg: for<'a> FromSql<'a>, TAppender: ColumnAppender<TPg> {
	fn copy_value(&mut self, reader: &TRow) -> Result<usize, String> {
		let v = reader.ab_get::<Option<TPg>>(self.column_i);

		self.appender.copy_value_opt(&v)
	}
}

pub trait PgBorrowHelper {
	type SqlValue<'a>: FromSql<'a>;
	type Value: ?Sized;

	fn as_value<'a>(value: &'a Self::SqlValue<'_>) -> &'a Self::Value;
}

pub struct PgBorrowRef<T: ?Sized>(PhantomData<Box<T>>);

impl<T: ?Sized + 'static> PgBorrowHelper for PgBorrowRef<T>
where
	for<'a> &'a T: FromSql<'a>,
{
	type SqlValue<'a> = &'a T;
	type Value = T;

	fn as_value<'a>(value: &'a Self::SqlValue<'_>) -> &'a Self::Value {
		value
	}
}

/// Read reference from pg Row and passes that to appenders
pub struct BorrowedPgRowColumnAppender<TBorrow: PgBorrowHelper, TInner: ColumnAppender<TBorrow::Value>>
{
	column_i: usize,
	appender: TInner,
	_borrowed: PhantomData<TBorrow>,
}

impl<TBorrow: PgBorrowHelper, TInner: ColumnAppender<TBorrow::Value>> BorrowedPgRowColumnAppender<TBorrow, TInner>
{
	pub fn new(column_i: usize, appender: TInner) -> Self {
		Self {
			column_i,
			appender,
			_borrowed: PhantomData,
		}
	}
}

impl<TBorrow: PgBorrowHelper, TInner: ColumnAppender<TBorrow::Value>> ColumnAppenderBase for BorrowedPgRowColumnAppender<TBorrow, TInner>
{
	fn write_null(&mut self) -> Result<usize, String> {
		self.appender.write_null()
	}

	fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
		self.appender.to_arrow_array()
	}
}

impl<TRow: PgAbstractRow, TBorrow: PgBorrowHelper, TInner: ColumnAppender<TBorrow::Value>> ColumnAppender<TRow> for BorrowedPgRowColumnAppender<TBorrow, TInner>
{
	fn copy_value(&mut self, reader: &TRow) -> Result<usize, String> {
		let value = reader.ab_get::<Option<TBorrow::SqlValue<'_>>>(self.column_i);
		match value {
			Some(value) => self.appender.copy_value(TBorrow::as_value(&value)),
			None => self.write_null(),
		}
	}
}
