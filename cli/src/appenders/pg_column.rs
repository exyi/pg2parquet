use std::{marker::PhantomData, sync::Arc, borrow::Cow};

use postgres::types::FromSql;

use crate::{pg_custom_types::PgAbstractRow};

use super::{ColumnAppender, ColumnAppenderBase};

pub struct BasicPgRowColumnAppender<TPg, TInner>
	where TPg: Clone, TInner: ColumnAppender<TPg> {
	column_i: usize,
	appender: TInner,
	_dummy: PhantomData<TPg>
}

impl<TPg, TInner> BasicPgRowColumnAppender<TPg, TInner>
	where TPg: Clone, TInner: ColumnAppender<TPg> {
	pub fn new(column_i: usize, appender: TInner) -> Self {
		BasicPgRowColumnAppender {
			column_i,
			appender,
			_dummy: PhantomData
		}
	}
}

impl<TPg, TInner> ColumnAppenderBase for BasicPgRowColumnAppender<TPg, TInner>
	where TPg: Clone,  TInner: ColumnAppender<TPg> {
	fn write_null(&mut self) -> Result<usize, String> {
		self.appender.write_null()
	}

	fn to_arrow_array(&mut self) -> Result<arrow::array::ArrayRef, String> {
		self.appender.to_arrow_array()
	}
}

impl<TPg, TAppender, TRow: PgAbstractRow + Clone> ColumnAppender<TRow> for BasicPgRowColumnAppender<TPg, TAppender>
	where TPg: for<'a> FromSql<'a> + Clone, TAppender: ColumnAppender<TPg> {
	fn copy_value(&mut self, reader: Cow<TRow>) -> Result<usize, String> {
		let v = reader.ab_get::<Option<TPg>>(self.column_i);

		self.appender.copy_value_opt(Cow::Owned(v))
	}
}
