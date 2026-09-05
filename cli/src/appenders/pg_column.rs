use std::{marker::PhantomData, sync::Arc, borrow::Cow};

use postgres::types::FromSql;

use crate::{pg_custom_types::PgAbstractRow, level_index::LevelIndexList};

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
	fn write_null(&mut self, repetition_index: &LevelIndexList, level: i16) -> Result<usize, String> {
		// println!("write_null: level: {}, max_rl: {}", level, self.column.max_rl());
		debug_assert_eq!(repetition_index.level, self.appender.max_rl());
		self.appender.write_null(repetition_index, level)
	}

	fn max_dl(&self) -> i16 { self.appender.max_dl() }

	fn max_rl(&self) -> i16 { self.appender.max_rl() }

	fn to_arrow_array(&mut self, target: &arrow::datatypes::DataType) -> Result<arrow::array::ArrayRef, String> {
		self.appender.to_arrow_array(target)
	}
}

impl<TPg, TAppender, TRow: PgAbstractRow + Clone> ColumnAppender<TRow> for BasicPgRowColumnAppender<TPg, TAppender>
	where TPg: for<'a> FromSql<'a> + Clone, TAppender: ColumnAppender<TPg> {

	fn copy_value(&mut self, repetition_index: &LevelIndexList, reader: Cow<TRow>) -> Result<usize, String> {
		debug_assert_eq!(repetition_index.level, self.appender.max_rl());

		let v = reader.ab_get::<Option<TPg>>(self.column_i);

		self.appender.copy_value_opt(repetition_index, Cow::Owned(v))
	}
}
