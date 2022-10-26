use std::marker::PhantomData;

use parquet::data_type::BoolType;
use postgres::types::FromSql;

use crate::{column_appender::{ColumnAppender, GenericColumnAppender}, pg_custom_types::{PgRawRange, PgAbstractRow}};

pub struct RangeColumnAppender<TElement, TInner> where TInner: ColumnAppender<TElement> {
	lower_col: TInner,
	upper_col: TInner,
	is_empty_col: GenericColumnAppender<bool, BoolType, fn(bool) -> bool>, // TODO: how to use this Fn(bool) -> bool?
	lower_incl_col: GenericColumnAppender<bool, BoolType, fn(bool) -> bool>,
	upper_incl_col: GenericColumnAppender<bool, BoolType, fn(bool) -> bool>,
	_dummy1: PhantomData<TElement>,
}

impl<TElement, TInner> RangeColumnAppender<TElement, TInner> where TInner: ColumnAppender<TElement> {
	pub fn new(column: TInner) -> Self {
		let max_dl = column.max_dl();
		let max_rl = column.max_rl();
		RangeColumnAppender {
			lower_col: column.clone(),
			upper_col: column,
			is_empty_col: GenericColumnAppender::new(max_dl, max_rl, |x| x),
			lower_incl_col: GenericColumnAppender::new(max_dl, max_rl, |x| x),
			upper_incl_col: GenericColumnAppender::new(max_dl, max_rl, |x| x),
			_dummy1: PhantomData,
		}
	}
}

impl<TElement, TInner> Clone for RangeColumnAppender<TElement, TInner> where TInner: ColumnAppender<TElement> {
	fn clone(&self) -> Self {
		RangeColumnAppender {
			lower_col: self.lower_col.clone(),
			upper_col: self.upper_col.clone(),
			is_empty_col: self.is_empty_col.clone(),
			lower_incl_col: self.lower_incl_col.clone(),
			upper_incl_col: self.upper_incl_col.clone(),
			_dummy1: PhantomData,
		}
	}
}

impl<TElement: for <'a> FromSql<'a>, TInner: ColumnAppender<TElement>> ColumnAppender<PgRawRange> for RangeColumnAppender<TElement, TInner> {
	fn copy_value(&mut self, repetition_index: &crate::level_index::LevelIndexList, value: PgRawRange) -> Result<usize, String> {
		let mut count = 0;
		count += self.lower_col.copy_value_opt(repetition_index, value.ab_get(0))?;
		count += self.upper_col.copy_value_opt(repetition_index, value.ab_get(1))?;
		count += self.is_empty_col.copy_value(repetition_index, value.is_empty)?;
		count += self.lower_incl_col.copy_value(repetition_index, value.lower_inclusive)?;
		count += self.upper_incl_col.copy_value(repetition_index, value.lower_inclusive)?;
		Ok(count)
	}

	fn write_null(&mut self, repetition_index: &crate::level_index::LevelIndexList, level: i16) -> Result<usize, String> {
		let mut count = 0;
		count += self.lower_col.write_null(repetition_index, level)?;
		count += self.upper_col.write_null(repetition_index, level)?;
		count += self.is_empty_col.write_null(repetition_index, level)?;
		count += self.lower_incl_col.write_null(repetition_index, level)?;
		count += self.upper_incl_col.write_null(repetition_index, level)?;
		Ok(count)
	}

	fn write_columns<'b>(&mut self, column_i: usize, next_col: &mut dyn crate::column_pg_copier::DynamicSerializedWriter) -> Result<(), String> {
		self.lower_col.write_columns(column_i, next_col)?;
		self.upper_col.write_columns(column_i + 1, next_col)?;
		self.is_empty_col.write_columns(column_i + 2, next_col)?;
		self.lower_incl_col.write_columns(column_i + 3, next_col)?;
		self.upper_incl_col.write_columns(column_i + 4, next_col)?;
		Ok(())
	}

	fn max_dl(&self) -> i16 {
		self.is_empty_col.max_dl() - 1
	}

	fn max_rl(&self) -> i16 {
		self.is_empty_col.max_rl()
	}
}

