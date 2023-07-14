use parquet::data_type::FixedLenByteArray;
use postgres::types::FromSql;

use crate::myfrom::MyFrom;

use super::utils;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PgInterval {
	pub microseconds: i64,
	pub day: i32,
	pub month: i32
}

impl<'a> FromSql<'a> for PgInterval {
	fn from_sql(_ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let time = utils::read_i64(&raw[0..8]);
		let day = utils::read_i32(&raw[8..12]);
		let month = utils::read_i32(&raw[12..16]);
		Ok(PgInterval { microseconds: time, day, month })
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		ty == &postgres::types::Type::INTERVAL
	}
}

impl MyFrom<PgInterval> for FixedLenByteArray {
	fn my_from(t: PgInterval) -> Self {
		// Parquet INTERVAL type:
		// This data is composed of three separate little endian unsigned integers. Each stores a component of a duration of time. The first integer identifies the number of months associated with the duration, the second identifies the number of days associated with the duration and the third identifies the number of milliseconds associated with the provided duration. This duration of time is independent of any particular timezone or date.

		// Postgres interval has microsecond resolution, parquet only milliseconds
		// plus postgres doesn't overflow the seconds into the day field
		let ms_per_day = 1000 * 60 * 60 * 24;
		let millis_total = t.microseconds / 1000;
		let days = millis_total / ms_per_day;
		let millis = millis_total % ms_per_day;
		let mut b = [0u8; 12];
		b[0..4].copy_from_slice(&i32::to_le_bytes(t.month));
		b[4..8].copy_from_slice(&i32::to_le_bytes(t.day + days as i32));
		b[8..12].copy_from_slice(&i32::to_le_bytes(millis as i32));
		FixedLenByteArray::from(b.to_vec())
	}
}
