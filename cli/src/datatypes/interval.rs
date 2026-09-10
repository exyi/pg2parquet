use byteorder::{ReadBytesExt, BigEndian, WriteBytesExt, LittleEndian};
use parquet::data_type::FixedLenByteArray;
use postgres::types::FromSql;

use crate::myfrom::MyFrom;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PgInterval {
	pub microseconds: i64,
	pub days: i32,
	pub months: i32
}

impl<'a> FromSql<'a> for PgInterval {
	fn from_sql(_ty: &postgres::types::Type, mut raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let time = raw.read_i64::<BigEndian>()?;
		let day = raw.read_i32::<BigEndian>()?;
		let month = raw.read_i32::<BigEndian>()?;
		Ok(PgInterval { microseconds: time, days: day, months: month })
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		ty == &postgres::types::Type::INTERVAL
	}
}
impl PgInterval {
	pub fn to_parquet_bytes(&self) -> [u8; 12] {
		// Parquet INTERVAL type:
		// This data is composed of three separate little endian unsigned integers. Each stores a component of a duration of time. The first integer identifies the number of months associated with the duration, the second identifies the number of days associated with the duration and the third identifies the number of milliseconds associated with the provided duration. This duration of time is independent of any particular timezone or date.

		// Postgres interval has microsecond resolution, parquet only milliseconds
		// plus postgres doesn't overflow the seconds into the day field
		let ms_per_day = 1000 * 60 * 60 * 24;
		let millis_total = self.microseconds / 1000;
		let days = millis_total / ms_per_day;
		let millis = millis_total % ms_per_day;
		let mut b = [0u8; 12];
		b[0..4].copy_from_slice(&i32::to_le_bytes(self.months));
		b[4..8].copy_from_slice(&i32::to_le_bytes(self.days + days as i32));
		b[8..12].copy_from_slice(&i32::to_le_bytes(millis as i32));
		b
	}
}
