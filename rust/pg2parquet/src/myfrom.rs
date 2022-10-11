use std::ops::Sub;

use chrono::Datelike;
use parquet::data_type::{ByteArray, FixedLenByteArray};

use crate::pg_custom_types::PgEnum;

pub trait MyFrom<T> {
	fn my_from(t: T) -> Self;
}

impl<T> MyFrom<T> for T {
	fn my_from(t: T) -> Self {
		t
	}
}

impl MyFrom<i32> for i64 {
	fn my_from(t: i32) -> Self {
		t as i64
	}
}

impl MyFrom<i16> for i32 {
	fn my_from(t: i16) -> Self {
		t as i32
	}
}

impl MyFrom<i8> for i32 {
	fn my_from(t: i8) -> Self {
		t as i32
	}
}

impl MyFrom<u32> for i32 {
	fn my_from(t: u32) -> Self {
		t as i32
	}
}

impl MyFrom<Vec<u8>> for ByteArray {
	fn my_from(t: Vec<u8>) -> Self {
		ByteArray::from(t)
	}
}
impl MyFrom<String> for ByteArray {
	fn my_from(t: String) -> Self {
		ByteArray::from(t.into_bytes())
	}
}

impl MyFrom<&str> for ByteArray {
	fn my_from(t: &str) -> Self {
		ByteArray::from(t)
	}
}

impl MyFrom<chrono::DateTime<chrono::Utc>> for i64 {
	fn my_from(t: chrono::DateTime<chrono::Utc>) -> Self {
		t.timestamp_micros()
	}
}

impl MyFrom<chrono::NaiveDateTime> for i64 {
	fn my_from(t: chrono::NaiveDateTime) -> Self {
		t.timestamp_micros()
	}
}

impl MyFrom<chrono::NaiveDate> for i32 {
	fn my_from(t: chrono::NaiveDate) -> Self {
		// number of days since 1970-01-01
		t.sub(chrono::NaiveDate::from_ymd(1970, 1, 1)).num_days() as i32
	}
}

impl MyFrom<chrono::NaiveTime> for i64 {
	fn my_from(t: chrono::NaiveTime) -> Self {
		// number of seconds since 00:00:00
		t.signed_duration_since(chrono::NaiveTime::from_hms(0, 0, 0)).num_microseconds().unwrap()
	}
}

impl MyFrom<uuid::Uuid> for FixedLenByteArray {
	fn my_from(t: uuid::Uuid) -> Self {
		FixedLenByteArray::from(t.as_bytes().to_vec())
	}
}

impl<'a> MyFrom<PgEnum> for i32 {
    fn my_from(t: PgEnum) -> Self {
		t.case as i32
    }
}

impl<'a> MyFrom<PgEnum> for ByteArray {
    fn my_from(t: PgEnum) -> Self {
		ByteArray::from(t.name.into_bytes())
    }
}
