use std::{ops::Sub, net::IpAddr};

use bit_vec::BitVec;
use chrono::{Datelike, format::Fixed};
use parquet::data_type::{ByteArray, FixedLenByteArray, Int64Type};
use pg_bigdecimal::{PgNumeric, BigDecimal};

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
		t.sub(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
	}
}

impl MyFrom<chrono::NaiveTime> for i64 {
	fn my_from(t: chrono::NaiveTime) -> Self {
		// number of seconds since 00:00:00
		t.signed_duration_since(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap()).num_microseconds().unwrap()
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

impl<'a> MyFrom<eui48::MacAddress> for ByteArray {
	fn my_from(t: eui48::MacAddress) -> Self {
		let str = t.to_hex_string();
		ByteArray::from(str.into_bytes())
	}
}
impl<'a> MyFrom<eui48::MacAddress> for FixedLenByteArray {
	fn my_from(t: eui48::MacAddress) -> Self {
		let b = t.as_bytes();
		FixedLenByteArray::from(b.to_vec())
	}
}
impl<'a> MyFrom<eui48::MacAddress> for i64 {
	fn my_from(t: eui48::MacAddress) -> Self {
		let mut b = [0u8; 8];
		b[0..6].copy_from_slice(t.as_bytes());
		i64::from_be_bytes(b)
	}
}
impl<'a> MyFrom<IpAddr> for ByteArray {
	fn my_from(t: IpAddr) -> Self {
		let str = t.to_string();
		ByteArray::from(str.into_bytes())
	}
}
impl<'a> MyFrom<BitVec> for ByteArray {
	fn my_from(t: BitVec) -> Self {
		// this format should be easiest to work with, and parquet should compress it anyway
		let str = t.iter().map(|b| if b { '1' } else { '0' }).collect::<String>();
		ByteArray::from(str.into_bytes())
	}
}
