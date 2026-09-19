use postgres::types::{FromSql, Type};

use crate::appenders::PgBorrowHelper;

#[derive(Debug, Clone, Copy)]
pub struct PgRawJsonb<'a> {
	pub data: &'a str,
}

impl<'a> FromSql<'a> for PgRawJsonb<'a> {
    fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		if ty == &postgres::types::Type::JSON {
			let str = <&str>::from_sql(&Type::TEXT, raw)?;
			Ok(PgRawJsonb { data: str })
		} else {
			let Some((&version, json)) = raw.split_first() else {
				return Err("JSONB value has no version byte".into());
			};
			if version != 1 {
				return Err(format!("Unknown JSONB version {version}").into());
			}
			let str = <&str>::from_sql(&Type::TEXT, json)?;
			Ok(PgRawJsonb { data: str })
		}
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        ty == &postgres::types::Type::JSONB || ty == &postgres::types::Type::JSON
    }
}

pub struct PgRawJsonbBorrow;

impl PgBorrowHelper for PgRawJsonbBorrow {
	type SqlValue<'a> = PgRawJsonb<'a>;
	type Value = str;

	fn as_value<'a>(value: &'a Self::SqlValue<'_>) -> &'a Self::Value {
		value.data
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn json_borrows_the_original_text() {
		let raw = b"{\"answer\":42}";
		let json = PgRawJsonb::from_sql(&Type::JSON, raw).unwrap();
		assert_eq!(json.data, "{\"answer\":42}");
		assert_eq!(json.data.as_ptr(), raw.as_ptr());
	}

	#[test]
	fn jsonb_borrows_the_payload_after_the_version() {
		let raw = b"\x01{\"answer\":42}";
		let json = PgRawJsonb::from_sql(&Type::JSONB, raw).unwrap();
		assert_eq!(json.data, "{\"answer\":42}");
		assert_eq!(json.data.as_ptr(), raw[1..].as_ptr());
		assert!(PgRawJsonb::from_sql(&Type::JSONB, b"").is_err());
		assert!(PgRawJsonb::from_sql(&Type::JSONB, b"\x02{}").is_err());
	}
}
