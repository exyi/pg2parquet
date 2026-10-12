use postgres::types::{FromSql, Type};

use crate::appenders::PgBorrowHelper;


// TODO: upstream? Should be just matter of adding xml to the allowed &str types
#[derive(Debug, Clone, Copy)]
pub struct PgXml<'a> {
	pub data: &'a str,
}

impl<'a> FromSql<'a> for PgXml<'a> {
    fn from_sql(_ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let str = <&str>::from_sql(&Type::TEXT, raw)?;
		Ok(PgXml { data: str })
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        ty == &postgres::types::Type::XML
    }
}

pub struct PgXmlBorrow;

impl PgBorrowHelper for PgXmlBorrow {
	type SqlValue<'a> = PgXml<'a>;
	type Value = str;

	fn as_value<'a>(value: &'a Self::SqlValue<'_>) -> &'a Self::Value {
		value.data
	}
}

