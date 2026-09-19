use postgres::types::FromSql;

use crate::appenders::PgBorrowHelper;


pub struct PgRawBytes<'a>(&'a [u8]);

impl<'a> FromSql<'a> for PgRawBytes<'a> {
    fn from_sql(_ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(PgRawBytes(raw))
    }

    fn accepts(_ty: &postgres::types::Type) -> bool {
        true
    }
}

pub struct PgRawBytesBorrow;

impl PgBorrowHelper for PgRawBytesBorrow {
    type SqlValue<'a> = PgRawBytes<'a>;
    type Value = [u8];

    fn as_value<'a>(value: &'a Self::SqlValue<'_>) -> &'a Self::Value {
        value.0
    }
}


