use parquet::data_type::ByteArray;
use postgres::types::{FromSql, Type};

use crate::myfrom::MyFrom;

pub struct PgRawJsonb {
	pub data: String,
}

impl<'a> FromSql<'a> for PgRawJsonb {
    fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		if ty == &postgres::types::Type::JSON {
			let str = String::from_sql(&Type::TEXT, raw)?;
			Ok(PgRawJsonb { data: str })
		} else {
			let version = raw[0];
			match version {
				1 => {
					let str = String::from_sql(&Type::TEXT, &raw[1..])?;
					Ok(PgRawJsonb { data: str })
				},
				_ => panic!("Unknown jsonb version {}", version)
			}
		}
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        ty == &postgres::types::Type::JSONB || ty == &postgres::types::Type::JSON
    }
}

impl MyFrom<PgRawJsonb> for ByteArray {
	fn my_from(t: PgRawJsonb) -> Self {
		ByteArray::from(t.data.into_bytes())
	}
}
