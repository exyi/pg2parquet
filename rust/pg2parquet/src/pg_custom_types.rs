use std::sync::Arc;

use postgres::types::{FromSql, Kind};


pub struct PgEnum {
	pub name: String,
	pub case: i64
}

impl<'a> FromSql<'a> for PgEnum {
    fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let s = String::from_utf8(raw.to_vec())?;
		let case =
			match ty.kind() {
				Kind::Enum(cases) =>
					cases.iter()
						.position(|c| c == &s)
						.map(|x| x as i64)
						.unwrap_or(-1),
				_ => -1
			};
        Ok(PgEnum {
			name: s,
			case
		})
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        match ty.kind() {
			Kind::Enum(_) => true,
			_ => false
		}
    }
}
