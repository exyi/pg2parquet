use postgres::types::FromSql;

use crate::myfrom::MyFrom;

use super::utils;


pub struct PgMoney {
	pub amount: i64
}

impl<'a> FromSql<'a> for PgMoney {
    fn from_sql(_ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let amount = utils::read_i64(&raw);
		Ok(PgMoney { amount })
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        ty == &postgres::types::Type::MONEY
    }
}

impl MyFrom<PgMoney> for i64 {
	fn my_from(t: PgMoney) -> Self {
		t.amount
	}
}
