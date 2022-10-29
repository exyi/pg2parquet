use postgres::types::FromSql;

use crate::myfrom::MyFrom;


pub struct PgMoney {
	pub amount: i64
}

impl<'a> FromSql<'a> for PgMoney {
    fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut b = [0u8; 8];
		b.copy_from_slice(raw);
		let amount = i64::from_be_bytes(b);
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
