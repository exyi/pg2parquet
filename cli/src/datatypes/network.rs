use std::net::IpAddr;

use parquet::data_type::ByteArray;
use postgres::types::{FromSql, Type};

use crate::myfrom::MyFrom;

#[derive(Debug, Clone)]
pub struct PgNetwork {
	address: IpAddr,
	prefix_len: u8,
	is_cidr: bool,
}

impl PgNetwork {
	fn address_width(&self) -> u8 {
		match self.address {
			IpAddr::V4(_) => 32,
			IpAddr::V6(_) => 128,
		}
	}

	fn to_postgres_text(&self) -> String {
		let address = self.address.to_string();
		if self.is_cidr || self.prefix_len != self.address_width() {
			format!("{address}/{}", self.prefix_len)
		} else {
			address
		}
	}
}

impl<'a> FromSql<'a> for PgNetwork {
	fn from_sql(
		ty: &Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let inet = postgres_protocol::types::inet_from_sql(raw)?;
		Ok(Self {
			address: inet.addr(),
			prefix_len: inet.netmask(),
			is_cidr: ty == &Type::CIDR,
		})
	}

	fn accepts(ty: &Type) -> bool {
		ty == &Type::INET || ty == &Type::CIDR
	}
}

impl MyFrom<PgNetwork> for ByteArray {
	fn my_from(network: PgNetwork) -> Self {
		ByteArray::from(network.to_postgres_text().into_bytes())
	}
}
