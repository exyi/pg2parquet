use byteorder::{ReadBytesExt, BigEndian, ByteOrder};
use postgres::types::FromSql;
use pgvector::{HalfVector, Vector, SparseVector};
use half::f16;

use crate::myfrom::MyFrom;

#[derive(Debug, Clone)]
pub struct PgF32Vector {
	pub data: Vector
}

#[derive(Debug, Clone)]
pub struct PgF16Vector {
	pub data: HalfVector
}

#[derive(Debug, Clone)]
pub struct PgSparseVector {
	pub data: Vec<(i32, f32)>
}

impl<'a> FromSql<'a> for PgF32Vector {
	fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(PgF32Vector { data: Vector::from_sql(ty, raw)? })
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		Vector::accepts(ty)
	}
}

impl<'a> FromSql<'a> for PgF16Vector {
	fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(PgF16Vector { data: HalfVector::from_sql(ty, raw)? })
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		HalfVector::accepts(ty)
	}
}
impl<'a> FromSql<'a> for PgSparseVector {
	fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let data = SparseVector::from_sql(ty, raw)?;
		Ok(PgSparseVector {
			data: data.indices().iter().copied().zip(data.values().iter().copied()).collect()
		})
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		ty.name() == "sparsevec"
	}
}

impl<'a> IntoIterator for PgF32Vector {
    type Item = f32;
    type IntoIter = <Vec<f32> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.to_vec().into_iter() // TODO: remove copy
    }
}

impl<'a> IntoIterator for &'a PgF32Vector {
    type Item = &'a f32;
    type IntoIter = std::slice::Iter<'a, f32>;

    fn into_iter(self) -> Self::IntoIter { self.data.as_slice().iter() }
}

impl IntoIterator for PgF16Vector {
    type Item = f16;
    type IntoIter = <Vec<f16> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.to_vec().into_iter()
    }
}

impl<'a> IntoIterator for &'a PgF16Vector {
    type Item = &'a f16;
    type IntoIter = std::slice::Iter<'a, f16>;

    fn into_iter(self) -> Self::IntoIter { self.data.as_slice().iter() }
}

impl IntoIterator for PgSparseVector {
	type Item = (i32, f32);
	type IntoIter = std::vec::IntoIter<(i32, f32)>;

	fn into_iter(self) -> Self::IntoIter {
		self.data.into_iter()
	}
}

impl<'a> IntoIterator for &'a PgSparseVector {
    type Item = &'a (i32, f32);
    type IntoIter = std::slice::Iter<'a, (i32, f32)>;

    fn into_iter(self) -> Self::IntoIter { self.data.iter() }
}
