use std::{borrow::Cow, iter::Map};

use bit_vec::IntoIter;
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
	pub data: SparseVector
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
        Ok(PgSparseVector { data: SparseVector::from_sql(ty, raw)? })
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

impl IntoIterator for PgF16Vector {
    type Item = f16;
    type IntoIter = <Vec<f16> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.to_vec().into_iter()
    }
}

impl IntoIterator for PgSparseVector {
    type Item = (i32, f32);
    type IntoIter = SparseVecIter<'static>;

    fn into_iter(self) -> Self::IntoIter {
        if self.data.indices().len() > i32::MAX as usize {
            panic!();
        }

        SparseVecIter {
            vec: Cow::Owned(self.data),
            index: 0
        }
    }
}

pub struct SparseVecIter<'a> {
    vec: Cow<'a, SparseVector>,
    index: i32
}

impl Iterator for SparseVecIter<'_> {
    type Item = (i32, f32);

    fn next(&mut self) -> Option<Self::Item> {
        if (self.index as usize) < self.vec.indices().len() {
            let result = (self.vec.indices()[self.index as usize], self.vec.values()[self.index as usize]);
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}
