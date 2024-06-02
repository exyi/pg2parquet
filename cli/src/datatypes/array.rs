use std::vec;

use parquet::data_type::ByteArray;
use postgres::{fallible_iterator::FallibleIterator, types::{FromSql, Kind, Type}};
use postgres_protocol::types::array_from_sql;

use crate::myfrom::MyFrom;

#[derive(Debug, Clone)]
pub struct PgMultidimArray<T> {
    pub data: Vec<T>,
    pub dims: Option<Vec<i32>>,
    pub lower_bounds: PgMultidimArrayLowerBounds
}

#[derive(Debug, Clone)]
pub enum PgMultidimArrayLowerBounds {
    Const(i32),
    PerDim(Vec<i32>),
}

impl<'a, T> FromSql<'a> for PgMultidimArray<T> where T: FromSql<'a> {
    fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let array = array_from_sql(raw)?;
        let mut dims_iter = array.dimensions();
        let (count, dims, lower_bounds) = if let Some(dim1) = dims_iter.next()? {
            if let Some(dim2) = dims_iter.next()? {
                let mut dims: Vec<i32> = vec![dim1.len, dim2.len];
                let mut count = dim1.len * dim2.len;
                let mut lb = vec![dim1.lower_bound, dim2.lower_bound];
                for dim in dims_iter.iterator() {
                    let dim = dim?;
                    count *= dim.len;
                    dims.push(dim.len);
                    lb.push(dim.lower_bound);
                }
                (count, Some(dims), PgMultidimArrayLowerBounds::PerDim(lb))
            } else {
                (dim1.len, None, PgMultidimArrayLowerBounds::Const(dim1.lower_bound))
            }
        } else {
            (0, None, PgMultidimArrayLowerBounds::Const(1))
        };

        let mut data: Vec<T> = Vec::with_capacity(count as usize);
        for elem in array.values().iterator() {
            let elem = elem?;
            data.push(T::from_sql_nullable(member_type, elem)?);
        }

        Ok(PgMultidimArray { data, dims, lower_bounds })
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        matches!(ty.kind(), Kind::Array(_))
    }
}
