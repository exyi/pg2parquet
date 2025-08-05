use std::{sync::Arc, any::TypeId, io::Read};

use byteorder::{ReadBytesExt, BigEndian};
use postgres::types::{FromSql, Kind, WrongType, Field};
use postgres::binary_copy::BinaryCopyOutRow;
use postgres_protocol::types as pgtypes;

fn read_pg_len(bytes: &[u8]) -> i32 {
	let mut x = [0u8; 4];
	x.copy_from_slice(&bytes[0..4]);
	return i32::from_be_bytes(x);
}

#[derive(Debug, Clone)]
pub struct PgEnum {
	pub name: String
}

impl<'a> FromSql<'a> for PgEnum {
	fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		debug_assert!(match ty.kind() { Kind::Enum(_) => true, _ => false });
		let s = String::from_utf8(raw.to_vec())?;
		Ok(PgEnum { name: s })
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		match ty.kind() {
			Kind::Enum(_) => true,
			_ => false
		}
	}
}

#[derive(Debug, Clone)]
pub struct PgAny {
	pub ty: postgres::types::Type,
	pub value: Vec<u8>
}
impl<'a> FromSql<'a> for PgAny {
	fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		match ty.kind() {
			Kind::Array(_) => panic!("Nooo {}", ty),
			_ => {}
		};
		Ok(PgAny {
			ty: ty.clone(),
			value: raw.to_vec()
		})
	}

	fn accepts(_ty: &postgres::types::Type) -> bool { true }
}
#[derive(Debug, Clone)]
pub struct PgAnyRef<'a> {
	pub ty: postgres::types::Type,
	pub value: &'a [u8]
}
impl<'b, 'a: 'b> FromSql<'a> for PgAnyRef<'b> {
	fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(PgAnyRef {
			ty: ty.clone(),
			value: raw
		})
	}

	fn accepts(_ty: &postgres::types::Type) -> bool { true }
}

// #[derive(Debug, Clone)]
// pub struct PgTypedRef<'a, const TYPE_OID: u32> {
// 	pub value: &'a [u8]
// }
// impl<'b, 'a: 'b, const TYPE_OID: u32> FromSql<'a> for PgTypedRef<'b, TYPE_OID> {
// 	fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
// 		postgres::types::Type::TEXT.oid()
// 		debug_assert_eq!(TYPE_OID, ty.oid());
// 		Ok(PgTypedRef {
// 			value: raw
// 		})
// 	}

// 	fn accepts(_ty: &postgres::types::Type) -> bool { true }
// }

#[derive(Debug, Clone)]
pub struct PgRawRange {
	pub element_type: postgres::types::Type,
	pub lower: Option<Vec<u8>>,
	pub upper: Option<Vec<u8>>,
	pub lower_inclusive: bool,
	pub upper_inclusive: bool,
	pub is_empty: bool
}

fn read_byte_vec(raw: &mut &[u8], len: usize) -> Result<Vec<u8>, Box<dyn std::error::Error + Sync + Send>> {
	let mut buf = vec![0u8; len];
	raw.read_exact(&mut buf)?;
	Ok(buf)
}

impl<'a> FromSql<'a> for PgRawRange {
	fn from_sql(ty: &postgres::types::Type, mut raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let element_type = match ty.kind() {
			Kind::Range(inner_t) => inner_t.clone(),
			_ => panic!("Not a range type")
		};
		// /* A range's flags byte contains these bits: */
		// #define RANGE_EMPTY         0x01    /* range is empty */
		// #define RANGE_LB_INC        0x02    /* lower bound is inclusive */
		// #define RANGE_UB_INC        0x04    /* upper bound is inclusive */
		// #define RANGE_LB_INF        0x08    /* lower bound is -infinity */
		// #define RANGE_UB_INF        0x10    /* upper bound is +infinity */
		// #define RANGE_LB_NULL       0x20    /* lower bound is null (NOT USED) */
		// #define RANGE_UB_NULL       0x40    /* upper bound is null (NOT USED) */
		// #define RANGE_CONTAIN_EMPTY 0x80/* marks a GiST internal-page entry whose
		// 								 * subtree contains some empty ranges */
		// A range has no lower bound if any of RANGE_EMPTY, RANGE_LB_INF (or RANGE_LB_NULL, not used anymore) is set. The same applies for upper bounds.
		let flags = raw.read_u8()?;
		let is_empty = flags & 0x01 != 0;
		let lower_inclusive = flags & 0x02 != 0;
		let upper_inclusive = flags & 0x04 != 0;
		let lower_inf = flags & 0x08 != 0;
		let upper_inf = flags & 0x10 != 0;
		let lower_null = flags & 0x20 != 0;
		let upper_null = flags & 0x40 != 0;

		let lower = if is_empty || lower_inf || lower_null {
			None
		} else {
			let len = raw.read_i32::<BigEndian>()?;
			if len < 0 {
				None
			} else {
				Some(read_byte_vec(&mut raw, len as usize)?)
			}
		};
		let upper = if is_empty || upper_inf || upper_null {
			None
		} else {
			let len = raw.read_i32::<BigEndian>()?;
			if len < 0 {
				None
			} else {
				Some(read_byte_vec(&mut raw, len as usize)?)
			}
		};
		assert_eq!(0, raw.len()); // Nothing should be remaining in the buffer

		if is_empty {
			Ok(PgRawRange { element_type, lower: None, upper: None, lower_inclusive: false, upper_inclusive: false, is_empty: true })
		} else {
			Ok(PgRawRange { element_type, lower, upper, lower_inclusive, upper_inclusive, is_empty: false })
		}
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		match ty.kind() {
			Kind::Range(_) => true,
			_ => false
		}
	}
}
#[derive(Debug, Clone)]
pub struct PgRawRecord {
	pub ty: postgres::types::Type,
	data: Vec<u8>,
	fields: Vec<Option<usize>>
}

impl<'a> FromSql<'a> for PgRawRecord {
    fn from_sql(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		// println!("Record type: {:?}, bytes: {:?}", ty, raw);
        let fields = match ty.kind() {
			Kind::Composite(fields) => fields,
			_ => return Err("Not a record/composite type".into())
		};

		let mut index = 0;
		let num_cols = read_pg_len(&raw[index..]) as usize;
		index += 4;
		assert!(num_cols <= fields.len());
		let data_buffer = raw[index..].to_vec();
		index = 0;
		let mut values = Vec::with_capacity(num_cols);
		for field_i in 0..num_cols {
			// println!("Reading field {}, bytes {:?}", fields[field_i].name(), &raw[index..]);
			let oid = read_pg_len(&data_buffer[index..]) as u32;
			index += 4;
			debug_assert_eq!(oid, fields[field_i].type_().oid());
			let len = read_pg_len(&data_buffer[index..]);
			// println!("Reading field {}: {}, len {}", fields[field_i].name(), oid, len);
			if len < 0 {
				values.push(None);
				index += 4;
			} else {
				values.push(Some(index));
				index += 4 + len as usize;
			}
		}

		Ok(PgRawRecord {
			ty: ty.clone(),
			data: data_buffer,
			fields: values
		})
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
		match ty.kind() {
			Kind::Composite(_) => true,
			_ => false
		}
    }
}

// const ZERO_BUFFER: &[u8] = &[0u8; 128];
// const DEFAULT_JSONB: &[u8] = &[0, 0, 0, 1, '{' as u8, '}' as u8];

// fn pg_hack_default_value<'a, T: FromSql<'a>>(ty: &postgres::types::Type) -> Option<T> {
// 	if ty.name() == "jsonb" {
// 		T::from_sql(ty, DEFAULT_JSONB).ok()
// 	} else if ty.name() == "json" {
// 		T::from_sql(ty, &DEFAULT_JSONB[4..]).ok()
// 	} else {
// 		T::from_sql(ty, ZERO_BUFFER).ok()
// 	}
// }


pub trait PgAbstractRow {
	fn ab_get<'a, T: FromSql<'a>>(&'a self, index: usize) -> T;
	fn ab_len(&self) -> usize;
}

impl PgAbstractRow for postgres::Row {
	fn ab_get<'a, T: FromSql<'a>>(&'a self, index: usize) -> T {
		self.get(index)
	}

	fn ab_len(&self) -> usize {
		self.len()
	}
}

impl<'b> PgAbstractRow for PgRawRange {
    fn ab_get<'a, T: FromSql<'a>>(&'a self, index: usize) -> T {
		// println!("ab_get: {:?} {:?}", index, &self);
		let r = match index {
			0 => self.lower.as_ref(),
			1 => self.upper.as_ref(),
			_ => panic!("Invalid index")
		};
		assert!(T::accepts(&self.element_type));
		T::from_sql_nullable(&self.element_type, r.map(|x| &x[..])).unwrap()
	}

    fn ab_len(&self) -> usize {
		5
    }
}

impl PgAbstractRow for PgRawRecord {
    fn ab_get<'a, T: FromSql<'a>>(&'a self, index: usize) -> T {
		// println!("ab_get: {:?} {:?}", index, &self);
		let f = match self.ty.kind() {
			Kind::Composite(fields) => &fields[index],
			_ => unreachable!()
		};
		assert!(T::accepts(f.type_()));
		if self.fields.len() < index {
			return T::from_sql_null(f.type_()).unwrap()
		}
		match &self.fields[index] {
			None => T::from_sql_null(f.type_()).unwrap(),
			Some(x) => {
				let len = read_pg_len(&self.data[*x..]) as usize;
				T::from_sql(f.type_(), &self.data[*x+4 .. x+4+len]).unwrap()
			}
		}
	}

    fn ab_len(&self) -> usize {
		match self.ty.kind() {
			Kind::Composite(fields) => fields.len(),
			_ => unreachable!()
		}
    }
}


impl PgAbstractRow for PgAny {
	fn ab_get<'a, T: FromSql<'a>>(&'a self, index: usize) -> T {
		debug_assert_eq!(0, index);
		T::from_sql(&self.ty, &self.value).unwrap()
	}

	fn ab_len(&self) -> usize { 1 }
}

impl<'b> PgAbstractRow for PgAnyRef<'b> {
	fn ab_get<'a, T: FromSql<'a>>(&'a self, index: usize) -> T {
		debug_assert_eq!(0, index);
		T::from_sql(&self.ty, &self.value).unwrap()
	}

	fn ab_len(&self) -> usize { 1 }
}

impl<TRow: PgAbstractRow> PgAbstractRow for Arc<TRow> {
    fn ab_get<'a, T: postgres::types::FromSql<'a>>(&'a self, index: usize) -> T {
        self.as_ref().ab_get(index)
    }

    fn ab_len(&self) -> usize {
        self.as_ref().ab_len()
    }
}

impl PgAbstractRow for BinaryCopyOutRow {
    fn ab_get<'a, T: FromSql<'a>>(&'a self, index: usize) -> T {
        self.get(index)
    }

    fn ab_len(&self) -> usize {
        // ab_len is not used in the current implementation
        0
    }
}

pub struct UnclonableHack<T>(pub T);

impl<T> Clone for UnclonableHack<T> {
	fn clone(&self) -> Self {
		panic!("Cloning of type {} is disabled", std::any::type_name::<T>())
	}
}

impl<TRow: PgAbstractRow> PgAbstractRow for UnclonableHack<TRow> {
    fn ab_get<'a, T: postgres::types::FromSql<'a>>(&'a self, index: usize) -> T {
        self.0.ab_get(index)
    }

    fn ab_len(&self) -> usize {
        self.0.ab_len()
    }
}
