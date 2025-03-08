use std::{cmp::max, collections::HashSet, fmt};

use bigdecimal::{BigDecimal, FromPrimitive, RoundingMode, Zero};
use chrono::format::Parsed;
use pg_bigdecimal::BigInt;
use postgres::types::{Kind, Type as PgType};

pub fn type_needs_details(t: &PgType) -> bool {
	match t.kind() {
		Kind::Array(x) => type_needs_details(x),
		Kind::Domain(x) => type_needs_details(x),
		Kind::Enum(_) => false,
		Kind::Range(x) => type_needs_details(x),
		Kind::Multirange(x) => type_needs_details(x),
		Kind::Composite(_) => false,
		Kind::Simple =>
			matches!(t.name(), "numeric" | "geography" | "geometry" | "bit" | "bpchar"),
		_ => false
	}
}


pub fn collect_custom_types(t: &PgType, composites: &mut HashSet<PgType>, domains: &mut HashSet<PgType>) {
	match t.kind() {
		Kind::Array(inner) => collect_custom_types(inner, composites, domains),
		Kind::Range(inner) => collect_custom_types(inner, composites, domains),
		Kind::Multirange(inner) => collect_custom_types(inner, composites, domains),
		Kind::Domain(inner) => {
			if domains.insert(t.to_owned()) {
				collect_custom_types(inner, composites, domains);
			}
		},
		Kind::Composite(fields) => {
			if composites.insert(t.to_owned()) {
				for field in fields {
					collect_custom_types(field.type_(), composites, domains);
				}
			}
		},
		_ => {}
	}
}


#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParsedType {
	Numeric(u32, u32, Option<(BigInt, BigInt)>), // precision, scale, range (if different from 2^precision)
	Char(u32), // length
	VarChar(u32), // length
	Bit(u32), // length
	Geography { oid: Option<u32>, kind: String, srid: Option<u32> },
	Geometry { oid: Option<u32>, kind: String },
	WithArguments(Option<u32>, i32, String, Vec<String>), // oid, typemod, name, args
	Plain(Option<u32>, String), // oid, name
	Array(Option<u32>, Box<ParsedType>), // oid, inner
	// Range { multi: bool,  }, // ranges are irrelevant, they do not support numeric(X, Y), it's always arbitrary precision
	// TODO domains
	Null
}


impl fmt::Display for ParsedType {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ParsedType::Numeric(precision, scale, Some((range_min, range_max))) => write!(f, "numeric({}, {}, [{} ... {}])", precision, scale, range_min, range_max), // TODO: show?
			ParsedType::Numeric(precision, scale, _) => write!(f, "numeric({}, {})", precision, scale),
			ParsedType::Char(length) => write!(f, "char({})", length),
			ParsedType::VarChar(length) => write!(f, "varchar({})", length),
			ParsedType::Bit(length) => write!(f, "bit({})", length),
			ParsedType::Geography { oid: _, kind, srid: Some(srid) } => write!(f, "geography({}, {})", kind, srid),
			ParsedType::Geography { oid: _, kind, srid: None } => write!(f, "geography({})", kind),
			ParsedType::Geometry { oid: _, kind } => write!(f, "geometry({})", kind),
			ParsedType::WithArguments(_, _, name, args) => write!(f, "{}({})", name, args.join(", ")),
			ParsedType::Plain(_, x) => write!(f, "{}", x),
			ParsedType::Array(_, inner) => write!(f, "{}[]", inner),
			ParsedType::Null => write!(f, "<NULL>")
		}
	}
}

impl ParsedType {
	pub fn oid(&self) -> Option<u32> {
		match self {
			ParsedType::Numeric { .. } => Some(PgType::NUMERIC.oid()),
			ParsedType::Char { .. } => Some(PgType::CHAR.oid()),
			ParsedType::VarChar { .. } => Some(PgType::VARCHAR.oid()),
			ParsedType::Bit { .. } => Some(PgType::BIT.oid()),
			ParsedType::Geography { oid, .. } => *oid,
			ParsedType::Geometry { oid, .. } => *oid,
			ParsedType::WithArguments(oid, _, _, _) => *oid,
			ParsedType::Plain(oid, _) => *oid,
			ParsedType::Array(oid, _) => *oid,
			ParsedType::Null => None
		}
	}
	pub fn typemod(&self) -> i32 {
		match self {
			ParsedType::Numeric(precision, scale, _) => 4 + (scale | (precision << 16)) as i32,
			ParsedType::Char(length) => *length as i32,
			ParsedType::VarChar(length) => *length as i32,
			ParsedType::Bit(length) => *length as i32,
			ParsedType::Geography { oid:_, kind:_, srid:_ } => 1, // TODO?
			ParsedType::Geometry { oid:_, kind:_ } => 1, // TODO?
			ParsedType::WithArguments(_, _, _, args) if args.len() == 1 && args[0].parse::<i32>().is_ok() => {
				args[0].parse::<i32>().unwrap()
			},
			ParsedType::WithArguments(_, _, _, _) => 1,
			ParsedType::Plain(_, _) => -1,
			ParsedType::Array(_, x) => x.typemod(),
			ParsedType::Null => -1,
		}
	}
	pub fn name<'a>(&'a self) -> &'a str {
		match self {
			ParsedType::Numeric(_, _, _) => "numeric",
			ParsedType::Char(_) => "char",
			ParsedType::VarChar(_) => "varchar",
			ParsedType::Bit(_) => "bit",
			ParsedType::Geography { oid:_, kind:_, srid:_ } => "geography",
			ParsedType::Geometry { oid:_, kind:_ } => "geometry",
			ParsedType::WithArguments(_, _, name, _) => &name[..],
			ParsedType::Plain(_, name) => &name[..],
			ParsedType::Array(_, parsed_type) => parsed_type.name(),
			ParsedType::Null => "NULL",
		}
	}
	pub fn number_range_and_scale(&self) -> Option<(u32, BigInt, BigInt)> {
		match self {
			ParsedType::Numeric(_, scale, Some((min, max))) => Some((*scale, min.clone(), max.clone())),
			ParsedType::Numeric(precision, scale, None) => {
				// range from -99999 to +99999 where number of nines is precision
				let max: BigInt = BigInt::from_i64(10).unwrap().pow(*precision) - 1;
				Some((*scale, -max.clone(), max))
			},
			ParsedType::Plain(Some(oid), _) => {
				let oid = *oid;
				let int_props =
					if oid == PgType::INT4.oid() {
						Some((true, 32))
					} else if oid == PgType::INT2.oid() {
						Some((true, 16))
					} else if oid == PgType::INT8.oid() {
						Some((true, 64))
					} else if oid == PgType::CHAR.oid() {
						Some((false, 8))
					} else if oid == PgType::BOOL.oid() {
						Some((false, 1))
					} else {
						None
					};
				if let Some((signed, bits)) = int_props {
					if signed {
						Some((0, -BigInt::from_i64(2).unwrap().pow(bits-1), BigInt::from_i64(2).unwrap().pow(bits-1) - 1))
					} else {
						Some((0, BigInt::zero(), BigInt::from_i64(2).unwrap().pow(bits) - 1))
					}
				} else {
					None
				}
			},
			ParsedType::Array(_, element) => element.number_range_and_scale(),
			_ => None,
		}
	}
	pub fn is_number(&self) -> bool {
		self.oid().map(|x| x == PgType::NUMERIC.oid() || x == PgType::INT2.oid() || x == PgType::INT4.oid() || x == PgType::INT8.oid() || x == PgType::FLOAT4.oid() || x == PgType::FLOAT8.oid())
			.unwrap_or_else(|| match self.name() { "???TODO" => true, _ => false } )
	}
	pub fn is_texty(&self) -> bool {
		match self {
			ParsedType::Char(_) => true,
			ParsedType::VarChar(_) => true,
			ParsedType::Plain(Some(oid), _) => *oid == PgType::TEXT.oid() || *oid == PgType::CHAR.oid() || *oid == PgType::VARCHAR.oid(),
			_ => false
		}
	}
	pub fn pgtype(&self) -> PgType {
		// TODO: refactor to support custom types better
		self.oid().and_then(PgType::from_oid).unwrap_or(PgType::UNKNOWN)
	}

	pub fn number_dec_range(&self) -> Option<(BigDecimal, BigDecimal)> {
		let (scale, min, max) = self.number_range_and_scale()?;
		Some((BigDecimal::new(min, scale as i64), BigDecimal::new(max, scale as i64)))
	}
	// pub fn from_oid(oid: u32, typemod: i32) -> ParsedType {
	// 	let pg_type = PgType::from_oid(oid);
	// }
	pub fn from_int_range(scale: u32, minv: BigInt, maxv: BigInt) -> ParsedType {
		let precision = max(minv.magnitude().to_radix_le(10).len(), maxv.magnitude().to_radix_le(10).len()) as u32;
		ParsedType::Numeric(max(precision, 1), scale, Some((minv, maxv)))
	}
	pub fn from_dec_range(minv: BigDecimal, maxv: BigDecimal) -> ParsedType {
		let scale = max(0, max(minv.fractional_digit_count(), maxv.fractional_digit_count()));
		ParsedType::from_int_range(scale as u32, minv.with_scale(scale).into_bigint_and_scale().0, maxv.with_scale(scale).into_bigint_and_exponent().0)
	}

	pub fn round(&self, newscale: i64, mode: RoundingMode) -> ParsedType {
		let newscale_0 = max(newscale, 0) as u32;
		match self {
			ParsedType::Numeric(_, scale, _) if (*scale as i64) <= newscale => self.clone(),
			ParsedType::Numeric(precision, scale, None) => ParsedType::Numeric(precision - scale + newscale_0, newscale_0, None),
			ParsedType::Numeric(_, _, Some(_)) => {
				let (minv, maxv) = self.number_dec_range().unwrap();
				let (minv, maxv) = (minv.with_scale_round(newscale, mode), maxv.with_scale_round(newscale, mode));
				ParsedType::from_dec_range(minv, maxv)
			},
			_ => self.clone()
		}
	}

	pub fn with_oid(self, oid: Option<u32>) -> ParsedType {
		let Some(oid) = oid else { return self };
		match self {
			ParsedType::Geography { oid:_, kind, srid } => ParsedType::Geography { oid: Some(oid), kind, srid },
			ParsedType::Geometry { oid:_, kind } => ParsedType::Geometry { oid: Some(oid), kind },
			ParsedType::WithArguments(_oid, typemod, name, args) => ParsedType::WithArguments(Some(oid), typemod, name, args),
			ParsedType::Plain(_oid, name) => ParsedType::Plain(Some(oid), name),
			ParsedType::Array(_oid, inner) => ParsedType::Array(Some(oid), inner),
			other => other
		}
	}

	pub fn parse(name: &str, oid: Option<u32>, typemod: i32) -> ParsedType {
		let oid = oid.or_else(|| find_oid_by_name(name));
		let pg_type = oid.and_then(PgType::from_oid);
		if typemod >= 0 {
			if oid == Some(PgType::BIT.oid()) {
				return ParsedType::Bit(typemod as u32)
			}
			if oid == Some(PgType::BIT_ARRAY.oid()) {
				return ParsedType::Array(oid, Box::new(ParsedType::Bit(typemod as u32)))
			}

			if oid == Some(PgType::CHAR.oid()) {
				return ParsedType::Char(typemod as u32)
			}
			if oid == Some(PgType::CHAR_ARRAY.oid()) {
				return ParsedType::Array(oid, Box::new(ParsedType::Char(typemod as u32)))
			}

			if oid == Some(PgType::VARCHAR.oid()) {
				return ParsedType::VarChar(typemod as u32)
			}
			if oid == Some(PgType::VARCHAR_ARRAY.oid()) {
				return ParsedType::Array(oid, Box::new(ParsedType::VarChar(typemod as u32)))
			}
		}

		if name.ends_with("[]") {
			let inner_oid = oid
				.and_then(|oid| PgType::from_oid(oid))
				.and_then(|t| match t.kind() { Kind::Array(x) => Some(x.oid()), _ => None });
			let inner = ParsedType::parse(&name[..name.len()-2], inner_oid, typemod);
			return ParsedType::Array(oid, Box::new(inner))
		}

		if let Some(element_type) = pg_type.as_ref().and_then(|t| match t.kind() { Kind::Array(x) => Some(x), _ => None }) {
			return ParsedType::Array(oid, Box::new(ParsedType::parse(name, Some(element_type.oid()), typemod)))
		}

		match name.find('(') {
			Some(i) => {
				let (name, args) = name.split_at(i);
				let args = &args[1..args.len()-1];
				let args: Vec<&str> = args.split(',').map(|s| s.trim_ascii()).collect();
				ParsedType::from_name_args(name.to_owned(), &args, Some(typemod)).with_oid(oid)
			},
			None =>
				ParsedType::Plain(oid, if name == "?" && pg_type.is_some() { pg_type.unwrap().name().to_owned() } else { name.to_owned() })
		}
	}

	pub fn from_name_args(name: String, args: &[&str], typemod: Option<i32>) -> ParsedType {
		match (&name[..], args.len()) {
			("numeric", 2) => ParsedType::Numeric(args[0].parse().unwrap(), args[1].parse().unwrap(), None),
			("character" | "char", 1) => ParsedType::Char(args[0].parse().unwrap()),
			("character varying" | "varchar", 1) => ParsedType::VarChar(args[0].parse().unwrap()),
			("bit", 1) => ParsedType::Bit(args[0].parse().unwrap()),
			("geography", 2..) => ParsedType::Geography { oid: None, kind: args[0].to_owned(), srid: Some(args[1].parse().unwrap()) },
			("geography", 1..) => ParsedType::Geography { oid: None, kind: args[0].to_lowercase(), srid: None },
			("geometry", x) if x >= 1 => ParsedType::Geometry { oid: None, kind: args[0].to_lowercase() },
			(_, 1..) => ParsedType::WithArguments(None, typemod.unwrap_or(-1), name, args.iter().map(|s| s[..].to_owned()).collect()),

			(n, 0) => ParsedType::Plain(find_oid_by_name(n), name),
		}
	}
}

fn find_oid_by_name(name: &str) -> Option<u32> {
	// oid 1..16383 are reserved for built-in types
	// "OIDs 1-9999 are reserved for manual assignment (see .dat files in src/include/catalog/).  Of these, 8000-9999 are reserved for development purposes (such as in-progress patches and forks);they should not appear in released versions."
	// select typnamespace, pg_catalog.format_type(oid, NULL) as pretty_name, typname, oid, typcategory, typtype from pg_type where oid <= 8000 and typcategory != 'A' and typtype != 'c' order by oid;
	// select '        "' || pg_catalog.format_type(oid, NULL) || '" | "' || typname || '" => ' || oid || ', // ' || typcategory::text || typtype::text from pg_type where oid <= 8000 and typcategory != 'A' and typtype != 'c' order by oid;
	let oid = match name {
		"boolean" | "bool" => 16, // Bb
		"bytea" => 17, // Ub
		"\"char\"" | "char" => 18, // Zb
		"name" => 19, // Sb
		"bigint" | "int8" | "bigserial" | "serial8" => 20, // Nb
		"smallint" | "int2" | "smallserial" | "serial2" => 21, // Nb
		"integer" | "int4" | "serial" | "serial4" => 23, // Nb
		"regproc" => 24, // Nb
		"text" => 25, // Sb
		"oid" => 26, // Nb
		"tid" => 27, // Ub
		"xid" => 28, // Ub
		"cid" => 29, // Ub
		"pg_ddl_command" => 32, // Pp
		"json" => 114, // Ub
		"xml" => 142, // Ub
		"pg_node_tree" => 194, // Zb
		"table_am_handler" => 269, // Pp
		"index_am_handler" => 325, // Pp
		"point" => 600, // Gb
		"lseg" => 601, // Gb
		"path" => 602, // Gb
		"box" => 603, // Gb
		"polygon" => 604, // Gb
		"line" => 628, // Gb
		"cidr" => 650, // Ib
		"real" | "float4" => 700, // Nb
		"double precision" | "float8" => 701, // Nb
		"unknown" => 705, // Xp
		"circle" => 718, // Gb
		"macaddr8" => 774, // Ub
		"money" => 790, // Nb
		"macaddr" => 829, // Ub
		"inet" => 869, // Ib
		"aclitem" => 1033, // Ub
		"character" | "bpchar" => 1042, // Sb
		"character varying" | "varchar" => 1043, // Sb
		"date" => 1082, // Db
		"time without time zone" | "time" => 1083, // Db
		"timestamp without time zone" | "timestamp" => 1114, // Db
		"timestamp with time zone" | "timestamptz" => 1184, // Db
		"interval" => 1186, // Tb
		"time with time zone" | "timetz" => 1266, // Db
		"bit" => 1560, // Vb
		"bit varying" | "varbit" => 1562, // Vb
		"numeric" => 1700, // Nb
		"refcursor" => 1790, // Ub
		"regprocedure" => 2202, // Nb
		"regoper" => 2203, // Nb
		"regoperator" => 2204, // Nb
		"regclass" => 2205, // Nb
		"regtype" => 2206, // Nb
		"record" => 2249, // Pp
		"cstring" => 2275, // Pp
		"\"any\"" | "any" => 2276, // Pp
		"anyarray" => 2277, // Pp
		"void" => 2278, // Pp
		"trigger" => 2279, // Pp
		"language_handler" => 2280, // Pp
		"internal" => 2281, // Pp
		"anyelement" => 2283, // Pp
		"record[]" | "_record" => 2287, // Pp
		"anynonarray" => 2776, // Pp
		"uuid" => 2950, // Ub
		"txid_snapshot" => 2970, // Ub
		"fdw_handler" => 3115, // Pp
		"pg_lsn" => 3220, // Ub
		"tsm_handler" => 3310, // Pp
		"pg_ndistinct" => 3361, // Zb
		"pg_dependencies" => 3402, // Zb
		"anyenum" => 3500, // Pp
		"tsvector" => 3614, // Ub
		"tsquery" => 3615, // Ub
		"gtsvector" => 3642, // Ub
		"regconfig" => 3734, // Nb
		"regdictionary" => 3769, // Nb
		"jsonb" => 3802, // Ub
		"anyrange" => 3831, // Pp
		"event_trigger" => 3838, // Pp
		"int4range" => 3904, // Rr
		"numrange" => 3906, // Rr
		"tsrange" => 3908, // Rr
		"tstzrange" => 3910, // Rr
		"daterange" => 3912, // Rr
		"int8range" => 3926, // Rr
		"jsonpath" => 4072, // Ub
		"regnamespace" => 4089, // Nb
		"regrole" => 4096, // Nb
		"regcollation" => 4191, // Nb
		"int4multirange" => 4451, // Rm
		"nummultirange" => 4532, // Rm
		"tsmultirange" => 4533, // Rm
		"tstzmultirange" => 4534, // Rm
		"datemultirange" => 4535, // Rm
		"int8multirange" => 4536, // Rm
		"anymultirange" => 4537, // Pp
		"anycompatiblemultirange" => 4538, // Pp
		"pg_brin_bloom_summary" => 4600, // Zb
		"pg_brin_minmax_multi_summary" => 4601, // Zb
		"pg_mcv_list" => 5017, // Zb
		"pg_snapshot" => 5038, // Ub
		"xid8" => 5069, // Ub
		"anycompatible" => 5077, // Pp
		"anycompatiblearray" => 5078, // Pp
		"anycompatiblenonarray" => 5079, // Pp
		"anycompatiblerange" => 5080, // Pp
		_ => 0
	};
	if oid >= 1 {
		Some(oid as u32)
	} else {
		None
	}
}


#[test]
fn test_parse_type() {
	assert_eq!(ParsedType::parse("numeric(10,5)", None, 0), ParsedType::Numeric(10, 5, None));
	assert_eq!(ParsedType::parse("numeric(10, 5)", None, 0), ParsedType::Numeric(10, 5, None));
	assert_eq!(ParsedType::parse("text", Some(PgType::TEXT.oid()), -1), ParsedType::Plain(Some(PgType::TEXT.oid()), "text".to_owned()));
	assert_eq!(ParsedType::parse("text[]", Some(PgType::TEXT_ARRAY.oid()), -1), ParsedType::Array(Some(PgType::TEXT_ARRAY.oid()), Box::new(ParsedType::Plain(Some(PgType::TEXT.oid()), "text".to_owned()))));
	assert_eq!(ParsedType::parse("text[]", None, -1), ParsedType::Array(None, Box::new(ParsedType::Plain(Some(PgType::TEXT.oid()), "text".to_owned()))));
	assert_eq!(ParsedType::parse("?", Some(PgType::TEXT_ARRAY.oid()), -1), ParsedType::Array(Some(PgType::TEXT_ARRAY.oid()), Box::new(ParsedType::Plain(Some(PgType::TEXT.oid()), "text".to_owned()))));
}

#[test]
fn test_type_range() {
	assert_eq!(ParsedType::Numeric(6, 3, None).number_range_and_scale(), Some((3, BigInt::from_i64(-999999).unwrap(), BigInt::from_i64(999999).unwrap())));
	assert_eq!(ParsedType::parse("integer", None, -1).number_range_and_scale(), Some((0, BigInt::from_i32(i32::MIN).unwrap(), BigInt::from_i32(i32::MAX).unwrap())));
	assert_eq!(ParsedType::parse("?", Some(PgType::INT2.oid()), -1).number_range_and_scale(), Some((0, BigInt::from_i16(i16::MIN).unwrap(), BigInt::from_i16(i16::MAX).unwrap())));
}
