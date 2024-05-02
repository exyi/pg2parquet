use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::IpAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use clap::error::Error;
use parquet::basic::{Repetition, self, ConvertedType, LogicalType};
use parquet::data_type::{DataType, BoolType, Int32Type, Int64Type, FloatType, DoubleType, ByteArray, ByteArrayType, FixedLenByteArrayType, FixedLenByteArray};
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::TimestampType;
use pg_bigdecimal::PgNumeric;
use postgres::error::SqlState;
use postgres::types::{Kind, Type as PgType, FromSql};
use postgres::{self, Client, RowIter, Row, Column, Statement, NoTls};
use postgres::fallible_iterator::FallibleIterator;
use parquet::schema::types::{Type as ParquetType, TypePtr, GroupTypeBuilder};

use crate::PostgresConnArgs;
use crate::appenders::{ColumnAppender, DynamicMergedAppender, RealMemorySize, ArrayColumnAppender, ColumnAppenderBase, GenericColumnAppender, BasicPgRowColumnAppender, RcWrapperAppender, StaticMergedAppender, new_autoconv_generic_appender, PreprocessExt, new_static_merged_appender, DynColumnAppender};
use crate::datatypes::interval::PgInterval;
use crate::datatypes::jsonb::PgRawJsonb;
use crate::datatypes::money::PgMoney;
use crate::datatypes::numeric::{new_decimal_bytes_appender, new_decimal_int_appender};
use crate::myfrom::{MyFrom, self};
use crate::parquet_writer::{WriterStats, ParquetRowWriter, WriterSettings};
use crate::pg_custom_types::{PgEnum, PgRawRange, PgAbstractRow, PgRawRecord, PgAny, PgAnyRef, UnclonableHack};

type ResolvedColumn<TRow> = (DynColumnAppender<TRow>, ParquetType);

#[derive(Clone, Debug)]
pub struct SchemaSettings {
	pub macaddr_handling: SchemaSettingsMacaddrHandling,
	pub json_handling: SchemaSettingsJsonHandling,
	pub enum_handling: SchemaSettingsEnumHandling,
	pub interval_handling: SchemaSettingsIntervalHandling,
	pub numeric_handling: SchemaSettingsNumericHandling,
	pub decimal_scale: i32,
	pub decimal_precision: u32,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SchemaSettingsMacaddrHandling {
	/// MAC address is converted to a string
	Text,
	/// MAC is stored as fixed byte array of length 6
	ByteArray,
	/// MAC is stored in Int64 (lowest 6 bytes)
	Int64
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SchemaSettingsJsonHandling {
	/// JSON is stored as a Parquet JSON type. This is essentially the same as text, but with a different ConvertedType, so it may not be supported in all tools.
	TextMarkedAsJson,
	/// JSON is stored as a UTF8 text
	Text
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum SchemaSettingsEnumHandling {
	/// Enum is stored as the postgres enum name, Parquet LogicalType is set to ENUM
	Text,
	/// Enum is stored as the postgres enum name, Parquet LogicalType is set to String
	PlainText,
	/// Enum is stored as an 32-bit integer (one-based index of the value in the enum definition)
	Int
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SchemaSettingsIntervalHandling {
	/// Enum is stored as the Parquet INTERVAL type. This has lower precision than postgres interval (milliseconds instead of microseconds).
	Interval,
	/// Enum is stored as struct { months: i32, days: i32, microseconds: i64 }, exactly as PostgreSQL stores it.
	Struct
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SchemaSettingsNumericHandling {
	/// Numeric is stored using the DECIMAL parquet type. Use --decimal-precision and --decimal-scale to set the desired precision and scale.
	Decimal,
	/// Numeric is converted to float64 (DOUBLE).
	#[clap(alias="float", alias="float64")]
	Double,
	/// Numeric is converted to float32 (FLOAT).
	Float32,
	/// Convert the numeric to a string and store it as UTF8 text. This option never looses precision. Note that text "NaN" may be present if NaN is present in the database.
	String
}

pub fn default_settings() -> SchemaSettings {
	SchemaSettings {
		macaddr_handling: SchemaSettingsMacaddrHandling::Text,
		json_handling: SchemaSettingsJsonHandling::Text, // DuckDB doesn't load JSON converted type, so better to use string I guess
		enum_handling: SchemaSettingsEnumHandling::Text,
		interval_handling: SchemaSettingsIntervalHandling::Interval,
		numeric_handling: SchemaSettingsNumericHandling::Decimal,
		decimal_scale: 18,
		decimal_precision: 38,
	}
}

fn read_password(user: &str) -> Result<String, String> {
	let password = rpassword::prompt_password(&format!("Password for user {}: ", user));
	password.map_err(|e| format!("Failed to read password from TTY: {}", e))
}

#[cfg(any(target_os = "macos", target_os="windows", all(target_os="linux", not(target_env="musl"), any(target_arch="x86_64", target_arch="aarch64"))))]
fn build_tls_connector() -> Result<postgres_native_tls::MakeTlsConnector, String> {
	let connector = native_tls::TlsConnector::new().map_err(|e| format!("Creating TLS connector failed: {}", e.to_string()))?;
	let pg_connector = postgres_native_tls::MakeTlsConnector::new(connector);
	Ok(pg_connector)
}

#[cfg(not(any(target_os = "macos", target_os="windows", all(target_os="linux", not(target_env="musl"), any(target_arch="x86_64", target_arch="aarch64")))))]
fn build_tls_connector() -> Result<NoTls, String> {
	Ok(NoTls)
}

fn pg_connect(args: &PostgresConnArgs) -> Result<Client, String> {
	let user_env = std::env::var("PGUSER").ok();

	let mut pg_config = postgres::Config::new();
	pg_config.dbname(&args.dbname)
		.application_name("pg2parquet")
		.host(&args.host)
		.port(args.port.unwrap_or(5432))
		.user(args.user.as_ref().or(user_env.as_ref()).unwrap_or(&args.dbname));

	if let Some(password) = args.password.as_ref() {
		pg_config.password(password);
	} else if let Ok(password) = std::env::var("PGPASSWORD") {
		pg_config.password(&password);
	} else {
		pg_config.password(&read_password(pg_config.get_user().unwrap())?.trim());
	}

	#[cfg(not(any(target_os = "macos", target_os="windows", all(target_os="linux", not(target_env="musl"), any(target_arch="x86_64", target_arch="aarch64")))))]
	match &args.sslmode {
		None | Some(crate::SslMode::Disable) => {},
		Some(x) => return Err(format!("SSL/TLS is disabled in this build of pg2parquet, so ssl mode {:?} cannot be used. Only 'disable' option is allowed.", x)),
	}
	match &args.sslmode {
		None => {},
		Some(crate::SslMode::Disable) => {
			pg_config.ssl_mode(postgres::config::SslMode::Disable);
		},
		Some(crate::SslMode::Prefer) => {
			pg_config.ssl_mode(postgres::config::SslMode::Prefer);
		},
		Some(crate::SslMode::Require) => {
			pg_config.ssl_mode(postgres::config::SslMode::Require);
		},
	}

	let connector = build_tls_connector()?;

	let client = pg_config.connect(connector).map_err(|e| format!("DB connection failed: {}", e.to_string()))?;

	Ok(client)
}

pub fn execute_copy(pg_args: &PostgresConnArgs, query: &str, output_file: &PathBuf, output_props: WriterPropertiesPtr, quiet: bool, schema_settings: &SchemaSettings) -> Result<WriterStats, String> {

	let mut client = pg_connect(pg_args)?;
	let statement = client.prepare(query).map_err(|db_err| { db_err.to_string() })?;

	let (row_appender, schema) = map_schema_root(statement.columns(), schema_settings)?;
	if !quiet {
		eprintln!("Schema: {}", format_schema(&schema, 0));
	}
	let schema = Arc::new(schema);

	let settings = WriterSettings { row_group_byte_limit: 500 * 1024 * 1024, row_group_row_limit: output_props.max_row_group_size() };

	let output_file_f = std::fs::File::create(output_file).unwrap();
	let pq_writer = SerializedFileWriter::new(output_file_f, schema.clone(), output_props)
		.map_err(|e| format!("Failed to create parquet writer: {}", e))?;
	let mut row_writer = ParquetRowWriter::new(pq_writer, schema.clone(), row_appender, quiet, settings)
		.map_err(|e| format!("Failed to create row writer: {}", e))?;

	let rows: RowIter = client.query_raw::<Statement, &i32, &[i32]>(&statement, &[]).unwrap();
	for row in rows.iterator() {
		let row = row.map_err(|err| err.to_string())?;
		let row = Arc::new(row);

		row_writer.write_row(row)?;
	}

	Ok(row_writer.close()?)
}

fn format_schema(schema: &ParquetType, indent: u32) -> String {
	let basic_info = schema.get_basic_info();
	let mut additional_info =
		basic_info.logical_type().map(|lt| format!("{:?}", lt))
			.or_else(||
				match basic_info.converted_type() {
					ConvertedType::NONE => None,
					c => Some(c.to_string())
				}
			).unwrap_or("".to_string());

	match schema {
		ParquetType::PrimitiveType { basic_info, physical_type, type_length, scale, precision } => {
			if *precision > 0 {
				additional_info += &format!(" precision: {}", precision);
			}
			if *scale > 0 {
				additional_info += &format!(" scale: {}", scale);
			}
			if !additional_info.trim().is_empty() {
				additional_info = format!(" ({})", additional_info.trim());
			}
			let byte_size = if *type_length >= 0 { format!(" [{}b]", *type_length) } else { "".to_string() };

			format!("{} {}: {}{}{}", basic_info.repetition(), basic_info.name(), physical_type, byte_size, additional_info)
		},
		ParquetType::GroupType { basic_info, fields } => {
			let fields_str = fields.iter()
				.map(|f| "\t".repeat(indent as usize) + " * " + &format_schema(f, indent + 1))
				.collect::<Vec<_>>();

			let rp = if basic_info.has_repetition() && basic_info.repetition() != Repetition::REQUIRED { basic_info.repetition().to_string() + " " } else { "".to_string() };

			if !additional_info.trim().is_empty() {
				additional_info = format!(" ({})", additional_info.trim());
			}

			rp + basic_info.name() + &additional_info + "\n" + &fields_str.join("\n")
		}
	}
}

fn count_columns(p: &ParquetType) -> usize {
	match p {
		ParquetType::PrimitiveType { .. } => 1,
		ParquetType::GroupType { fields, .. } => fields.iter().map(|f| count_columns(f)).sum()
	}
}


fn map_schema_root<'a>(row: &[Column], s: &SchemaSettings) -> Result<ResolvedColumn<Arc<Row>>, String> {
	let mut fields: Vec<ResolvedColumn<Arc<Row>>> = vec![];
	for (col_i, c) in row.iter().enumerate() {

		let t = c.type_();

		let schema = map_schema_column(t, &ColumnInfo::root(col_i, c.name().to_owned()), s)?;
		fields.push(schema)
	}


	let (column_appenders, parquet_types): (Vec<_>, Vec<_>) = fields.into_iter().unzip();

	let merged_appender: DynColumnAppender<Arc<Row>> = Box::new(DynamicMergedAppender::new(column_appenders, 0, 0));
	let struct_type = ParquetType::group_type_builder("root")
		.with_fields(&mut parquet_types.into_iter().map(Arc::new).collect())
		.build()
		.unwrap();

	Ok((merged_appender, struct_type))
}

fn map_schema_column<TRow: PgAbstractRow + Clone + 'static>(
	t: &PgType,
	c: &ColumnInfo,
	settings: &SchemaSettings,
) -> Result<ResolvedColumn<TRow>, String> {
	match t.kind() {
		Kind::Simple =>
			map_simple_type(t, c, settings),
		Kind::Enum(ref _enum_data) =>
			match settings.enum_handling {
				SchemaSettingsEnumHandling::Int => {
					let mut mapping = HashMap::new();
					for (i, v) in _enum_data.iter().enumerate() {
						mapping.insert(v.to_string(), i as i32 + 1);
					}
					Ok(resolve_primitive_conv::<PgEnum, Int32Type, _, _>(c.col_name(), c, None, None, None, move |e|
						*mapping.get(&e.name).unwrap_or_else(|| panic!("Could not map enum value {}. Was new enum case added while pg2parquet is running?", &e.name))
					))
				},
				SchemaSettingsEnumHandling::Text | SchemaSettingsEnumHandling::PlainText => {
					let logical_type = if settings.enum_handling == SchemaSettingsEnumHandling::Text {
						LogicalType::Enum
					} else {
						LogicalType::String
					};
					Ok(resolve_primitive::<PgEnum, ByteArrayType, _>(c.col_name(), c, Some(logical_type), None))
				},
			}
		Kind::Array(ref element_type) => {
			let list_column = c.nest("list", 0).as_array();
			let element_column = list_column.nest("element", 0);

			let (element_appender, element_schema) = map_schema_column(element_type, &element_column, settings)?;
			
			debug_assert_eq!(element_schema.name(), "element");
			let list_schema = ParquetType::group_type_builder("list")
				.with_repetition(Repetition::REPEATED)
				.with_fields(&mut vec![
					Arc::new(element_schema)
				])
				.build().unwrap();

			let schema = ParquetType::group_type_builder(c.col_name())
				.with_logical_type(Some(LogicalType::List))
				.with_repetition(Repetition::OPTIONAL)
				.with_fields(&mut vec![
					Arc::new(list_schema)
				])
				.build().unwrap();
			assert_eq!(element_appender.max_dl(), element_column.definition_level + 1);
			assert_eq!(element_appender.max_rl(), element_column.repetition_level);
			let array_appender = create_array_appender(element_appender, &c);
			Ok((Box::new(array_appender), schema))
		},
		Kind::Domain(ref element_type) => {
			map_schema_column(element_type, c, settings)
		},
		&Kind::Range(ref element_type) => {
			let col_lower = map_schema_column::<UnclonableHack<PgRawRange>>(element_type, &c.nest("lower", 0), settings)?;
			let col_upper = map_schema_column::<UnclonableHack<PgRawRange>>(element_type, &c.nest("upper", 1), settings)?;

			let schema = ParquetType::group_type_builder(c.col_name())
				.with_fields(&mut vec![
					Arc::new(col_lower.1),
					Arc::new(col_upper.1),
					Arc::new(ParquetType::primitive_type_builder("lower_inclusive", basic::Type::BOOLEAN).build().unwrap()),
					Arc::new(ParquetType::primitive_type_builder("upper_inclusive", basic::Type::BOOLEAN).build().unwrap()),
					Arc::new(ParquetType::primitive_type_builder("is_empty", basic::Type::BOOLEAN).build().unwrap()),
				])
				.with_repetition(Repetition::OPTIONAL)
				.build()
				.unwrap();

			let appender = new_static_merged_appender::<UnclonableHack<PgRawRange>>(c.definition_level + 1, c.repetition_level)
				.add_appender(col_lower.0)
				.add_appender(col_upper.0)
				.add_appender_map(
					new_autoconv_generic_appender::<bool, BoolType>(c.definition_level + 2, c.repetition_level),
					|r| Cow::Owned(r.0.lower_inclusive)
				)
				.add_appender_map(
					new_autoconv_generic_appender::<bool, BoolType>(c.definition_level + 2, c.repetition_level),
					|r| Cow::Owned(r.0.upper_inclusive)
				)
				.add_appender_map(
					new_autoconv_generic_appender::<bool, BoolType>(c.definition_level + 2, c.repetition_level),
					|r| Cow::Owned(r.0.is_empty)
				)
				.preprocess(|x: Cow<PgRawRange>| match x {
					Cow::Owned(x) => Cow::Owned(UnclonableHack(x)),
					Cow::Borrowed(_) => panic!()
				});

			let appender_dyn = wrap_pg_row_reader(c, appender);

			Ok((Box::new(appender_dyn), schema))
		},
		&Kind::Composite(ref fields) => {
			let (mut column_appenders, mut parquet_types) = (vec![], vec![]);
			for (i, f) in fields.into_iter().enumerate() {
				let (c, t) = map_schema_column(f.type_(), &c.nest(f.name(), i), settings)?;
				column_appenders.push(c);
				parquet_types.push(t);
			}

			let schema = ParquetType::group_type_builder(c.col_name())
				.with_fields(&mut parquet_types.into_iter().map(Arc::new).collect())
				.with_repetition(Repetition::OPTIONAL)
				.build()
				.unwrap();

			let appender = create_complex_appender::<PgRawRecord, TRow>(c, column_appenders);

			Ok((Box::new(appender), schema))
		}
		_ => Err(format!("Could not map column {}, unsupported type: {}", c.full_name(), t))
	}
}


fn map_simple_type<TRow: PgAbstractRow + Clone + 'static>(
	t: &PgType,
	c: &ColumnInfo,
	s: &SchemaSettings,
) -> Result<ResolvedColumn<TRow>, String> {
	let name = c.col_name();

	Ok(match t.name() {
		"bool" => resolve_primitive::<bool, BoolType, _>(name, c, None, None),
		"int2" => resolve_primitive::<i16, Int32Type, _>(name, c, Some(LogicalType::Integer { bit_width: 16, is_signed: true }), None),
		"int4" => resolve_primitive::<i32, Int32Type, _>(name, c, None, None),
		"oid" => resolve_primitive::<u32, Int32Type, _>(name, c, Some(LogicalType::Integer { bit_width: 32, is_signed: false }), None),
		"int8" => resolve_primitive::<i64, Int64Type, _>(name, c, None, None),
		"float4" => resolve_primitive::<f32, FloatType, _>(name, c, None, None),
		"float8" => resolve_primitive::<f64, DoubleType, _>(name, c, None, None),
		"numeric" => {
			resolve_numeric(s, name, c)?
		},
		"money" => resolve_primitive::<PgMoney, Int64Type, _>(name, c, Some(LogicalType::Decimal { scale: 2, precision: 18 }), None),
		"char" => resolve_primitive::<i8, Int32Type, _>(name, c, Some(LogicalType::Integer { bit_width: 8, is_signed: false }), None),
		"bytea" => resolve_primitive::<Vec<u8>, ByteArrayType, _>(name, c, None, None),
		"name" | "text" | "xml" | "bpchar" | "varchar" | "citext" =>
			resolve_primitive::<String, ByteArrayType, _>(name, c, None, Some(ConvertedType::UTF8)),
		"jsonb" | "json" =>
			resolve_primitive::<PgRawJsonb, ByteArrayType, _>(name, c, Some(match s.json_handling {
				SchemaSettingsJsonHandling::Text => LogicalType::String,
				SchemaSettingsJsonHandling::TextMarkedAsJson => LogicalType::Json
			}), None),
		"timestamptz" =>
			resolve_primitive::<chrono::DateTime<chrono::Utc>, Int64Type, _>(name, c, Some(LogicalType::Timestamp { is_adjusted_to_u_t_c: true, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),
		"timestamp" =>
			resolve_primitive::<chrono::NaiveDateTime, Int64Type, _>(name, c, Some(LogicalType::Timestamp { is_adjusted_to_u_t_c: false, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),
		"date" =>
			resolve_primitive::<chrono::NaiveDate, Int32Type, _>(name, c, Some(LogicalType::Date), None),
		"time" =>
			resolve_primitive::<chrono::NaiveTime, Int64Type, _>(name, c, Some(LogicalType::Time { is_adjusted_to_u_t_c: false, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),

		"uuid" =>
			resolve_primitive_conv::<uuid::Uuid, FixedLenByteArrayType, _, _>(name, c, Some(16), Some(LogicalType::Uuid), None, |v| MyFrom::my_from(v)),

		"macaddr" =>
			match s.macaddr_handling {
				SchemaSettingsMacaddrHandling::Text =>
					resolve_primitive::<eui48::MacAddress, ByteArrayType, _>(name, c, Some(LogicalType::String), None),
				SchemaSettingsMacaddrHandling::ByteArray =>
					resolve_primitive_conv::<eui48::MacAddress, FixedLenByteArrayType, _, _>(name, c, Some(6), None, None, |v| MyFrom::my_from(v)),
				SchemaSettingsMacaddrHandling::Int64 =>
					resolve_primitive::<eui48::MacAddress, Int64Type, _>(name, c, None, None),
			},
		"inet" =>
			resolve_primitive::<IpAddr, ByteArrayType, _>(name, c, Some(LogicalType::String), None),
		"bit" | "varbit" =>
			resolve_primitive::<bit_vec::BitVec, ByteArrayType, _>(name, c, Some(LogicalType::String), None),

		"interval" =>
			match s.interval_handling {
				SchemaSettingsIntervalHandling::Interval =>
					resolve_primitive_conv::<PgInterval, FixedLenByteArrayType, _, _>(name, c, Some(12), None, Some(ConvertedType::INTERVAL), |v| MyFrom::my_from(v)),
				SchemaSettingsIntervalHandling::Struct => {
					let t = GroupTypeBuilder::new(c.col_name())
						.with_repetition(Repetition::OPTIONAL)
						.with_fields(&mut vec![
							Arc::new(ParquetType::primitive_type_builder("months", basic::Type::INT32).build().unwrap()),
							Arc::new(ParquetType::primitive_type_builder("days", basic::Type::INT32).build().unwrap()),
							Arc::new(ParquetType::primitive_type_builder("microseconds", basic::Type::INT64).build().unwrap()),
						])
						.build().unwrap();
					let appender = new_static_merged_appender::<PgInterval>(c.definition_level + 1, c.repetition_level)
						.add_appender_map(new_autoconv_generic_appender::<i32, Int32Type>(c.definition_level + 2, c.repetition_level), |i| Cow::Owned(i.months))
						.add_appender_map(new_autoconv_generic_appender::<i32, Int32Type>(c.definition_level + 2, c.repetition_level), |i| Cow::Owned(i.days))
						.add_appender_map(new_autoconv_generic_appender::<i64, Int64Type>(c.definition_level + 2, c.repetition_level), |i| Cow::Owned(i.microseconds));
					(Box::new(wrap_pg_row_reader(c, appender)), t)
				},
			},

		// TODO: Regproc Tid Xid Cid PgNodeTree Point Lseg Path Box Polygon Line Cidr Unknown Circle Macaddr8 Aclitem Bpchar Timetz Refcursor Regprocedure Regoper Regoperator Regclass Regtype TxidSnapshot PgLsn PgNdistinct PgDependencies TsVector Tsquery GtsVector Regconfig Regdictionary Jsonpath Regnamespace Regrole Regcollation PgMcvList PgSnapshot Xid9


		n => 
			return Err(format!("Could not map column {}, unsupported primitive type: {}", c.full_name(), n)),
	})
}

fn resolve_numeric<TRow: PgAbstractRow + Clone + 'static>(s: &SchemaSettings, name: &str, c: &ColumnInfo) -> Result<ResolvedColumn<TRow>, String> {
	match s.numeric_handling {
		SchemaSettingsNumericHandling::Decimal => {
			let scale = s.decimal_scale;
			let precision = s.decimal_precision;
			let pq_type = if precision <= 9 {
				basic::Type::INT32
			} else if precision <= 18 {
				basic::Type::INT64
			} else {
				basic::Type::BYTE_ARRAY
			};
		let schema = ParquetType::primitive_type_builder(name, pq_type)
				.with_logical_type(Some(LogicalType::Decimal { scale, precision: precision as i32 }))
				.with_precision(precision as i32)
				.with_scale(scale)
				.build().unwrap();
		let cp: DynColumnAppender<TRow> = if pq_type == basic::Type::INT32 {
				let appender = new_decimal_int_appender::<i32, Int32Type>(c.definition_level + 1, c.repetition_level, precision, scale);
				Box::new(wrap_pg_row_reader(c, appender))
			} else if pq_type == basic::Type::INT64 {
				let appender = new_decimal_int_appender::<i64, Int64Type>(c.definition_level + 1, c.repetition_level, precision, scale);
				Box::new(wrap_pg_row_reader(c, appender))
			} else {
				let appender = new_decimal_bytes_appender(c.definition_level + 1, c.repetition_level, s.decimal_precision, s.decimal_scale);
				Box::new(wrap_pg_row_reader(c, appender))
			};
			Ok((cp, schema))
		},

		SchemaSettingsNumericHandling::Double =>
			Ok(resolve_primitive::<PgNumeric, DoubleType, _>(name, c, None, None)),
		SchemaSettingsNumericHandling::Float32 =>
			Ok(resolve_primitive::<PgNumeric, FloatType, _>(name, c, None, None)),
		SchemaSettingsNumericHandling::String =>
			Ok(resolve_primitive_conv::<PgNumeric, ByteArrayType, _, _>(name, c, None, Some(LogicalType::String), None, |v: PgNumeric| match v.n {
				Some(n) => ByteArray::my_from(n.to_string()),
				None => ByteArray::my_from("NaN".to_string())
			}))
	}
}

fn resolve_primitive<T: for<'a> FromSql<'a> + Clone + 'static, TDataType, TRow: PgAbstractRow + Clone + 'static>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>
) -> ResolvedColumn<TRow>
	where TDataType: DataType, TDataType::T : RealMemorySize + MyFrom<T> {
	resolve_primitive_conv::<T, TDataType, _, TRow>(name, c, None, logical_type, conv_type, |v| MyFrom::my_from(v))
}

fn resolve_primitive_conv<T: for<'a> FromSql<'a> + Clone + 'static, TDataType, FConversion: Fn(T) -> TDataType::T + 'static, TRow: PgAbstractRow + Clone + 'static>(
	name: &str,
	c: &ColumnInfo,
	length: Option<i32>,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> ResolvedColumn<TRow>
	where TDataType: DataType, TDataType::T : RealMemorySize {
	let mut c = c.clone();
	c.definition_level += 1; // TODO: can we support NOT NULL fields?
	let mut t =
		ParquetType::primitive_type_builder(name, TDataType::get_physical_type())
		.with_converted_type(conv_type.unwrap_or(ConvertedType::NONE));

	match length {
		Some(l) => {
			t = t.with_length(l);
		},
		_ => {}
	};

	match &logical_type {
		Some(LogicalType::Decimal { scale, precision }) => {
			t = t.with_precision(*precision).with_scale(*scale);
		},
		_ => {}
	};
	
	let t = t.with_logical_type(logical_type).build().unwrap();

	let cp =
		create_primitive_appender::<T, TDataType, _, _>(&c, convert);

	(Box::new(cp), t)
}
fn create_primitive_appender_simple<T: for <'a> FromSql<'a> + Clone + 'static, TDataType, TRow: PgAbstractRow + Clone + 'static>(
	c: &ColumnInfo,
) -> DynColumnAppender<TRow>
	where TDataType: DataType, TDataType::T: RealMemorySize + MyFrom<T> {
	let mut c = c.clone();
	c.definition_level += 1;
	Box::new(create_primitive_appender::<T, TDataType, _, TRow>(&c, |x| TDataType::T::my_from(x)))
}

fn create_primitive_appender<T: for <'a> FromSql<'a> + Clone + 'static, TDataType, FConversion: Fn(T) -> TDataType::T + 'static, TRow: PgAbstractRow + Clone>(
	c: &ColumnInfo,
	convert: FConversion
) -> impl ColumnAppender<TRow>
	where TDataType: DataType, TDataType::T: RealMemorySize {
	let basic_appender: GenericColumnAppender<T, TDataType, _> = GenericColumnAppender::new(c.definition_level, c.repetition_level, convert);
	wrap_pg_row_reader(c, basic_appender)
}

fn create_complex_appender<T: for <'a> FromSql<'a> + Clone + 'static, TRow: PgAbstractRow + Clone>(c: &ColumnInfo, columns: Vec<DynColumnAppender<Arc<T>>>) -> impl ColumnAppender<TRow> {
	let main_cp = DynamicMergedAppender::new(columns, c.definition_level + 1, c.repetition_level);
	wrap_pg_row_reader(c, RcWrapperAppender::new(main_cp))
}

fn create_array_appender<TRow: PgAbstractRow + Clone>(inner: DynColumnAppender<PgAny>, c: &ColumnInfo) -> impl ColumnAppender<TRow> {
	let outer_dl = c.definition_level + 1;
	debug_assert_eq!(outer_dl + 2, inner.max_dl());
	let array_appender = ArrayColumnAppender::new(inner, true, true, outer_dl, c.repetition_level);
	wrap_pg_row_reader::<TRow, Vec<Option<PgAny>>>(c, array_appender)
}

fn wrap_pg_row_reader<TRow: PgAbstractRow + Clone, T: Clone + for <'a> FromSql<'a>>(c: &ColumnInfo, a: impl ColumnAppender<T>) -> impl ColumnAppender<TRow> {
	BasicPgRowColumnAppender::new(c.col_i, a)
}

#[derive(Debug, Clone)]
struct ColumnInfo {
	pub names: Arc<Vec<String>>,
	pub col_i: usize,
	pub is_array: bool,
	pub definition_level: i16,
	pub repetition_level: i16,
}
impl ColumnInfo {
	pub fn root(col_i: usize, name: String) -> ColumnInfo {
		ColumnInfo {
			names: Arc::new(vec![name]),
			col_i,
			is_array: false,
			definition_level: 0,
			repetition_level: 0,
		}
	}

	fn nest<TString: Into<String>>(&self, name: TString, col_i: usize) -> ColumnInfo {
		ColumnInfo {
			names: Arc::new({
				let mut v = (*self.names).clone();
				v.push(name.into());
				v
			}),
			col_i,
			is_array: false,
			definition_level: self.definition_level + 1,
			repetition_level: self.repetition_level,
		}
	}

	fn as_array(&self) -> ColumnInfo {
		assert!(self.is_array == false, "Parquet does not support nested arrays");
		ColumnInfo {
			names: self.names.clone(),
			col_i: self.col_i,
			is_array: true,
			definition_level: self.definition_level,
			repetition_level: self.repetition_level + 1,
		}
	}

	fn col_name(&self) -> &str {
		&self.names[self.names.len() - 1]
	}

	fn full_name(&self) -> String {
		self.names.join("/")
	}
}


