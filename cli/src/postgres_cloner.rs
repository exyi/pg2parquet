use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::IpAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::error::Error;
use parquet::basic::{self, ConvertedType, IntType, LogicalType, Repetition};
use parquet::data_type::{DataType, BoolType, Int32Type, Int64Type, FloatType, DoubleType, ByteArray, ByteArrayType, FixedLenByteArrayType, FixedLenByteArray};
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::TimestampType;
use pg_bigdecimal::PgNumeric;
use postgres::error::SqlState;
use postgres::types::{Kind, Type as PgType, FromSql};
use postgres::{self, Client, RowIter, Row, Column, Statement, NoTls};
use postgres::fallible_iterator::FallibleIterator;
use parquet::schema::types::{GroupTypeBuilder, PrimitiveTypeBuilder, Type as ParquetType, TypePtr};
use half::f16;

use crate::datatypes::array::{PgMultidimArray, PgMultidimArrayLowerBounds};
use crate::datatypes::pgvector::{self, PgSparseVector};
use crate::PostgresConnArgs;
use crate::appenders::{new_autoconv_generic_appender, new_static_merged_appender, ArrayColumnAppender, BasicPgRowColumnAppender, ColumnAppender, ColumnAppenderBase, DynColumnAppender, DynamicMergedAppender, GenericColumnAppender, PreprocessAppender, PreprocessExt, RcWrapperAppender, RealMemorySize, StaticMergedAppender};
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
	pub array_handling: SchemaSettingsArrayHandling,
	pub float16_handling: SchemaSettingsFloat16Handling,
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

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum SchemaSettingsArrayHandling {
	/// Postgres arrays are simply stored as Parquet LIST
	Plain,
	/// Postgres arrays are stored as struct of { data: List[T], dims: List[int] }
	#[clap(alias="dims")]
	Dimensions,
	/// Postgres arrays are stored as struct of { data: List[T], dims: List[int], lower_bound: List[int] }
	#[clap(name="dimensions+lowerbound", alias="dimensions+lower_bound", alias="dimensions+lower-bound", alias="dims+lb")]
	DimensionsAndLowerBound,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum SchemaSettingsFloat16Handling {
	/// Serialize float16 values as float32 for better compatibility. Usually, compression will handle this and it won't take significantly more space.
	Float32,
	/// Use Float16 parquet logical type. Currently, compatibility with other tools is limited and the implementation in pg2parquet has performance issues, but might offer a size reduction.
	Float16
}

pub fn default_settings() -> SchemaSettings {
	SchemaSettings {
		macaddr_handling: SchemaSettingsMacaddrHandling::Text,
		json_handling: SchemaSettingsJsonHandling::Text, // DuckDB doesn't load JSON converted type, so better to use string I guess
		enum_handling: SchemaSettingsEnumHandling::Text,
		interval_handling: SchemaSettingsIntervalHandling::Interval,
		numeric_handling: SchemaSettingsNumericHandling::Double,
		decimal_scale: 18,
		decimal_precision: 38,
		array_handling: SchemaSettingsArrayHandling::Plain,
		float16_handling: SchemaSettingsFloat16Handling::Float32,
	}
}

fn read_password(user: &str) -> Result<String, String> {
	let password = rpassword::prompt_password(&format!("Password for user {}: ", user));
	password.map_err(|e| format!("Failed to read password from TTY: {}", e))
}

#[cfg(any(target_os = "macos", target_os="windows", all(target_os="linux", not(target_env="musl"), any(target_arch="x86_64", target_arch="aarch64"))))]
fn build_tls_connector(certificates: &Option<Vec<PathBuf>>, accept_invalid_certs: bool) -> Result<postgres_native_tls::MakeTlsConnector, String> {
	fn load_cert(f: &PathBuf) -> Result<native_tls::Certificate, String> {
		let bytes = std::fs::read(f).map_err(|e| format!("Failed to read certificate file {:?}: {}", f, e))?;
		if let Ok(pem) = native_tls::Certificate::from_pem(&bytes) {
			return Ok(pem);
		}
		if let Ok(der) = native_tls::Certificate::from_der(&bytes) {
			return Ok(der);
		}
		
		Err(format!("Failed to load certificate from file {:?}", f))
	}
	let mut builder = native_tls::TlsConnector::builder();
	builder.danger_accept_invalid_certs(accept_invalid_certs);
	builder.danger_accept_invalid_hostnames(accept_invalid_certs);
	match certificates {
		None => {},
		Some(certificates) => {
			builder.disable_built_in_roots(true);
			for cert in certificates {
				builder.add_root_certificate(load_cert(cert)?);
			}
		}
	}
	let connector = builder.build().map_err(|e| format!("Creating TLS connector failed: {}", e.to_string()))?;
	let pg_connector = postgres_native_tls::MakeTlsConnector::new(connector);
	Ok(pg_connector)
}

#[cfg(not(any(target_os = "macos", target_os="windows", all(target_os="linux", not(target_env="musl"), any(target_arch="x86_64", target_arch="aarch64")))))]
fn build_tls_connector(certificates: &Option<Vec<PathBuf>>, allow_invalid_certs: bool) -> Result<NoTls, String> {
	if certificates.is_some() {
		return Err("SSL/TLS is not supported in this build of pg2parquet".to_string());
	}
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
	let mut allow_invalid_certs = false;
	match &args.sslmode {
		None => {
			if args.ssl_root_cert.is_some() {
				pg_config.ssl_mode(postgres::config::SslMode::Require);
			} else {
				pg_config.ssl_mode(postgres::config::SslMode::Prefer);
				allow_invalid_certs = true;
			}
		},
		Some(crate::SslMode::Disable) => {
			pg_config.ssl_mode(postgres::config::SslMode::Disable);
		},
		Some(crate::SslMode::Prefer) => {
			pg_config.ssl_mode(postgres::config::SslMode::Prefer);
			allow_invalid_certs = true;
		},
		Some(crate::SslMode::Require) => {
			pg_config.ssl_mode(postgres::config::SslMode::Require);
		},
	}

	let connector = build_tls_connector(&args.ssl_root_cert, allow_invalid_certs)?;

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

	let rows: RowIter = client.query_raw::<Statement, &i32, &[i32]>(&statement, &[])
		.map_err(|err| format!("Failed to execute the SQL query: {}", err))?;
	for row in rows.iterator() {
		let row = row.map_err(|err| err.to_string())?;
		let row = Arc::new(row);

		row_writer.write_row(row)?;
	}

	Ok(row_writer.close()?)
}

fn format_schema(schema: &ParquetType, indent: u32) -> String {
	fn format_time_unit(u: &parquet::format::TimeUnit) -> &str {
		match u {
			basic::TimeUnit::MILLIS(_) => "ms",
			basic::TimeUnit::MICROS(_) => "µs",
			basic::TimeUnit::NANOS(_) => "ns",
		}
	}
	fn format_logical_type(t: &LogicalType) -> String {
		match t {
			LogicalType::Decimal { scale, precision } => format!("Decimal({}, {})", precision, scale),
			LogicalType::Time { is_adjusted_to_u_t_c, unit } =>
				format!("Time({}, utc={:?})", format_time_unit(unit), is_adjusted_to_u_t_c),
			LogicalType::Timestamp { is_adjusted_to_u_t_c, unit } =>
				format!("Timestamp({}, utc={:?})", format_time_unit(unit), is_adjusted_to_u_t_c),
			LogicalType::Integer { bit_width, is_signed } => {
				let sign = if *is_signed { "" } else { "U" };
				format!("{}Int{}", sign, bit_width)
			}
			_ => format!("{:?}", t)
		}
	}
	let basic_info = schema.get_basic_info();
	let logical_type =
		basic_info.logical_type().map(|lt| format_logical_type(&lt))
			.or_else(||
				match basic_info.converted_type() {
					ConvertedType::NONE => None,
					c => Some(c.to_string())
				}
			);

	match schema {
		ParquetType::PrimitiveType { basic_info, physical_type, type_length, scale, precision } => {
			let (primary_type, mut additional_info) = match logical_type {
				Some(lt) => (lt, physical_type.to_string()),
				None => (physical_type.to_string(), "".to_owned())
			};

			if *precision > 0 && !matches!(basic_info.logical_type(), Some(LogicalType::Decimal { .. })) {
				additional_info += &format!(" precision: {}", precision);
			}
			if *scale > 0 && !matches!(basic_info.logical_type(), Some(LogicalType::Decimal { .. })) {
				additional_info += &format!(" scale: {}", scale);
			}
			if !additional_info.trim().is_empty() {
				additional_info = format!(" ({})", additional_info.trim());
			}
			let byte_size = if *type_length >= 0 { format!(" [{}b]", *type_length) } else { "".to_string() };

			format!("{} {}: {}{}{}",
				basic_info.repetition().to_string().to_lowercase(),
				basic_info.name(),
				primary_type,
				byte_size,
				additional_info)
		},
		ParquetType::GroupType { basic_info, fields } => {
			let mut additional_info = logical_type.unwrap_or_else(|| "".to_owned());
			let fields_str = fields.iter()
				.map(|f| "\t".repeat(indent as usize) + " * " + &format_schema(f, indent + 1))
				.collect::<Vec<_>>();

			let rp = if basic_info.has_repetition() && basic_info.repetition() != Repetition::REQUIRED { basic_info.repetition().to_string().to_lowercase() + " " } else { "".to_string() };

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
		.with_fields(parquet_types.into_iter().map(Arc::new).collect())
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
				SchemaSettingsEnumHandling::Text =>
					Ok(resolve_primitive::<PgEnum, ByteArrayType, _>(c.col_name(), c, Some(LogicalType::Enum), None)),
				SchemaSettingsEnumHandling::PlainText =>
					Ok(resolve_primitive::<PgEnum, ByteArrayType, _>(c.col_name(), c, Some(LogicalType::String), None)),
			}
		Kind::Array(ref element_type) => {
			let list_column = c.nest("list", 0).as_array();
			let element_column = list_column.nest("element", 0);

			let (element_appender, element_schema) = map_schema_column(element_type, &element_column, settings)?;
			
			debug_assert_eq!(element_schema.name(), "element");

			let plain_schema = settings.array_handling == SchemaSettingsArrayHandling::Plain;

			let schema = if plain_schema {
				make_list_schema(c.col_name(), Repetition::OPTIONAL, element_schema)
			} else {
				make_list_schema("data", Repetition::REQUIRED, element_schema)
			};

			assert_eq!(element_appender.max_dl(), element_column.definition_level + 1);
			assert_eq!(element_appender.max_rl(), element_column.repetition_level);
			let array_appender = create_array_appender(element_appender, &c, plain_schema);
			let dim_appender = create_array_dim_appender::<PgAny, TRow>(&c);
			let lb_appender = create_array_lower_bound_appender::<PgAny, TRow>(&c);
			let dim_schema = make_list_schema("dims", Repetition::REQUIRED, ParquetType::primitive_type_builder("element", basic::Type::INT32).with_repetition(Repetition::REQUIRED).with_logical_type(Some(LogicalType::Integer { bit_width: 32, is_signed: false })).build().unwrap());
			let lb_schema = make_list_schema("lower_bound", Repetition::REQUIRED, ParquetType::primitive_type_builder("element", basic::Type::INT32).with_repetition(Repetition::REQUIRED).with_logical_type(Some(LogicalType::Integer { bit_width: 32, is_signed: true })).build().unwrap());
			match settings.array_handling {
				SchemaSettingsArrayHandling::Plain => Ok((Box::new(array_appender), schema)),
				SchemaSettingsArrayHandling::Dimensions => Ok((
					Box::new(
						new_static_merged_appender(c.definition_level + 1, c.repetition_level).add_appender(array_appender).add_appender(dim_appender)
					),
					ParquetType::group_type_builder(c.col_name())
						.with_repetition(Repetition::OPTIONAL)
						.with_fields(vec![ Arc::new(schema), Arc::new(dim_schema) ])
						.build().unwrap()
				)),
				SchemaSettingsArrayHandling::DimensionsAndLowerBound => Ok((
					Box::new(
						new_static_merged_appender(c.definition_level + 1, c.repetition_level).add_appender(array_appender).add_appender(dim_appender).add_appender(lb_appender)
					),
					ParquetType::group_type_builder(c.col_name())
						.with_repetition(Repetition::OPTIONAL)
						.with_fields(vec![ Arc::new(schema), Arc::new(dim_schema), Arc::new(lb_schema) ])
						.build().unwrap()
				))
			}
		},
		Kind::Domain(ref element_type) => {
			map_schema_column(element_type, c, settings)
		},
		&Kind::Range(ref element_type) => {
			let col_lower = map_schema_column::<UnclonableHack<PgRawRange>>(element_type, &c.nest("lower", 0), settings)?;
			let col_upper = map_schema_column::<UnclonableHack<PgRawRange>>(element_type, &c.nest("upper", 1), settings)?;

			let schema = ParquetType::group_type_builder(c.col_name())
				.with_fields(vec![
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
				.with_fields(parquet_types.into_iter().map(Arc::new).collect())
				.with_repetition(Repetition::OPTIONAL)
				.build()
				.unwrap();

			let appender = create_complex_appender::<PgRawRecord, TRow>(c, column_appenders);

			Ok((Box::new(appender), schema))
		}
		_ => Err(format!("Could not map column {}, unsupported type: {}", c.full_name(), t))
	}
}

fn make_list_schema(name: &str, repetition: Repetition, element_schema: ParquetType) -> ParquetType {
	ParquetType::group_type_builder(name)
		.with_logical_type(Some(LogicalType::List))
		.with_repetition(repetition)
		.with_fields(vec![
			Arc::new(ParquetType::group_type_builder("list")
				.with_repetition(Repetition::REPEATED)
				.with_fields(vec![
					Arc::new(element_schema)
				])
				.build().unwrap())
		])
		.build().unwrap()
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
			resolve_primitive::<String, ByteArrayType, _>(name, c, Some(LogicalType::String), Some(ConvertedType::UTF8)),
			// (Box::new(crate::appenders::byte_array::create_pg_raw_appender(c.definition_level + 1, c.repetition_level, c.col_i)),
			// 	ParquetType::primitive_type_builder(name, basic::Type::BYTE_ARRAY).with_logical_type(Some(LogicalType::String)).with_converted_type(ConvertedType::UTF8).build().unwrap()),
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
						.with_fields(vec![
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


		// pgvector extension: vector = 32-bit float array, halfvec = 16-bit float array, sparsevec = sparse f32 vector
		"vector" => resolve_vector_conv::<pgvector::PgF32Vector, f32, FloatType, _, TRow>(name, c, None, None, None, |v| v),
		"halfvec" => match s.float16_handling {
			SchemaSettingsFloat16Handling::Float16 =>
				resolve_vector_conv::<pgvector::PgF16Vector, f16, FixedLenByteArrayType, _, TRow>(name, c, Some(2), Some(LogicalType::Float16), None, |v|
					FixedLenByteArray::from(ByteArray::from(v.to_le_bytes().to_vec()))),
			SchemaSettingsFloat16Handling::Float32 =>
				resolve_vector_conv::<pgvector::PgF16Vector, f16, FloatType, _, TRow>(name, c, None, None, None, |v| v.into())
		},
		"sparsevec" => {
			let inner_appender = new_static_merged_appender::<(i32, f32)>(c.definition_level + 2, c.repetition_level + 1)
				// index+1, because pgvector uses 0-based in binary, but 1-based in text and operators 
				.add_appender(GenericColumnAppender::<_, Int32Type, _>::new(c.definition_level + 2, c.repetition_level + 1, |v: (i32, f32)| v.0 + 1))
				.add_appender(GenericColumnAppender::<_, FloatType, _>::new(c.definition_level + 2, c.repetition_level + 1, |v: (i32, f32)| v.1));

			let schema = ParquetType::group_type_builder(name)
				.with_repetition(Repetition::OPTIONAL)
				.with_fields(vec![
					Arc::new(ParquetType::group_type_builder("key_value").with_repetition(Repetition::REPEATED).with_fields(vec![
						Arc::new(ParquetType::primitive_type_builder("key", basic::Type::INT32)
							.with_repetition(Repetition::REQUIRED)
							.with_logical_type(Some(LogicalType::Integer { bit_width: 32, is_signed: false }))
							.build().unwrap()),
						Arc::new(ParquetType::primitive_type_builder("value", basic::Type::FLOAT)
							.with_repetition(Repetition::REQUIRED)
							.build().unwrap())
					]).build().unwrap())
				])
				.with_converted_type(ConvertedType::MAP)
				.with_logical_type(Some(LogicalType::Map))
				.build().unwrap();

			let array_appender = ArrayColumnAppender::new(inner_appender, true, false, c.definition_level + 1, c.repetition_level);

			(Box::new(wrap_pg_row_reader::<TRow, PgSparseVector>(&c, array_appender)), schema)
		}


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
	let t =
		build_primitive_pq_type(name, TDataType::get_physical_type(), length, logical_type, conv_type)
		.build().unwrap();

	let cp = create_primitive_appender::<T, TDataType, _, _>(&c, convert);
	(Box::new(cp), t)
}

fn resolve_vector_conv<TArr: for<'a> FromSql<'a> + Clone + IntoIterator<Item=T> + 'static, T: Clone + 'static, TDataType, FConversion: Fn(T) -> TDataType::T + 'static, TRow: PgAbstractRow + Clone + 'static>(
	name: &str,
	c: &ColumnInfo,
	length: Option<i32>,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> ResolvedColumn<TRow>
	where TDataType: DataType, TDataType::T : RealMemorySize {

	let mut c = c.clone();
	c.definition_level += 1; // TODO: NOT NULL fields
	let t =
		build_primitive_pq_type("element", TDataType::get_physical_type(), length, logical_type, conv_type)
		.with_repetition(Repetition::REQUIRED)
		.build().unwrap();

	let arr_t = make_list_schema(name, Repetition::OPTIONAL, t);

	let inner_appender = GenericColumnAppender::<T, TDataType, FConversion>::new(c.definition_level + 1, c.repetition_level + 1, convert);
	let array_appender = ArrayColumnAppender::new(inner_appender, true, false, c.definition_level, c.repetition_level);

	let cp = wrap_pg_row_reader::<TRow, TArr>(&c, array_appender);
	(Box::new(cp), arr_t)
}

fn build_primitive_pq_type(name: &str, data_type: parquet::basic::Type, length: Option<i32>, logical_type: Option<LogicalType>, conv_type: Option<ConvertedType>) -> PrimitiveTypeBuilder {
	let mut t =
		ParquetType::primitive_type_builder(name, data_type)
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

	t.with_logical_type(logical_type)
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

fn create_array_appender<TRow: PgAbstractRow + Clone>(inner: DynColumnAppender<PgAny>, c: &ColumnInfo, warn_on_multidim: bool) -> impl ColumnAppender<TRow> {
	let outer_dl = c.definition_level + 1;
	debug_assert_eq!(outer_dl + 2, inner.max_dl());
	let array_appender = ArrayColumnAppender::new(inner, true, true, outer_dl, c.repetition_level);
	let warned = AtomicBool::new(false);
	let col_clone = c.clone();
	let multidim_appender = array_appender.preprocess(move |x: Cow<PgMultidimArray<Option<PgAny>>>| {
		if warn_on_multidim && x.dims.is_some() && !warned.load(Ordering::Relaxed) {
			if !warned.fetch_or(true, Ordering::SeqCst) {
				eprintln!("Warning: Column {} contains a {}-dimensional array which will be flattened in Parquet (i.e. {} -> {}). Use --array-handling=dimensions, include another column with the PostgreSQL array dimensions.",
					col_clone.full_name(),
					x.dims.as_ref().unwrap().len(),
					x.dims.as_ref().unwrap().iter().map(|x| x.to_string()).collect::<Vec<_>>().join("x"),
					x.data.len()
				)
			}
		}
		match x {
			Cow::Owned(x) => Cow::Owned(x.data),
			Cow::Borrowed(x) => Cow::Borrowed(&x.data)
		}
	});
	wrap_pg_row_reader::<TRow, PgMultidimArray<Option<PgAny>>>(c, multidim_appender)
}

fn create_array_dim_appender<T: Clone + for <'a> FromSql<'a> + 'static, TRow: PgAbstractRow + Clone>(c: &ColumnInfo) -> impl ColumnAppender<TRow> {
	let int_appender = new_autoconv_generic_appender::<i32, Int32Type>(c.definition_level + 2, c.repetition_level + 1);
	let dim_appender =
		ArrayColumnAppender::new(int_appender, false, false, c.definition_level + 1, c.repetition_level)
			.preprocess(|x: Cow<PgMultidimArray<Option<T>>>| Cow::<Vec<Option<i32>>>::Owned(
				x.dims.as_ref()
					.map(|x| x.iter().map(|c| Some(*c)).collect())
					.unwrap_or_else(|| if x.data.len() == 0 { Vec::new() } else { vec![Some(x.data.len() as i32)] })
			));
	wrap_pg_row_reader::<TRow, PgMultidimArray<Option<T>>>(c, dim_appender)
}


fn create_array_lower_bound_appender<T: Clone + for <'a> FromSql<'a> + 'static, TRow: PgAbstractRow + Clone>(c: &ColumnInfo) -> impl ColumnAppender<TRow> {
	let int_appender = new_autoconv_generic_appender::<i32, Int32Type>(c.definition_level + 2, c.repetition_level + 1);
	let dim_appender =
		ArrayColumnAppender::new(int_appender, false, false, c.definition_level + 1, c.repetition_level)
			.preprocess(|x: Cow<PgMultidimArray<Option<T>>>| Cow::<Vec<Option<i32>>>::Owned(
				match &x.lower_bounds {
					_ if x.data.len() == 0 => Vec::new(),
					PgMultidimArrayLowerBounds::Const(c) => vec![Some(*c); x.dims.as_ref().map(|x| x.len()).unwrap_or(1)],
					PgMultidimArrayLowerBounds::PerDim(v) => v.iter().map(|x| Some(*x)).collect()
				}
			));
	wrap_pg_row_reader::<TRow, PgMultidimArray<Option<T>>>(c, dim_appender)
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


