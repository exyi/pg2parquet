use std::collections::HashMap;
use std::fmt::{Display, Write as _};
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::IpAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::error::Error;
use futures_util::StreamExt;
use parquet::basic::{self, ConvertedType, IntType, LogicalType, Repetition};
use parquet::data_type::{DataType, BoolType, Int32Type, Int64Type, FloatType, DoubleType, ByteArray, ByteArrayType, FixedLenByteArrayType, FixedLenByteArray};
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use pg_bigdecimal::PgNumeric;
use postgres::error::SqlState;
use postgres::types::{Kind, Type as PgType, FromSql};
use postgres::{self, Client, Row, Column, NoTls};
use parquet::schema::types::{GroupTypeBuilder, PrimitiveTypeBuilder, Type as ParquetType, TypePtr};
use half::f16;
use arrow::datatypes::{Schema as ArrowSchema, Field as ArrowField};
use arrow::record_batch::RecordBatch;

use crate::datatypes::array::{PgMultidimArray, PgMultidimArrayLowerBounds};
use crate::datatypes::pgvector::{self, PgSparseVector};
use crate::PostgresConnArgs;
use crate::appenders::{ArrayColumnAppender, BasicPgRowColumnAppender, BorrowedPgRowColumnAppender, ByteArrayColumnAppender, ColumnAppender, ColumnAppenderBase, DynColumnAppender, DynamicMergedAppender, FixedLenBinaryColumnAppender, PgBorrowHelper, PgBorrowRef, PreprocessAppender, PreprocessExt, PrimitiveColumnAppender, PrimitiveParquetType, RealMemorySize, StaticMergedAppender, UnwrapOptionAppender, new_autoconv_generic_appender, new_static_merged_appender};
use crate::appenders::dictionary::DictionaryColumnAppender;
use crate::datatypes::interval::PgInterval;
use crate::datatypes::jsonb::PgRawJsonbBorrow;
use crate::datatypes::xml::PgXmlBorrow;
use crate::datatypes::money::PgMoney;
use crate::datatypes::network::PgNetwork;
use crate::datatypes::numeric::{new_decimal_bytes_appender, new_decimal_int_appender};
use crate::datatypes::raw_bytes::PgRawBytesBorrow;
use crate::myfrom::{MyFrom, self};
use crate::parquet_writer::{WriterStats, WriterSettings};
use crate::pg_custom_types::{PgEnum, PgRawRange, PgAbstractRow, PgRawRecord, PgAny};
use crate::postgresutils::identify_row;

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
fn build_tls_connector(certificates: &Option<Vec<PathBuf>>, accept_invalid_certs: bool, client_cert_key: Option<(&PathBuf, &PathBuf)>) -> Result<postgres_native_tls::MakeTlsConnector, String> {
    use native_tls::Identity;

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
	if let Some((client_cert, client_key)) = client_cert_key {
		let cert = std::fs::read(client_cert).map_err(|e| format!("Failed to read TLS client certificate {client_cert:?}: {e}"))?;
		let key = std::fs::read(client_key).map_err(|e| format!("Failed to read TLS client key {client_key:?}: {e}"))?;
		let identity = Identity::from_pkcs8(&cert, &key).map_err(|e| format!("Failed to parse TLS client certificate: {e}"))?;
		builder.identity(identity);
	}
	let connector = builder.build().map_err(|e| format!("Creating TLS connector failed: {}", e.to_string()))?;
	let pg_connector = postgres_native_tls::MakeTlsConnector::new(connector);
	Ok(pg_connector)
}

#[cfg(not(any(target_os = "macos", target_os="windows", all(target_os="linux", not(target_env="musl"), any(target_arch="x86_64", target_arch="aarch64")))))]
fn build_tls_connector(certificates: &Option<Vec<PathBuf>>, allow_invalid_certs: bool, client_cert_key: Option<(&PathBuf, &PathBuf)>) -> Result<NoTls, String> {
	if certificates.is_some() || client_cert_key.is_some() {
		return Err("SSL/TLS is not supported in this build of pg2parquet".to_string());
	}
	Ok(NoTls)
}

fn process_conn_error(err: postgres::error::Error, args: &PostgresConnArgs) -> String {
	let mut message = err.to_string();
	if message.contains("error performing TLS handshake") && message.contains("certificate verify failed") {
		message += "\n\n";
		let looks_like_ip = args.host.as_ref().is_some_and(|h| h.parse::<IpAddr>().is_ok());

		if args.ssl_root_cert.is_some() { message += "The certificate provided in --ssl-root-cert does't match the server certificate. You can use the option multiple times if you need to allow multiple certificates."; }
		else { message += "To verify self-signed certificates, you can use the --ssl-root-cert option. You might also use the insecure --sslmode=prefer option which does not check the certificate."; }
		if looks_like_ip { message += " Note that certificate verification commonly does not work when directly connecting to the server IP adress. You might need to specify a domain name matching the one in the certificate."; }
		_ = write!(message, "\nYou can use `openssl s_client -showcerts -connect {}:{} -starttls postgres` to see which certificate is presented by the server.", args.host.as_ref().unwrap_or(&"<HOST>".to_string()), args.port.unwrap_or(5432));
	}

	format!("DB connection failed: {}", message)
}

fn use_env_var(name: &str, label: &str, quiet: bool) -> Option<String> {
	match std::env::var(name) {
		Err(_) => None,
		Ok(var) => {
			if !quiet {
				println!("Reading {label} from `{name}` environment variable.");
			}
			Some(var)
		}
	}
}

fn process_connection_string_parse_error(_args: &PostgresConnArgs, conn_str: &str, e: postgres::Error) -> String {
	let mut help = String::new();
	if conn_str.contains("sslrootcert=") || conn_str.contains("sslcert=") || conn_str.contains("sslkey=") {
		help += "\nThe sslrootcert, sslcert and sslkey options are not supported in the connection string. You can use --ssl-root-cert, --ssl-client-cert, --ssl-client-key CLI options instead to configure the TLS connection.";
	}
	format!("Failed to parse connection string: {e}{help}")
}

async fn pg_connect(args: &PostgresConnArgs, quiet: bool) -> Result<(Client, tokio::task::JoinHandle<Result<(), postgres::Error>>), String> {
	let mut config = postgres::Config::new();

	let connection_string = args.connection_string.clone()
		.or_else(|| use_env_var("POSTGRES_URL", "DB connection URL", quiet))
		.or_else(|| use_env_var("DATABASE_URL", "DB connection URL", quiet));
	if let Some(connection_string) = connection_string {
		config = connection_string.parse::<postgres::Config>()
			.map_err(|e| process_connection_string_parse_error(args, &connection_string, e))?;
	} else {
		let host = args.host.as_ref().unwrap();
		let dbname = args.dbname.as_ref().unwrap();
		config
			.host(host)
			.dbname(dbname)
			.port(args.port.unwrap_or(5432))
			.user(&args.user.clone().or_else(|| use_env_var("PGUSER", "DB user", quiet)).unwrap_or_else(|| dbname.clone()));

		#[cfg(not(any(target_os = "macos", target_os="windows", all(target_os="linux", not(target_env="musl"), any(target_arch="x86_64", target_arch="aarch64")))))]
		match &args.sslmode {
			None | Some(crate::SslMode::Disable) => {},
			Some(x) => return Err(format!("SSL/TLS is disabled in this build of pg2parquet, so ssl mode {:?} cannot be used. Only 'disable' option is allowed.", x)),
		}
		match &args.sslmode {
			None => {
				if args.ssl_root_cert.is_some() {
					config.ssl_mode(postgres::config::SslMode::Require);
				} else {
					config.ssl_mode(postgres::config::SslMode::Prefer);
				}
			},
			Some(crate::SslMode::Disable) => {
				config.ssl_mode(postgres::config::SslMode::Disable);
			},
			Some(crate::SslMode::Prefer) => {
				config.ssl_mode(postgres::config::SslMode::Prefer);
			},
			Some(crate::SslMode::Require) => {
				config.ssl_mode(postgres::config::SslMode::Require);
			},
		}
	};

	if let Some(password) = args.password.as_ref() {
		config.password(password);
	} else if !config.get_password().is_none() {

	} else if let Some(password) = use_env_var("PGPASSWORD", "DB password", quiet) {
		config.password(&password);
	} else {
		config.password(&read_password(config.get_user().unwrap())?.trim());
	}

	if config.get_application_name().is_none() {
		config.application_name("pg2parquet");
	}

	// `Prefer` mode is succeptible to active attackers anyway due to downgrades,
	// so we can allow untrusted (self-signed) certificates
	let allow_invalid_certs = matches!(config.get_ssl_mode(), postgres::config::SslMode::Prefer);
	let client_cert = match (&args.ssl_client_cert, &args.ssl_client_key) {
		(Some(cert), Some(key)) => Some((cert, key)),
		(None, None) => None,
		(None, Some(_)) | (Some(_), None) => return Err(format!("Options ssl-client-cert and ssl-client-key must both be specified."))
	};
	let connector = build_tls_connector(&args.ssl_root_cert, allow_invalid_certs, client_cert)?;

	let (client, connection) = config.connect(connector).await.map_err(|e| process_conn_error(e, args))?;
	let connection = tokio::spawn(connection);

	Ok((client, connection))
}

pub fn execute_copy(
	pg_args: PostgresConnArgs,
	query: String,
	output_file: PathBuf,
	output_props: WriterPropertiesPtr,
	quiet: bool,
	schema_settings: SchemaSettings,
	threads: usize,
	row_group_byte_limit: usize,
) -> Result<WriterStats, String> {
	let threads = if threads == 0 {
		std::thread::available_parallelism().map_or(1, |x| x.get())
	} else {
		threads
	};

	let runtime = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(threads)
		.enable_all()
		.build()
		.map_err(|e| format!("Failed to create worker runtime: {e}"))?;
	let task = runtime.spawn(async move {
		execute_copy_async(
			&pg_args, &query, &output_file, output_props, quiet, &schema_settings,
			row_group_byte_limit,
		).await
	});
	runtime.block_on(task)
		.map_err(|e| format!("Export task failed: {e}"))?
}

async fn execute_copy_async(
	pg_args: &PostgresConnArgs,
	query: &str,
	output_file: &PathBuf,
	output_props: WriterPropertiesPtr,
	quiet: bool,
	schema_settings: &SchemaSettings,
	row_group_byte_limit: usize,
) -> Result<WriterStats, String> {

	let (client, connection) = pg_connect(pg_args, quiet).await?;
	let statement = client.prepare(query).await.map_err(|db_err| { format!("Prepare statement failed: {db_err} ({})", db_err.as_db_error().map_or("no message", |e| e.message())) })?;

	if statement.columns().is_empty() {
		return Err("The query must return at least one column".to_string());
	}
	let (mut col_appenders, schema) = map_schema_root(statement.columns(), schema_settings)?;
	assert!(col_appenders.len() > 0);
	if !quiet {
		eprintln!("Schema: {}", format_schema(&schema, 0));
	}

	let fields = col_appenders.iter_mut().zip(schema.get_fields())
		.map(|(appender, field)| {
			let array = appender.to_arrow_array()
				.map_err(|e| format!("Failed to get data type from column {}: {e}", field.name()))?;
			let (name, nullable) = field_metadata(field);
			Ok(ArrowField::new(name, array.data_type().clone(), nullable))
		})
		.collect::<Result<Vec<_>, String>>()?;
	let arrow_schema = Arc::new(ArrowSchema::new(fields));
	let schema = Arc::new(schema);
	let output_props = crate::parquet_writer::disable_binary_decimal_statistics(schema.clone(), output_props);
	let settings = WriterSettings {
		row_group_byte_limit,
		row_group_row_limit: output_props.max_row_group_row_count().unwrap_or(usize::MAX),
	};

	let output_file_f = std::fs::File::create(output_file).map_err(|e|
		format!("Cannot create the output file {output_file:?}: {e}")
	)?;
	let pq_writer = SerializedFileWriter::new(output_file_f, schema, output_props)
		.map_err(|e| format!("Failed to create parquet writer: {e}"))?;
	let mut writer = crate::arrow_parallel_writer::ArrowParallelWriter::new(
		pq_writer, arrow_schema.clone(), settings,
	);

	let flush_batch = |appenders: &mut [DynColumnAppender<Row>]| {
		let arrays = appenders.iter_mut().enumerate()
			.map(|(i, appender)| appender.to_arrow_array()
				.map_err(|e| format!("Failed Arrow conversion of column {}: {e}", arrow_schema.fields[i].name())))
			.collect::<Result<Vec<_>, _>>()?;
		RecordBatch::try_new(arrow_schema.clone(), arrays)
			.map_err(|e| format!("Failed to create RecordBatch: {e}"))
	};

	let batch_row_limit = settings.row_group_row_limit.min(64 * 1024);
	let batch_byte_limit = settings.row_group_byte_limit.min(16 * 1024 * 1024);
	let mut batch_rows = 0;
	let mut batch_bytes = 0;
	{
		let rows = client.query_raw::<_, &i32, &[i32]>(&statement, &[]).await
			.map_err(|err| format!("Failed to execute the SQL query: {err}"))?;
		tokio::pin!(rows);
		let mut row_i = 0;
		while let Some(row) = rows.next().await {
			let row = row.map_err(|err| format!("Error loading row from postgres: {err}"))?;
			for (col_i, appender) in col_appenders.iter_mut().enumerate() {
				batch_bytes += appender.copy_value(&row)
					.map_err(|e| format!(
						"Failed to copy row {row_i} [{}], column {col_i} ({}): {e}",
						identify_row(&row),
						arrow_schema.field(col_i).name(),
					))?;
			}
			row_i += 1;
			batch_rows += 1;
			if batch_rows >= batch_row_limit || batch_bytes >= batch_byte_limit {
				let batch = flush_batch(&mut col_appenders)?;
				writer.write_batch(&batch, batch_bytes).await?;
				if !quiet {
					writer.print_stats(false);
				}
				batch_rows = 0;
				batch_bytes = 0;
			}
		}
	}
	if batch_rows > 0 {
		let batch = flush_batch(&mut col_appenders)?;
		writer.write_batch(&batch, batch_bytes).await?;
	}

	if !quiet {
		writer.flush_row_group().await?;
		writer.print_stats(true);
	}
	let stats = writer.close().await?;
	drop((statement, client));
	connection.await
		.map_err(|e| format!("PostgreSQL connection task failed: {e}"))?
		.map_err(|e| format!("PostgreSQL connection failed: {e}"))?;
	Ok(stats)
}

fn format_schema(schema: &ParquetType, indent: u32) -> String {
	fn format_time_unit(u: &basic::TimeUnit) -> &str {
		match u {
			basic::TimeUnit::MILLIS => "ms",
			basic::TimeUnit::MICROS => "µs",
			basic::TimeUnit::NANOS => "ns",
		}
	}
	fn format_logical_type(t: &LogicalType) -> String {
		match t {
			LogicalType::Decimal(decimal) => format!("Decimal({}, {})", decimal.precision, decimal.scale),
			LogicalType::Time(time) =>
				format!("Time({}, utc={:?})", format_time_unit(&time.unit), time.is_adjusted_to_u_t_c),
			LogicalType::Timestamp(timestamp) =>
				format!("Timestamp({}, utc={:?})", format_time_unit(&timestamp.unit), timestamp.is_adjusted_to_u_t_c),
			LogicalType::Integer(integer) => {
				let sign = if integer.is_signed { "" } else { "U" };
				format!("{}Int{}", sign, integer.bit_width)
			}
			_ => format!("{:?}", t)
		}
	}
	let basic_info = schema.get_basic_info();
	let logical_type =
		basic_info.logical_type_ref().map(format_logical_type)
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

			if *precision > 0 && !matches!(basic_info.logical_type_ref(), Some(LogicalType::Decimal(_))) {
				additional_info += &format!(" precision: {}", precision);
			}
			if *scale > 0 && !matches!(basic_info.logical_type_ref(), Some(LogicalType::Decimal(_))) {
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

fn map_schema_root(row: &[Column], s: &SchemaSettings) -> Result<(Vec<DynColumnAppender<Row>>, ParquetType), String> {
	let mut fields: Vec<ResolvedColumn<Row>> = vec![];
	for (col_i, c) in row.iter().enumerate() {

		let t = c.type_();

		let schema = map_schema_column(t, &ColumnInfo::root(col_i, c.name().to_owned()), s)?;
		fields.push(schema)
	}


	let (column_appenders, parquet_types): (Vec<_>, Vec<_>) = fields.into_iter().unzip();

	let struct_type = ParquetType::group_type_builder("root")
		.with_fields(parquet_types.into_iter().map(Arc::new).collect())
		.build()
		.unwrap();

	Ok((column_appenders, struct_type))
}

fn map_schema_column<TRow: PgAbstractRow + 'static>(
	t: &PgType,
	c: &ColumnInfo,
	settings: &SchemaSettings,
) -> Result<ResolvedColumn<TRow>, String> {
	match t.kind() {
		Kind::Simple =>
			map_simple_type(t, c, settings),
		Kind::Enum(ref enum_data) =>
			match settings.enum_handling {
				SchemaSettingsEnumHandling::Int => {
					let mut mapping = HashMap::new();
					for (i, v) in enum_data.iter().enumerate() {
						mapping.insert(v.to_string(), i as i32 + 1);
					}
					Ok(resolve_primitive_conv::<PgEnum, Int32Type, _, _>(c.col_name(), c, None, None, None, move |e|
						*mapping.get(&e.name).unwrap_or_else(|| panic!("Could not map enum value {}. Was new enum case added while pg2parquet is running?", &e.name))
					))
				},
				SchemaSettingsEnumHandling::Text =>
					// TODO: small dictionary for small enums
					Ok(resolve_dictionary::<PgEnum, _, _>(c.col_name(), c, Some(LogicalType::Enum), |v| v.name.as_bytes())),
				SchemaSettingsEnumHandling::PlainText =>
					Ok(resolve_dictionary::<PgEnum, _, _>(c.col_name(), c, Some(LogicalType::String), |v| v.name.as_bytes())),
			}
		Kind::Array(ref element_type) => {
			let list_column = c.nest("list", 0).as_array();
			let element_column = list_column.nest("element", 0);

			let (element_appender, element_schema) = map_schema_column::<PgAny>(element_type, &element_column, settings)?;
			debug_assert_eq!(element_schema.name(), "element");

			let plain_schema = settings.array_handling == SchemaSettingsArrayHandling::Plain;
			let schema = if plain_schema {
				make_list_schema(c.col_name(), Repetition::OPTIONAL, element_schema.clone())
			} else {
				make_list_schema("data", Repetition::REQUIRED, element_schema.clone())
			};

			// Build array appender for the logical value type: PgMultidimArray<Option<PgAny>>
			let array_inner = ArrayColumnAppender::new(element_appender, true, true);
			let warned = AtomicBool::new(false);
			let col_clone = c.clone();
			let array_for_value = array_inner.preprocess_ref(move |x: &PgMultidimArray<Option<PgAny>>| {
				if plain_schema && x.dims.is_some() && !warned.load(Ordering::Relaxed) {
					if !warned.fetch_or(true, Ordering::SeqCst) {
						eprintln!("Warning: Column {} contains a {}-dimensional array which will be flattened in Parquet (i.e. {} -> {}). Use --array-handling=dimensions, include another column with the PostgreSQL array dimensions.",
							col_clone.full_name(),
							x.dims.as_ref().unwrap().len(),
							x.dims.as_ref().unwrap().iter().map(|x| x.to_string()).collect::<Vec<_>>().join("x"),
							x.data.len()
						)
					}
				}
				&x.data
			});

			// Build dims and lower_bound appenders for the logical value type
			let int_appender = new_autoconv_generic_appender::<i32, Int32Type>();
			let dims_for_value = ArrayColumnAppender::new(int_appender, false, false)
				.preprocess(|x: &PgMultidimArray<Option<PgAny>>|
					x.dims.as_ref()
						.map(|x| x.iter().map(|c| Some(*c)).collect())
						.unwrap_or_else(|| if x.data.len() == 0 { Vec::new() } else { vec![Some(x.data.len() as i32)] })
				);
			let lb_for_value = ArrayColumnAppender::new(new_autoconv_generic_appender::<i32, Int32Type>(), false, false)
				.preprocess(|x: &PgMultidimArray<Option<PgAny>>|
					match &x.lower_bounds {
						_ if x.data.len() == 0 => Vec::new(),
						PgMultidimArrayLowerBounds::Const(c) => vec![Some(*c); x.dims.as_ref().map(|x| x.len()).unwrap_or(1)],
						PgMultidimArrayLowerBounds::PerDim(v) => v.iter().map(|x| Some(*x)).collect()
					}
				);

			let dim_schema = make_list_schema("dims", Repetition::REQUIRED, ParquetType::primitive_type_builder("element", basic::Type::INT32).with_repetition(Repetition::REQUIRED).with_logical_type(Some(LogicalType::integer(32, false))).build().unwrap());
			let lb_schema = make_list_schema("lower_bound", Repetition::REQUIRED, ParquetType::primitive_type_builder("element", basic::Type::INT32).with_repetition(Repetition::REQUIRED).with_logical_type(Some(LogicalType::integer(32, true))).build().unwrap());

			match settings.array_handling {
				SchemaSettingsArrayHandling::Plain => {
					let wrapped = wrap_pg_row_reader::<TRow, PgMultidimArray<Option<PgAny>>>(&c, array_for_value);
					Ok((Box::new(wrapped), schema))
				},
				SchemaSettingsArrayHandling::Dimensions => {
					let children: Vec<DynColumnAppender<PgMultidimArray<Option<PgAny>>>> = vec![
						Box::new(array_for_value),
						Box::new(dims_for_value),
					];
					let dynm = DynamicMergedAppender::new(children, vec![field_metadata(&schema), field_metadata(&dim_schema)]);
					let dynm_opt = UnwrapOptionAppender::new(dynm);
					let wrapped = wrap_pg_row_reader::<TRow, Option<PgMultidimArray<Option<PgAny>>>>(&c, dynm_opt);
					Ok((Box::new(wrapped),
						ParquetType::group_type_builder(c.col_name())
							.with_repetition(Repetition::OPTIONAL)
							.with_fields(vec![ Arc::new(schema), Arc::new(dim_schema) ])
							.build().unwrap()))
				},
				SchemaSettingsArrayHandling::DimensionsAndLowerBound => {
					let children: Vec<DynColumnAppender<PgMultidimArray<Option<PgAny>>>> = vec![
						Box::new(array_for_value),
						Box::new(dims_for_value),
						Box::new(lb_for_value),
					];
					let dynm = DynamicMergedAppender::new(children, vec![field_metadata(&schema), field_metadata(&dim_schema), field_metadata(&lb_schema)]);
					let dynm_opt = UnwrapOptionAppender::new(dynm);
					let wrapped = wrap_pg_row_reader::<TRow, Option<PgMultidimArray<Option<PgAny>>>>(&c, dynm_opt);
					Ok((Box::new(wrapped),
						ParquetType::group_type_builder(c.col_name())
							.with_repetition(Repetition::OPTIONAL)
							.with_fields(vec![ Arc::new(schema), Arc::new(dim_schema), Arc::new(lb_schema) ])
							.build().unwrap()))
				},
			}
		},
		Kind::Domain(ref element_type) => {
			map_schema_column(element_type, c, settings)
		},
		&Kind::Range(ref element_type) => {
			let col_lower = map_schema_column::<PgRawRange>(element_type, &c.nest("lower", 0), settings)?;
			let col_upper = map_schema_column::<PgRawRange>(element_type, &c.nest("upper", 1), settings)?;

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

			// Build a dynamic merged appender so Arrow Struct export is supported
			let mut children: Vec<DynColumnAppender<PgRawRange>> = Vec::new();
			children.push(col_lower.0);
			children.push(col_upper.0);
			children.push(Box::new(new_autoconv_generic_appender::<bool, BoolType>()
				.preprocess(|r: &PgRawRange| r.lower_inclusive)));
			children.push(Box::new(new_autoconv_generic_appender::<bool, BoolType>()
				.preprocess(|r: &PgRawRange| r.upper_inclusive)));
			children.push(Box::new(new_autoconv_generic_appender::<bool, BoolType>()
				.preprocess(|r: &PgRawRange| r.is_empty)));
			let dynm = DynamicMergedAppender::new(children, schema.get_fields().iter().map(|f| field_metadata(f)).collect());

			let appender_dyn = wrap_pg_row_reader(c, dynm);

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

			let appender = create_complex_appender::<PgRawRecord, TRow>(c, column_appenders, schema.get_fields().iter().map(|f| field_metadata(f)).collect());

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

fn map_simple_type<TRow: PgAbstractRow + 'static>(
	t: &PgType,
	c: &ColumnInfo,
	s: &SchemaSettings,
) -> Result<ResolvedColumn<TRow>, String> {
	let name = c.col_name();

	Ok(match t.name() {
		"bool" => resolve_primitive::<bool, BoolType, _>(name, c, None, None),
		"int2" => resolve_primitive::<i16, Int32Type, _>(name, c, Some(LogicalType::integer(16, true)), None),
		"int4" => resolve_primitive::<i32, Int32Type, _>(name, c, None, None),
		"oid" => resolve_primitive::<u32, Int32Type, _>(name, c, Some(LogicalType::integer(32, false)), None),
		"int8" => resolve_primitive::<i64, Int64Type, _>(name, c, None, None),
		"float4" => resolve_primitive::<f32, FloatType, _>(name, c, None, None),
		"float8" => resolve_primitive::<f64, DoubleType, _>(name, c, None, None),
		"numeric" => {
			resolve_numeric(s, name, c)?
		},
		"money" => resolve_primitive::<PgMoney, Int64Type, _>(name, c, Some(LogicalType::decimal(2, 18)), None),
		"char" => resolve_primitive::<i8, Int32Type, _>(name, c, Some(LogicalType::integer(8, false)), None),
		"bytea" => resolve_borrowed_binary::<PgBorrowRef<[u8]>, _, _>(name, c, None, None, |x| x),
		"name" | "text" | "bpchar" | "varchar" | "citext" =>
			resolve_borrowed_binary::<PgBorrowRef<str>, _, _>(name, c, Some(LogicalType::String), Some(ConvertedType::UTF8), str::as_bytes),
			// resolve_borrowed_binary::<PgRawBytesBorrow, _, _>(name, c, Some(LogicalType::String), Some(ConvertedType::UTF8), |x| x),
		"xml" => resolve_borrowed_binary::<PgXmlBorrow, _, _>(name, c, Some(LogicalType::String), Some(ConvertedType::UTF8), str::as_bytes),
		"jsonb" | "json" => {
			let logical_type = Some(match s.json_handling {
				SchemaSettingsJsonHandling::Text => LogicalType::String,
				SchemaSettingsJsonHandling::TextMarkedAsJson => LogicalType::Json
			});
			resolve_borrowed_binary::<PgRawJsonbBorrow, _, _>(name, c, logical_type, None, str::as_bytes)
		},
		"timestamptz" =>
			resolve_primitive::<chrono::DateTime<chrono::Utc>, Int64Type, _>(name, c, Some(LogicalType::timestamp(true, basic::TimeUnit::MICROS)), None),
		"timestamp" =>
			resolve_primitive::<chrono::NaiveDateTime, Int64Type, _>(name, c, Some(LogicalType::timestamp(false, basic::TimeUnit::MICROS)), None),
		"date" =>
			resolve_primitive::<chrono::NaiveDate, Int32Type, _>(name, c, Some(LogicalType::Date), None),
		"time" =>
			resolve_primitive::<chrono::NaiveTime, Int64Type, _>(name, c, Some(LogicalType::time(false, basic::TimeUnit::MICROS)), None),

		"uuid" =>
			resolve_fixlen_binary::<16, uuid::Uuid, _, _>(name, c, Some(LogicalType::Uuid), None, |v| Some(v.into_bytes())),

		"macaddr" =>
			match s.macaddr_handling {
				SchemaSettingsMacaddrHandling::Text =>
					resolve_binary::<eui48::MacAddress, _>(name, c, Some(LogicalType::String), None),
				SchemaSettingsMacaddrHandling::ByteArray =>
					resolve_fixlen_binary::<6, eui48::MacAddress, _, _>(name, c, None, None, |v| Some(v.to_array())),
				SchemaSettingsMacaddrHandling::Int64 =>
					resolve_primitive::<eui48::MacAddress, Int64Type, _>(name, c, Some(LogicalType::integer(64, false)), None),
			},
		"inet" | "cidr" =>
			resolve_binary::<PgNetwork, _>(name, c, Some(LogicalType::String), None),
		"bit" | "varbit" =>
			resolve_binary::<bit_vec::BitVec, _>(name, c, Some(LogicalType::String), None),

		"interval" =>
			match s.interval_handling {
				SchemaSettingsIntervalHandling::Interval =>
					resolve_fixlen_binary::<12, PgInterval, _, _>(name, c, None, Some(ConvertedType::INTERVAL), |v| Some(v.to_parquet_bytes())),
				SchemaSettingsIntervalHandling::Struct => {
					let t = GroupTypeBuilder::new(c.col_name())
						.with_repetition(Repetition::OPTIONAL)
						.with_fields(vec![
							Arc::new(ParquetType::primitive_type_builder("months", basic::Type::INT32).build().unwrap()),
							Arc::new(ParquetType::primitive_type_builder("days", basic::Type::INT32).build().unwrap()),
							Arc::new(ParquetType::primitive_type_builder("microseconds", basic::Type::INT64).build().unwrap()),
						])
						.build().unwrap();
					// Build a dynamic merged appender so Arrow Struct export is supported
					let mut children: Vec<DynColumnAppender<PgInterval>> = Vec::new();
					children.push(Box::new(new_autoconv_generic_appender::<i32, Int32Type>()
						.preprocess(|i: &PgInterval| i.months)));
					children.push(Box::new(new_autoconv_generic_appender::<i32, Int32Type>()
						.preprocess(|i: &PgInterval| i.days)));
					children.push(Box::new(new_autoconv_generic_appender::<i64, Int64Type>()
						.preprocess(|i: &PgInterval| i.microseconds)));
					let dynm = DynamicMergedAppender::new(children, t.get_fields().iter().map(|f| field_metadata(f)).collect());
					(Box::new(wrap_pg_row_reader(c, dynm)), t)
				},
			},

		// TODO: Regproc Tid Xid Cid PgNodeTree Point Lseg Path Box Polygon Line Unknown Circle Macaddr8 Aclitem Bpchar Timetz Refcursor Regprocedure Regoper Regoperator Regclass Regtype TxidSnapshot PgLsn PgNdistinct PgDependencies TsVector Tsquery GtsVector Regconfig Regdictionary Jsonpath Regnamespace Regrole Regcollation PgMcvList PgSnapshot Xid9


		// pgvector extension: vector = 32-bit float array, halfvec = 16-bit float array, sparsevec = sparse f32 vector
		"vector" => resolve_vector_conv::<pgvector::PgF32Vector, f32, FloatType, _, TRow>(name, c, None, None, None, |v| *v),
		"halfvec" => match s.float16_handling {
			SchemaSettingsFloat16Handling::Float16 =>
				resolve_vector_binary_conv::<2, pgvector::PgF16Vector, f16, _, TRow>(name, c, Some(LogicalType::Float16), None, |v| Some(v.to_le_bytes())),
			SchemaSettingsFloat16Handling::Float32 =>
				resolve_vector_conv::<pgvector::PgF16Vector, f16, FloatType, _, TRow>(name, c, None, None, None, |v| (*v).into())
		},
		"sparsevec" => {
			let inner_appender = new_static_merged_appender::<(i32, f32)>()
				// index+1, because pgvector uses 0-based in binary, but 1-based in text and operators
				.add_appender("key", false, PrimitiveColumnAppender::<_, Int32Type, _>::new(|v: &(i32, f32)| v.0 + 1))
				.add_appender("value", false, PrimitiveColumnAppender::<_, FloatType, _>::new(|v: &(i32, f32)| v.1));

			let schema = ParquetType::group_type_builder(name)
				.with_repetition(Repetition::OPTIONAL)
				.with_fields(vec![
					Arc::new(ParquetType::group_type_builder("key_value").with_repetition(Repetition::REPEATED).with_fields(vec![
						Arc::new(ParquetType::primitive_type_builder("key", basic::Type::INT32)
							.with_repetition(Repetition::REQUIRED)
							.with_logical_type(Some(LogicalType::integer(32, false)))
							.build().unwrap()),
						Arc::new(ParquetType::primitive_type_builder("value", basic::Type::FLOAT)
							.with_repetition(Repetition::REQUIRED)
							.build().unwrap())
					]).build().unwrap())
				])
				.with_converted_type(ConvertedType::MAP)
				.with_logical_type(Some(LogicalType::Map))
				.build().unwrap();

			let array_appender = ArrayColumnAppender::new(inner_appender, true, false).with_element_name("key_value");

			(Box::new(wrap_pg_row_reader::<TRow, PgSparseVector>(&c, array_appender)), schema)
		}


		n => 
			return Err(format!("Could not map column {}, unsupported primitive type: {}", c.full_name(), n)),
	})
}

fn resolve_numeric<TRow: PgAbstractRow + 'static>(s: &SchemaSettings, name: &str, c: &ColumnInfo) -> Result<ResolvedColumn<TRow>, String> {
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
				.with_logical_type(Some(LogicalType::decimal(scale, precision as i32)))
				.with_precision(precision as i32)
				.with_scale(scale)
				.build().unwrap();
		let cp: DynColumnAppender<TRow> = if pq_type == basic::Type::INT32 {
				let appender = new_decimal_int_appender::<i32, Int32Type>(precision, scale);
				Box::new(wrap_pg_row_reader(c, appender))
			} else if pq_type == basic::Type::INT64 {
				let appender = new_decimal_int_appender::<i64, Int64Type>(precision, scale);
				Box::new(wrap_pg_row_reader(c, appender))
			} else {
				let appender = new_decimal_bytes_appender(s.decimal_precision, s.decimal_scale);
				Box::new(wrap_pg_row_reader(c, appender))
			};
			Ok((cp, schema))
		},

		SchemaSettingsNumericHandling::Double =>
			Ok(resolve_primitive::<PgNumeric, DoubleType, _>(name, c, None, None)),
		SchemaSettingsNumericHandling::Float32 =>
			Ok(resolve_primitive::<PgNumeric, FloatType, _>(name, c, None, None)),
		SchemaSettingsNumericHandling::String =>
			Ok(resolve_binary_conv::<PgNumeric, _, _>(name, c, None, Some(LogicalType::String), None, |v: &PgNumeric| match &v.n {
				Some(n) => ByteArray::my_from(n.to_string()),
				None => ByteArray::my_from("NaN".to_string())
			}))
	}
}

fn resolve_primitive<T: for<'a> FromSql<'a> + Clone + Send + 'static, TDataType, TRow: PgAbstractRow + 'static>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>
) -> ResolvedColumn<TRow>
	where TDataType: PrimitiveParquetType, TDataType::T : Copy + RealMemorySize + MyFrom<T> {
	resolve_primitive_conv::<T, TDataType, _, TRow>(name, c, None, logical_type, conv_type, |v| MyFrom::my_from(v.clone()))
}

fn resolve_primitive_conv<T: for<'a> FromSql<'a> + Clone + Send + 'static, TDataType, FConversion: Fn(&T) -> TDataType::T + Send + 'static, TRow: PgAbstractRow + 'static>(
	name: &str,
	c: &ColumnInfo,
	length: Option<i32>,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> ResolvedColumn<TRow>
	where TDataType: PrimitiveParquetType, TDataType::T: Copy + RealMemorySize {
	let t =
		build_primitive_pq_type(name, TDataType::get_physical_type(), length, logical_type, conv_type)
		.build().unwrap();

	let cp = create_primitive_appender::<T, TDataType, _, _>(&c, convert);
	(Box::new(cp), t)
}

fn resolve_binary<T: for<'a> FromSql<'a> + Clone + Send + 'static, TRow: PgAbstractRow + 'static>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>
) -> ResolvedColumn<TRow>
	where ByteArray: MyFrom<T> {
	resolve_binary_conv::<T, _, TRow>(name, c, None, logical_type, conv_type, |v| MyFrom::my_from(v.clone()))
}

fn resolve_borrowed_binary<TBorrowed: PgBorrowHelper + Send + 'static, FBytes, TRow>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	to_bytes: FBytes,
) -> ResolvedColumn<TRow>
where
	TBorrowed::Value: Send + 'static,
	FBytes: for<'a> Fn(&'a TBorrowed::Value) -> &'a [u8] + Send + 'static,
	TRow: PgAbstractRow + 'static,
{
	let schema = build_primitive_pq_type(
		name, basic::Type::BYTE_ARRAY, None, logical_type, conv_type,
	).build().unwrap();
	let binary_appender = ByteArrayColumnAppender::<TBorrowed::Value, _>::new(to_bytes);
	(Box::new(BorrowedPgRowColumnAppender::<TBorrowed, _>::new(c.col_i, binary_appender)), schema)
}

fn resolve_dictionary<T, FConversion, TRow>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	convert: FConversion,
) -> ResolvedColumn<TRow>
where
	T: for<'a> FromSql<'a> + Send + 'static,
	FConversion: for<'a> Fn(&'a T) -> &'a [u8] + Send + 'static,
	TRow: PgAbstractRow + 'static,
{
	let schema = build_primitive_pq_type(
		name, basic::Type::BYTE_ARRAY, None, logical_type, None,
	).build().unwrap();
	mk_resolved(c.col_i, DictionaryColumnAppender::new(convert), schema)
}

fn resolve_binary_conv<T: for<'a> FromSql<'a> + Clone + Send + 'static, FConversion: Fn(&T) -> ByteArray + Send + 'static, TRow: PgAbstractRow + 'static>(
	name: &str,
	c: &ColumnInfo,
	length: Option<i32>,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> ResolvedColumn<TRow> {
	let t =
		build_primitive_pq_type(name, parquet::basic::Type::BYTE_ARRAY, length, logical_type, conv_type)
		.build().unwrap();

	let cp = create_binary_appender(&c, convert);
	(cp, t)
}

fn resolve_fixlen_binary<const N: usize, T: for<'a> FromSql<'a> + Clone + Send + 'static, FConversion: Fn(&T) -> Option<[u8; N]> + Send + 'static, TRow: PgAbstractRow + 'static>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> ResolvedColumn<TRow> {
	let t =
		build_primitive_pq_type(name, parquet::basic::Type::FIXED_LEN_BYTE_ARRAY, Some(N as i32), logical_type, conv_type)
		.build().unwrap();

	let appender = FixedLenBinaryColumnAppender::new(convert);
	mk_resolved::<TRow, T>(c.col_i, appender, t)
}

fn resolve_vector_conv<TArr, T: Send + 'static, TDataType, FConversion: Fn(&T) -> TDataType::T + Send + 'static, TRow: PgAbstractRow + 'static>(
	name: &str,
	c: &ColumnInfo,
	length: Option<i32>,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> ResolvedColumn<TRow>
	where TArr: for<'a> FromSql<'a> + Clone + Send + 'static,
	      for<'a> &'a TArr: IntoIterator<Item = &'a T>,
	      TDataType: PrimitiveParquetType, TDataType::T: Copy + RealMemorySize {

	let t =
		build_primitive_pq_type("element", TDataType::get_physical_type(), length, logical_type, conv_type)
		.with_repetition(Repetition::REQUIRED)
		.build().unwrap();

	let arr_t = make_list_schema(name, Repetition::OPTIONAL, t);

	let inner_appender = PrimitiveColumnAppender::<T, TDataType, FConversion>::new(convert);
	let array_appender = ArrayColumnAppender::new(inner_appender, true, false);

	mk_resolved::<TRow, TArr>(c.col_i, array_appender, arr_t)
}

fn resolve_vector_binary_conv<const N: usize, TArr, T: Clone + Send + 'static, FConversion: Fn(&T) -> Option<[u8; N]> + Send + 'static, TRow: PgAbstractRow + 'static>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> ResolvedColumn<TRow>
	where TArr: for<'a> FromSql<'a> + Clone + Send + 'static,
	      for<'a> &'a TArr: IntoIterator<Item = &'a T> {

	let t =
		build_primitive_pq_type("element", parquet::basic::Type::FIXED_LEN_BYTE_ARRAY, Some(N as i32), logical_type, conv_type)
		.with_repetition(Repetition::REQUIRED)
		.build().unwrap();

	let arr_t = make_list_schema(name, Repetition::OPTIONAL, t);

	let inner_appender = FixedLenBinaryColumnAppender::new(convert);
	let array_appender = ArrayColumnAppender::new(inner_appender, true, false);

	mk_resolved::<TRow, TArr>(c.col_i, array_appender, arr_t)
}

fn build_primitive_pq_type(name: &'_ str, data_type: parquet::basic::Type, length: Option<i32>, logical_type: Option<LogicalType>, conv_type: Option<ConvertedType>) -> PrimitiveTypeBuilder<'_> {
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
		Some(LogicalType::Decimal(decimal)) => {
			t = t.with_precision(decimal.precision).with_scale(decimal.scale);
		},
		_ => {}
	};

	t.with_logical_type(logical_type)
}

fn create_primitive_appender<T: for <'a> FromSql<'a> + Send + 'static, TDataType, FConversion: Fn(&T) -> TDataType::T + Send + 'static, TRow: PgAbstractRow>(
	c: &ColumnInfo,
	convert: FConversion
) -> impl ColumnAppender<TRow>
	where TDataType: PrimitiveParquetType, TDataType::T: Copy + RealMemorySize {
	let basic_appender: PrimitiveColumnAppender<T, TDataType, _> = PrimitiveColumnAppender::new(convert);
	wrap_pg_row_reader(c, basic_appender)
}

fn create_binary_appender<T: for <'a> FromSql<'a> + Send + 'static, FConversion: Fn(&T) -> ByteArray + Send + 'static, TRow: PgAbstractRow + 'static>(
	c: &ColumnInfo,
	convert: FConversion,
) -> DynColumnAppender<TRow> {
	let inner = PrimitiveColumnAppender::<_, ByteArrayType, _>::new(convert);
	Box::new(wrap_pg_row_reader(c, inner))
}

fn create_complex_appender<T: for <'a> FromSql<'a> + PgAbstractRow + 'static, TRow: PgAbstractRow>(c: &ColumnInfo, columns: Vec<DynColumnAppender<T>>, fields: Vec<(String, bool)>) -> impl ColumnAppender<TRow> {
	let main_cp = DynamicMergedAppender::new(columns, fields);
	wrap_pg_row_reader(c, main_cp)
}

fn wrap_pg_row_reader<TRow: PgAbstractRow, T: for <'a> FromSql<'a>>(c: &ColumnInfo, a: impl ColumnAppender<T>) -> impl ColumnAppender<TRow> {
	BasicPgRowColumnAppender::new(c.col_i, a)
}

fn mk_resolved<TRow: PgAbstractRow, T: for <'a> FromSql<'a> + Send + 'static>(c: usize, a: impl ColumnAppender<T> + Send + 'static, t: ParquetType) -> ResolvedColumn<TRow> {
	let pg_app = BasicPgRowColumnAppender::new(c, a);
	(Box::new(pg_app), t)
}

#[derive(Debug, Clone)]
struct ColumnInfo {
	pub names: Arc<Vec<String>>,
	pub col_i: usize,
	pub is_array: bool,
}
impl ColumnInfo {
	pub fn root(col_i: usize, name: String) -> ColumnInfo {
		ColumnInfo {
			names: Arc::new(vec![name]),
			col_i,
			is_array: false,
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
		}
	}

	fn as_array(&self) -> ColumnInfo {
		assert!(self.is_array == false, "Parquet does not support nested arrays");
		ColumnInfo {
			names: self.names.clone(),
			col_i: self.col_i,
			is_array: true,
		}
	}

	fn col_name(&self) -> &str {
		&self.names[self.names.len() - 1]
	}

	fn full_name(&self) -> String {
		self.names.join("/")
	}
}

fn field_metadata(t: &ParquetType) -> (String, bool) {
	(t.name().to_owned(), t.get_basic_info().repetition() != Repetition::REQUIRED)
}
