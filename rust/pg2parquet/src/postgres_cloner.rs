use std::fmt::Display;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::error::Error;
use parquet::basic::{Repetition, self, ConvertedType, LogicalType};
use parquet::data_type::{DataType, BoolType, Int32Type, Int64Type, FloatType, DoubleType, ByteArray, ByteArrayType, FixedLenByteArrayType, FixedLenByteArray};
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::TimestampType;
use postgres::error::SqlState;
use postgres::types::{Kind, Type as PgType, FromSql};
use postgres::{self, Client, NoTls, RowIter, Row, Column, Statement};
use postgres::fallible_iterator::FallibleIterator;
use parquet::schema::types::{Type as ParquetType, TypePtr};

use crate::PostgresConnArgs;
use crate::column_appender::{ColumnAppender, GenericColumnAppender, ArrayColumnAppender, RealMemorySize};
use crate::column_pg_copier::{ColumnCopier, BasicPgColumnCopier, MergedColumnCopier};
use crate::datatypes::jsonb::PgRawJsonb;
use crate::datatypes::money::PgMoney;
use crate::datatypes::numeric::new_decimal_bytes_appender;
use crate::myfrom::{MyFrom, self};
use crate::parquet_row_writer::{WriterStats, ParquetRowWriter, ParquetRowWriterImpl, WriterSettings};
use crate::pg_custom_types::{PgEnum, PgRawRange, PgAbstractRow, PgRawRecord};

type DynCopier<TRow> = Box<dyn ColumnCopier<TRow>>;
type DynRowCopier = DynCopier<Row>;
type ResolvedColumn<TRow> = (DynCopier<TRow>, ParquetType);

#[derive(Clone, Debug)]
pub struct SchemaSettings {
	macaddr_handling: SchemaSettingsMacaddrHandling,
	json_handling: SchemaSettingsJsonHandling,
	decimal_scale: i32,
	decimal_precision: u32,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SchemaSettingsMacaddrHandling {
	/// MAC address is converted to a string
	String,
	/// MAC is stored as fixed byte array of length 6
	ByteArray,
	/// MAC is stored in Int64 (lowest 6 bytes)
	Int64
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum SchemaSettingsJsonHandling {
	/// JSON is stored as a Parquet JSON type. This is essentially the same as string, but with a different ConvertedType, so it may not be supported in all tools.
	StringMarkedAsJson,
	/// JSON is stored as a string
	String
}

pub fn default_settings() -> SchemaSettings {
	SchemaSettings {
		macaddr_handling: SchemaSettingsMacaddrHandling::String,
		json_handling: SchemaSettingsJsonHandling::String, // DuckDB doesn't load JSON converted type, so better to use string I guess
		decimal_scale: 18,
		decimal_precision: 38,
	}
}

fn read_password(user: &str) -> Result<String, String> {
	print!("Password for user {}: ", user);
	io::stdout().flush().unwrap();
	let mut password = String::new();
	io::stdin().read_line(&mut password).map_err(|x| x.to_string())?;
	Ok(password)
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

	// TODO: SSL

	let client = pg_config.connect(NoTls).map_err(|e| format!("DB connection failed: {}", e.to_string()))?;

	Ok(client)
}

pub fn execute_copy(pg_args: &PostgresConnArgs, query: &str, output_file: &PathBuf, output_props: WriterPropertiesPtr, schema_settings: &SchemaSettings) -> Result<WriterStats, String> {

	let mut client = pg_connect(pg_args)?;
	let statement = client.prepare(query).map_err(|db_err| { db_err.to_string() })?;

	let (copier, schema) = map_schema_root(statement.columns(), schema_settings)?;
	eprintln!("Schema: {}", format_schema(&schema, 0));
	let schema = Arc::new(schema);

	let settings = WriterSettings { row_group_byte_limit: 500 * 1024 * 1024, row_group_row_limit: output_props.max_row_group_size() };

	let output_file_f = std::fs::File::create(output_file).unwrap();
	let pq_writer = SerializedFileWriter::new(output_file_f, schema.clone(), output_props)
		.map_err(|e| format!("Failed to create parquet writer: {}", e))?;
	let mut row_writer = ParquetRowWriterImpl::new(pq_writer, schema.clone(), copier, settings)
		.map_err(|e| format!("Failed to create row writer: {}", e))?;

	let rows: RowIter = client.query_raw::<Statement, &i32, &[i32]>(&statement, &[]).unwrap();
	for row in rows.iterator() {
		let row = row.map_err(|err| err.to_string())?;

		row_writer.write_row(&row)?;
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


fn map_schema_root(row: &[Column], s: &SchemaSettings) -> Result<ResolvedColumn<Row>, String> {
	let mut fields: Vec<ResolvedColumn<Row>> = vec![];
	for (col_i, c) in row.iter().enumerate() {

		let t = c.type_();

		let schema = map_schema_column(t, &ColumnInfo::root(col_i, c.name().to_owned()), s)?;
		fields.push(schema)
	}


	let (column_copiers, parquet_types): (Vec<_>, Vec<_>) = fields.into_iter().unzip();

	let merged_copier: Box<dyn ColumnCopier<Row>> = Box::new(MergedColumnCopier::<Row>::new(column_copiers, 0, 0));
	let struct_type = ParquetType::group_type_builder("root")
		.with_fields(&mut parquet_types.into_iter().map(Arc::new).collect())
		.build()
		.unwrap();

	Ok((merged_copier, struct_type))
}

fn map_schema_column<TRow: PgAbstractRow>(
	t: &PgType,
	c: &ColumnInfo,
	s: &SchemaSettings
) -> Result<ResolvedColumn<TRow>, String> {
	match t.kind() {
		Kind::Simple =>
			map_simple_type(t, c, s, CreateCopierCallback::new()),
		Kind::Enum(ref _enum_data) =>
			Ok(resolve_primitive::<PgEnum, ByteArrayType, _>(c.col_name(), c, CreateCopierCallback::new(), Some(LogicalType::Enum), None)),
			// resolve_primitive::<PgEnum, Int32Type>(name, c, None, None),
		Kind::Array(ref element_type) => {
			map_schema_column(element_type, &c.as_array(), s)
		},
		Kind::Domain(ref element_type) => {
			map_schema_column(element_type, c, s)
		},
		&Kind::Range(ref element_type) => {
			// cc.repetition_level += c.is_array as i16;
			let col_lower = map_schema_column(element_type, &c.nest("lower", 0), s)?;
			let col_upper = map_schema_column(element_type, &c.nest("upper", 1), s)?;
			let col_lower_incl = create_primitive_copier_simple::<bool, BoolType, _>(&c.nest("lower_inclusive", 2));
			let col_upper_incl = create_primitive_copier_simple::<bool, BoolType, _>(&c.nest("upper_inclusive", 3));
			let col_is_empty = create_primitive_copier_simple::<bool, BoolType, _>(&c.nest("is_empty", 4));

			let schema = ParquetType::group_type_builder(c.col_name())
				.with_fields(&mut vec![
					Arc::new(col_lower.1),
					Arc::new(col_upper.1),
					Arc::new(ParquetType::primitive_type_builder("lower_inclusive", basic::Type::BOOLEAN).build().unwrap()),
					Arc::new(ParquetType::primitive_type_builder("upper_inclusive", basic::Type::BOOLEAN).build().unwrap()),
					Arc::new(ParquetType::primitive_type_builder("is_empty", basic::Type::BOOLEAN).build().unwrap()),
				])
				.with_repetition(c.pq_repetition())
				.build()
				.unwrap();

			let copier = create_complex_appender::<PgRawRange, _>(c, CreateCopierCallback::<TRow>::new(), vec![
				col_lower.0,
				col_upper.0,
				col_lower_incl,
				col_upper_incl,
				col_is_empty,
			]);

			Ok((copier, schema))
		},
		&Kind::Composite(ref fields) => {
			let (mut column_copiers, mut parquet_types) = (vec![], vec![]);
			for (i, f) in fields.into_iter().enumerate() {
				let (c, t) = map_schema_column::<PgRawRecord>(f.type_(), &c.nest(f.name(), i), s)?;
				column_copiers.push(c);
				parquet_types.push(t);
			}

			let schema = ParquetType::group_type_builder(c.col_name())
				.with_fields(&mut parquet_types.into_iter().map(Arc::new).collect())
				.with_repetition(c.pq_repetition())
				.build()
				.unwrap();

			let copier = create_complex_appender::<PgRawRecord, _>(c, CreateCopierCallback::<TRow>::new(), column_copiers);

			Ok((copier, schema))
		}
		_ => Err(format!("Could not map column {}, unsupported type: {}", c.full_name(), t))
	}
}


fn map_simple_type<Callback: AppenderCallback>(
	t: &PgType,
	c: &ColumnInfo,
	s: &SchemaSettings,
	callback: Callback,
) -> Result<(Callback::TResult, ParquetType), String> {
	let name = c.col_name();

	Ok(match t.name() {
		"bool" => resolve_primitive::<bool, BoolType, _>(name, c, callback, None, None),
		"int2" => resolve_primitive::<i16, Int32Type, _>(name, c, callback, None, Some(ConvertedType::INT_16)),
		"int4" => resolve_primitive::<i32, Int32Type, _>(name, c, callback, None, None),
		"oid" => resolve_primitive::<u32, Int32Type, _>(name, c, callback, None, None),
		"int8" => resolve_primitive::<i64, Int64Type, _>(name, c, callback, None, None),
		"float4" => resolve_primitive::<f32, FloatType, _>(name, c, callback, None, None),
		"float8" => resolve_primitive::<f64, DoubleType, _>(name, c, callback, None, None),
		"numeric" => {
			let scale = s.decimal_scale;
			let precision = s.decimal_precision;
			let schema = ParquetType::primitive_type_builder(name, basic::Type::BYTE_ARRAY)
				.with_repetition(c.pq_repetition())
				.with_logical_type(Some(LogicalType::Decimal { scale, precision: precision as i32 }))
				.with_precision(precision as i32)
				.with_scale(scale)
				.build().unwrap();
			let cp = wrap_appender(c, callback, new_decimal_bytes_appender(c.definition_level + 1, c.repetition_level, s.decimal_precision, s.decimal_scale));
			(cp, schema)
		},
		"money" => resolve_primitive::<PgMoney, Int64Type, _>(name, c, callback, Some(LogicalType::Decimal { scale: 2, precision: 18 }), None),
		"char" => resolve_primitive::<i8, Int32Type, _>(name, c, callback, None, Some(ConvertedType::INT_8)),
		"bytea" => resolve_primitive::<Vec<u8>, ByteArrayType, _>(name, c, callback, None, None),
		"name" | "text" | "xml" | "bpchar" | "varchar" =>
			resolve_primitive::<String, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),
		"jsonb" | "json" =>
			resolve_primitive::<PgRawJsonb, ByteArrayType, _>(name, c, callback, None, Some(match s.json_handling {
				SchemaSettingsJsonHandling::String => ConvertedType::UTF8,
				SchemaSettingsJsonHandling::StringMarkedAsJson => ConvertedType::JSON
			})),
		"timestamptz" =>
			resolve_primitive::<chrono::DateTime<chrono::Utc>, Int64Type, _>(name, c, callback, Some(LogicalType::Timestamp { is_adjusted_to_u_t_c: true, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),
		"timestamp" =>
			resolve_primitive::<chrono::NaiveDateTime, Int64Type, _>(name, c, callback, Some(LogicalType::Timestamp { is_adjusted_to_u_t_c: false, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),
		"date" =>
			resolve_primitive::<chrono::NaiveDate, Int32Type, _>(name, c, callback, Some(LogicalType::Date), None),
		"time" =>
			resolve_primitive::<chrono::NaiveTime, Int64Type, _>(name, c, callback, Some(LogicalType::Time { is_adjusted_to_u_t_c: false, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),

		"uuid" =>
			resolve_primitive::<uuid::Uuid, FixedLenByteArrayType, _>(name, c, callback, Some(LogicalType::Uuid), None),


		"macaddr" =>
			match s.macaddr_handling {
				SchemaSettingsMacaddrHandling::String =>
					resolve_primitive::<eui48::MacAddress, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),
				SchemaSettingsMacaddrHandling::ByteArray =>
					resolve_primitive::<eui48::MacAddress, FixedLenByteArrayType, _>(name, c, callback, None, None),
				SchemaSettingsMacaddrHandling::Int64 =>
					resolve_primitive::<eui48::MacAddress, Int64Type, _>(name, c, callback, None, None),
			},
		"inet" =>
			resolve_primitive::<IpAddr, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),
		"bit" | "varbit" =>
			resolve_primitive::<bit_vec::BitVec, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),

		// TODO: Regproc Tid Xid Cid PgNodeTree Point Lseg Path Box Polygon Line Cidr Unknown Circle Macaddr8 Aclitem Bpchar Interval Timetz Refcursor Regprocedure Regoper Regoperator Regclass Regtype TxidSnapshot PgLsn PgNdistinct PgDependencies TsVector Tsquery GtsVector Regconfig Regdictionary Jsonpath Regnamespace Regrole Regcollation PgMcvList PgSnapshot Xid9


		n => 
			return Err(format!("Could not map column {}, unsupported primitive type: {}", c.full_name(), n)),
	})
}

fn resolve_primitive<T: for<'a> FromSql<'a> + 'static, TDataType, Callback: AppenderCallback>(
	name: &str,
	c: &ColumnInfo,
	callback: Callback,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>
) -> (Callback::TResult, ParquetType)
	where TDataType: DataType, TDataType::T : RealMemorySize + MyFrom<T> {
	resolve_primitive_conv::<T, TDataType, _, Callback>(name, c, callback, logical_type, conv_type, |v| MyFrom::my_from(v))
}


fn resolve_primitive_conv<T: for<'a> FromSql<'a> + 'static, TDataType, FConversion: Fn(T) -> TDataType::T + 'static, Callback: AppenderCallback>(
	name: &str,
	c: &ColumnInfo,
	callback: Callback,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>,
	convert: FConversion
) -> (Callback::TResult, ParquetType)
	where TDataType: DataType, TDataType::T : RealMemorySize {
	let mut c = c.clone();
	c.definition_level += 1; // TODO: can we support NOT NULL fields?
	let mut t =
		ParquetType::primitive_type_builder(name, TDataType::get_physical_type())
		.with_repetition(c.pq_repetition())
		.with_converted_type(conv_type.unwrap_or(ConvertedType::NONE));

	match &logical_type {
		Some(LogicalType::Decimal { scale, precision }) => {
			t = t.with_precision(*precision).with_scale(*scale);
		},
		_ => {}
	};
	
	let t = t.with_logical_type(logical_type).build().unwrap();

	let cp =
		create_primitive_appender::<T, TDataType, _, _>(&c, callback, convert);

	(cp, t)
}
fn create_primitive_copier_simple<T: for <'a> FromSql<'a> + 'static, TDataType, TRow: PgAbstractRow>(
	c: &ColumnInfo,
) -> DynCopier<TRow>
	where TDataType: DataType, TDataType::T: RealMemorySize + MyFrom<T> {
	let mut c = c.clone();
	c.definition_level += 1;
	create_primitive_appender::<T, TDataType, _, _>(&c, CreateCopierCallback::<TRow>::new(), |x| TDataType::T::my_from(x))
}

fn create_primitive_appender<T: for <'a> FromSql<'a> + 'static, TDataType, FConversion: Fn(T) -> TDataType::T + 'static, Callback: AppenderCallback>(
	c: &ColumnInfo,
	callback: Callback,
	convert: FConversion
) -> Callback::TResult
	where TDataType: DataType, TDataType::T: RealMemorySize {
	let basic_appender: GenericColumnAppender<T, TDataType, _> = GenericColumnAppender::new(c.definition_level, c.repetition_level, convert);
	wrap_appender(c, callback, basic_appender)
}

fn create_complex_appender<T: for <'a> FromSql<'a> + 'static, Callback: AppenderCallback>(c: &ColumnInfo, callback: Callback, copiers: Vec<DynCopier<T>>) -> Callback::TResult {
	let main_cp = MergedColumnCopier::new(copiers, c.definition_level + 1, c.repetition_level);
	wrap_appender(c, callback, main_cp)
}

fn wrap_appender<T: for <'a> FromSql<'a> + 'static, Callback: AppenderCallback>(c: &ColumnInfo, callback: Callback, appender: impl ColumnAppender<T> + 'static) -> Callback::TResult {
	if c.is_array {
		callback.f::<Vec<Option<T>>, _>(c, ArrayColumnAppender::new(appender))
	} else {
		callback.f(c, appender)
	}
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
	fn pq_repetition(&self) -> Repetition {
		if self.is_array {
			Repetition::REPEATED
		} else {
			Repetition::OPTIONAL
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

trait AppenderCallback {
	type TResult;
	fn f<TPg, TAppender>(&self, c: &ColumnInfo, appender: TAppender) -> Self::TResult
		where TAppender: ColumnAppender<TPg> + 'static, TPg: for <'a> FromSql<'a> + 'static;
}

struct CreateCopierCallback<TRow> { _dummy: PhantomData<TRow> }
impl<TRow> CreateCopierCallback<TRow> { fn new() -> Self { CreateCopierCallback{_dummy: PhantomData} } }
impl<TRow: PgAbstractRow> AppenderCallback for CreateCopierCallback<TRow> {
	type TResult = DynCopier<TRow>;
	fn f<TPg, TAppender>(&self, c: &ColumnInfo, appender: TAppender) -> DynCopier<TRow>
		where TAppender: ColumnAppender<TPg> + 'static, TPg: for <'a> FromSql<'a> + 'static {
		Box::new(BasicPgColumnCopier::new(c.col_i, appender))
	}
}
// struct CreateRangeCpCallback<TInner: AppenderCallback> { inner: TInner }
// impl<TInner: AppenderCallback> AppenderCallback for CreateRangeCpCallback<TInner> {
// 	type TResult = TInner::TResult;
// 	fn f<TPg, TAppender>(&self, c: &ColumnInfo, appender: TAppender) -> TInner::TResult
// 		where TAppender: ColumnAppender<TPg> + 'static, TPg: for <'a> FromSql<'a> + 'static {

// 		let range_appender = RangeColumnAppender::new(appender);
// 		self.inner.f::<PgRawRange<TPg>, _>(c, range_appender)
// 	}
// }

struct NopCallback { }
impl AppenderCallback for NopCallback {
	type TResult = ();
	fn f<TPg, TAppender>(&self, _: &ColumnInfo, _: TAppender) -> ()
		where TAppender: ColumnAppender<TPg> + 'static, TPg: for <'a> FromSql<'a> + 'static {
		()
	}
}

