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

use crate::column_appender::{ColumnAppender, GenericColumnAppender, ArrayColumnAppender, RealMemorySize};
use crate::column_pg_copier::{ColumnCopier, BasicPgColumnCopier, MergedColumnCopier};
use crate::myfrom::MyFrom;
use crate::parquet_row_writer::{WriterStats, ParquetRowWriter, ParquetRowWriterImpl, WriterSettings};
use crate::pg_custom_types::{PgEnum, PgRawRange, PgAbstractRow};
use crate::range_col_appender::RangeColumnAppender;

type DynCopier<TRow> = Box<dyn ColumnCopier<TRow>>;
type DynRowCopier = DynCopier<Row>;
type ResolvedColumn<TRow> = (DynCopier<TRow>, ParquetType);

#[derive(Clone, Debug)]
pub struct SchemaSettings {
	macaddr_handling: SchemaSettingsMacaddrHandling
}

#[derive(Clone, Copy, Debug)]
pub enum SchemaSettingsMacaddrHandling {
	AsString,
	AsByteArray,
	AsInt64
}

pub fn default_settings() -> SchemaSettings {
	SchemaSettings {
		macaddr_handling: SchemaSettingsMacaddrHandling::AsString
	}
}


pub fn execute_copy(query: &str, output_file: &PathBuf, output_props: WriterPropertiesPtr, schema_settings: &SchemaSettings) -> Result<WriterStats, String> {
	let mut pg_config = postgres::Config::new();
	pg_config.dbname("xx")
		.host("127.0.0.1")
		.port(5439)
		.user("postgres")
		.password("postgres");
	let mut client = pg_config.connect(NoTls)
		.map_err(|e| format!("DB connection failed: {}", e.to_string()))?;

	let statement = client.prepare(query).map_err(|db_err| { db_err.to_string() })?;

	let (copier, schema) = map_schema_root(statement.columns(), schema_settings);
	eprintln!("Schema: {}", format_schema(&schema, 0));
	let schema = Arc::new(schema);

	let settings = WriterSettings { row_group_byte_limit: 500 * 1024 * 1024, row_group_row_limit: 1000000 };

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


fn map_schema_root(row: &[Column], s: &SchemaSettings) -> ResolvedColumn<Row> {
	let mut col_i = 0;
	let fields: Vec<ResolvedColumn<Row>> = row.iter().map(|c| {

		let t = c.type_();

		let schema = map_schema_column(t, c.name(), &ColumnInfo { col_i, is_array: false, definition_level: 0, repetition_level: 0 }, s);
		col_i += 1; //count_columns(&schema.1);
		schema
	}).collect();


	let (column_copiers, parquet_types): (Vec<_>, Vec<_>) = fields.into_iter().unzip();

	let merged_copier: Box<dyn ColumnCopier<Row>> = Box::new(MergedColumnCopier::<Row>::new(column_copiers, 0, 0));
	let struct_type = ParquetType::group_type_builder("root")
		.with_fields(&mut parquet_types.into_iter().map(Arc::new).collect())
		.build()
		.unwrap();

	(merged_copier, struct_type)
}

fn map_schema_column<TRow: PgAbstractRow>(
	t: &PgType,
	name: &str,
	c: &ColumnInfo,
	s: &SchemaSettings
) -> ResolvedColumn<TRow> {
	match t.kind() {
		Kind::Simple =>
			map_simple_type(t, name, c, s, CreateCopierCallback::new()),
		Kind::Enum(ref _enum_data) =>
			resolve_primitive::<PgEnum, ByteArrayType, _>(name, c, CreateCopierCallback::new(), Some(LogicalType::Enum), None),
			// resolve_primitive::<PgEnum, Int32Type>(name, c, None, None),
		Kind::Array(ref element_type) => {
			let mut cc = c.clone();
			cc.is_array = true;
			cc.repetition_level += 1;
			map_schema_column(element_type, name, &cc, s)
		},
		Kind::Domain(ref element_type) => {
			map_schema_column(element_type, name, c, s)
		},
		&Kind::Range(ref element_type) => {
			let mut cc = c.clone();
			cc.is_array = false;
			cc.col_i = 0;
			cc.definition_level += 1;
			// cc.repetition_level += c.is_array as i16;
			let col_lower = map_schema_column(element_type, "lower", &cc, s);
			cc.col_i += 1;
			let col_upper = map_schema_column(element_type, "upper", &cc, s);
			cc.col_i += 1;
			let col_lower_incl = create_primitive_copier_simple::<bool, BoolType, _>(&cc);
			cc.col_i += 1;
			let col_upper_incl = create_primitive_copier_simple::<bool, BoolType, _>(&cc);
			cc.col_i += 1;
			let col_is_empty = create_primitive_copier_simple::<bool, BoolType, _>(&cc);

			let schema = ParquetType::group_type_builder(name)
				.with_fields(&mut vec![
					Arc::new(col_lower.1),
					Arc::new(col_upper.1),
					Arc::new(ParquetType::primitive_type_builder("lower_inclusive", basic::Type::BOOLEAN).build().unwrap()),
					Arc::new(ParquetType::primitive_type_builder("upper_inclusive", basic::Type::BOOLEAN).build().unwrap()),
					Arc::new(ParquetType::primitive_type_builder("is_empty", basic::Type::BOOLEAN).build().unwrap()),
				])
				.with_repetition(if c.is_array { Repetition::REPEATED } else { Repetition::OPTIONAL })
				.build()
				.unwrap();

			let copier = create_complex_appender::<PgRawRange, _>(c, CreateCopierCallback::<TRow>::new(), vec![
				col_lower.0,
				col_upper.0,
				col_lower_incl,
				col_upper_incl,
				col_is_empty,
			]);

			(copier, schema)
		}
		_ => panic!("Unsupported type: {:?}", t),
	}
}


fn map_simple_type<Callback: AppenderCallback>(
	t: &PgType,
	name: &str,
	c: &ColumnInfo,
	s: &SchemaSettings,
	callback: Callback,
) -> (Callback::TResult, ParquetType) {

	match t.name() {
		"bool" => resolve_primitive::<bool, BoolType, _>(name, c, callback, None, None),
		"int2" => resolve_primitive::<i16, Int32Type, _>(name, c, callback, None, Some(ConvertedType::INT_16)),
		"int4" => resolve_primitive::<i32, Int32Type, _>(name, c, callback, None, None),
		"oid" => resolve_primitive::<u32, Int32Type, _>(name, c, callback, None, None),
		"int8" => resolve_primitive::<i64, Int64Type, _>(name, c, callback, None, None),
		"float4" => resolve_primitive::<f32, FloatType, _>(name, c, callback, None, None),
		"float8" => resolve_primitive::<f64, DoubleType, _>(name, c, callback, None, None),
		"numeric" => todo!(),
		"money" => todo!(),
		"char" => resolve_primitive::<i8, Int32Type, _>(name, c, callback, None, Some(ConvertedType::INT_8)),
		"bytea" => resolve_primitive::<Vec<u8>, ByteArrayType, _>(name, c, callback, None, None),
		"name" | "text" | "json" | "xml" | "bpchar" | "varchar" =>
			resolve_primitive::<String, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),
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
				SchemaSettingsMacaddrHandling::AsString =>
					resolve_primitive::<eui48::MacAddress, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),
				SchemaSettingsMacaddrHandling::AsByteArray =>
					resolve_primitive::<eui48::MacAddress, FixedLenByteArrayType, _>(name, c, callback, None, None),
				SchemaSettingsMacaddrHandling::AsInt64 =>
					resolve_primitive::<eui48::MacAddress, Int64Type, _>(name, c, callback, None, None),
			},
		"inet" =>
			resolve_primitive::<IpAddr, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),
		"bit" | "varbit" =>
			resolve_primitive::<bit_vec::BitVec, ByteArrayType, _>(name, c, callback, None, Some(ConvertedType::UTF8)),

		// TODO: Regproc Oid Tid Xid Cid PgNodeTree Point Lseg Path Box Polygon Line Cidr Unknown Circle Macaddr8 Money Aclitem Bpchar Interval Timetz Numeric Refcursor Regprocedure Regoper Regoperator Regclass Regtype TxidSnapshot PgLsn PgNdistinct PgDependencies TsVector Tsquery GtsVector Regconfig Regdictionary Jsonb Jsonpath Regnamespace Regrole Regcollation PgMcvList PgSnapshot Xid9


		n => panic!("Unsupported primitive type: {}", n),
	}
}

fn resolve_primitive<T: for<'a> FromSql<'a> + 'static, TDataType, Callback: AppenderCallback>(
	name: &str,
	c: &ColumnInfo,
	callback: Callback,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>
) -> (Callback::TResult, ParquetType)
	where TDataType: DataType, TDataType::T : RealMemorySize + MyFrom<T> {
	let mut c = c.clone();
	c.definition_level += 1; // TODO: can we support NOT NULL fields?
	let t =
		ParquetType::primitive_type_builder(name, TDataType::get_physical_type())
		.with_repetition(if c.is_array { Repetition::REPEATED } else { Repetition::OPTIONAL })
		.with_converted_type(conv_type.unwrap_or(ConvertedType::NONE))
		.with_logical_type(logical_type)
		.build().unwrap();

	let cp =
		create_primitive_appender::<T, TDataType, _, _>(&c, callback, |x| TDataType::T::my_from(x));

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
	where TDataType: DataType, TDataType::T: RealMemorySize + MyFrom<T> {
	let basic_appender: GenericColumnAppender<T, TDataType, _> = GenericColumnAppender::new(c.definition_level, c.repetition_level, convert);
	if c.is_array {
		callback.f::<Vec<Option<T>>, _>(c, ArrayColumnAppender::new(basic_appender))
	} else {
		callback.f(c, basic_appender)
	}
}

fn create_complex_appender<T: for <'a> FromSql<'a> + 'static, Callback: AppenderCallback>(c: &ColumnInfo, callback: Callback, copiers: Vec<DynCopier<T>>) -> Callback::TResult {
	let main_cp = MergedColumnCopier::new(copiers, c.definition_level + 1, c.repetition_level);
	if c.is_array {
		callback.f::<Vec<Option<T>>, _>(c, ArrayColumnAppender::new(main_cp))
	} else {
		callback.f(c, main_cp)
	}
}

#[derive(Debug, Clone)]
struct ColumnInfo {
	pub col_i: usize,
	pub is_array: bool,
	pub definition_level: i16,
	pub repetition_level: i16,
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

