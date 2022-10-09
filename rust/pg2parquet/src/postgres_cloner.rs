use std::path::PathBuf;
use std::sync::Arc;

use clap::error::Error;
use parquet::basic::{Repetition, self, ConvertedType, LogicalType};
use parquet::data_type::{DataType, BoolType, Int32Type, Int64Type, FloatType, DoubleType, ByteArray, ByteArrayType, FixedLenByteArrayType, FixedLenByteArray};
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::TimestampType;
use postgres::types::{Kind, Type as PgType, FromSql};
use postgres::{self, Client, NoTls, RowIter, Row, Column, Statement};
use postgres::fallible_iterator::FallibleIterator;
use parquet::schema::types::{Type as ParquetType, TypePtr};

use crate::column_appender::{ColumnAppender, GenericColumnAppender, ArrayColumnAppender, RealMemorySize};
use crate::column_pg_copier::{ColumnCopier, BasicPgColumnCopier, MergedColumnCopier};
use crate::myfrom::MyFrom;
use crate::parquet_row_writer::{WriterStats, ParquetRowWriter, ParquetRowWriterImpl, WriterSettings};

type DynCopier = Box<dyn ColumnCopier<Row>>;
type ResolvedColumn = (DynCopier, ParquetType);


pub fn execute_copy(query: &str, output_file: &PathBuf, output_props: WriterPropertiesPtr) -> Result<WriterStats, String> {
	let mut pg_config = postgres::Config::new();
	pg_config.dbname("xx")
		.host("127.0.0.1")
		.port(5439)
		.user("postgres")
		.password("postgres");
	let mut client = pg_config.connect(NoTls)
		.map_err(|e| format!("DB connection failed: {}", e.to_string()))?;

	let statement = client.prepare(query).unwrap();

	let (copier, schema) = map_schema_root(statement.columns());
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

fn map_schema_root(row: &[Column]) -> ResolvedColumn {
	let fields: Vec<ResolvedColumn> = row.iter().enumerate().map(|(col_i, c)| {

		let t = c.type_();

		map_schema_column(t, c.name(), &ColumnInfo { col_i, is_array: false, definition_level: 0, repetition_level: 0 })
	}).collect();


	let (column_copiers, parquet_types): (Vec<_>, Vec<_>) = fields.into_iter().unzip();

	let merged_copier: Box<dyn ColumnCopier<Row>> = Box::new(MergedColumnCopier::<Row>::new(column_copiers));
	let struct_type = ParquetType::group_type_builder("root")
		.with_fields(&mut parquet_types.into_iter().map(Arc::new).collect())
		.build()
		.unwrap();

	(merged_copier, struct_type)
}

fn map_schema_column(
	t: &PgType,
	name: &str,
	c: &ColumnInfo
) -> ResolvedColumn {
	match t.kind() {
		Kind::Simple =>
			map_simple_type(t, name, c),
		Kind::Array(element_type) => {
			let mut cc = c.clone();
			cc.is_array = true;
			cc.repetition_level += 1;
			map_schema_column(element_type, name, &cc)
		},
		Kind::Domain(element_type) => {
			map_schema_column(element_type, name, c)
		},
		_ => panic!("Unsupported type: {:?}", t),
	}
}


fn map_simple_type(
	t: &PgType,
	name: &str,
	c: &ColumnInfo
) -> ResolvedColumn {

	match t.name() {
		"bool" => resolve_primitive::<bool, BoolType>(name, c, None, None),
		"int2" => resolve_primitive::<i16, Int32Type>(name, c, None, Some(ConvertedType::INT_16)),
		"int4" => resolve_primitive::<i32, Int32Type>(name, c, None, None),
		"oid" => resolve_primitive::<u32, Int32Type>(name, c, None, None),
		"int8" => resolve_primitive::<i64, Int64Type>(name, c, None, None),
		"float4" => resolve_primitive::<f32, FloatType>(name, c, None, None),
		"float8" => resolve_primitive::<f64, DoubleType>(name, c, None, None),
		"numeric" => todo!(),
		"money" => todo!(),
		"char" => resolve_primitive::<i8, Int32Type>(name, c, None, Some(ConvertedType::INT_8)),
		"bytea" => resolve_primitive::<Vec<u8>, ByteArrayType>(name, c, None, None),
		"name" | "text" | "json" | "xml" | "bpchar" | "varchar" =>
			resolve_primitive::<String, ByteArrayType>(name, c, None, Some(ConvertedType::UTF8)),
		"timestamptz" =>
			resolve_primitive::<chrono::DateTime<chrono::Utc>, Int64Type>(name, c, Some(LogicalType::Timestamp { is_adjusted_to_u_t_c: true, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),
		"timestamp" =>
			resolve_primitive::<chrono::NaiveDateTime, Int64Type>(name, c, Some(LogicalType::Timestamp { is_adjusted_to_u_t_c: false, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),
		"date" =>
			resolve_primitive::<chrono::NaiveDate, Int32Type>(name, c, Some(LogicalType::Date), None),
		"time" =>
			resolve_primitive::<chrono::NaiveTime, Int64Type>(name, c, Some(LogicalType::Time { is_adjusted_to_u_t_c: false, unit: parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds {  }) }), None),

		"uuid" =>
			resolve_primitive::<uuid::Uuid, FixedLenByteArrayType>(name, c, Some(LogicalType::Uuid), None),


		// "macaddr"

		// TODO: Regproc Oid Tid Xid Cid PgNodeTree Point Lseg Path Box Polygon Line Cidr Float4 Float8 Unknown Circle Macaddr8 Money Macaddr Inet Aclitem Bpchar Interval Timetz Bit Varbit Numeric Refcursor Regprocedure Regoper Regoperator Regclass Regtype Uuid TxidSnapshot PgLsn PgNdistinct PgDependencies TsVector Tsquery GtsVector Regconfig Regdictionary Jsonb Jsonpath Regnamespace Regrole Regcollation PgMcvList PgSnapshot Xid9


		n => panic!("Unsupported primitive type: {}", n),
	}
}

fn resolve_primitive<T: for<'a> FromSql<'a> + 'static, TDataType>(
	name: &str,
	c: &ColumnInfo,
	logical_type: Option<LogicalType>,
	conv_type: Option<ConvertedType>
) -> ResolvedColumn
	where TDataType: DataType, TDataType::T : RealMemorySize + MyFrom<T> {
	let mut c = c.clone();
	c.definition_level += 1; // TODO: can we support NOT NULL fields?
	let t =
		ParquetType::primitive_type_builder(name, TDataType::get_physical_type())
		.with_repetition(if c.is_array { Repetition::REPEATED } else { Repetition::OPTIONAL })
		.with_converted_type(conv_type.unwrap_or(ConvertedType::NONE))
		.with_logical_type(logical_type)
		.build().unwrap();

	let cp = create_primitive_appender::<T, TDataType>(&c);


	(cp, t)
}

fn create_primitive_appender<T: for <'a> FromSql<'a> + 'static, TDataType>(
	c: &ColumnInfo
) -> DynCopier
	where TDataType: DataType, TDataType::T: RealMemorySize + MyFrom<T> {
	let basic_appender: GenericColumnAppender<T, TDataType, _> = GenericColumnAppender::new(c.definition_level, c.repetition_level, |x| TDataType::T::my_from(x));
	if c.is_array {
		Box::new(BasicPgColumnCopier::<Vec<T>, _>::new(c.col_i, ArrayColumnAppender::new(basic_appender)))
	} else {
		Box::new(BasicPgColumnCopier::new(c.col_i, basic_appender))
	}
}

#[derive(Debug, Clone)]
struct ColumnInfo {
	pub col_i: usize,
	pub is_array: bool,
	pub definition_level: i16,
	pub repetition_level: i16,
}

