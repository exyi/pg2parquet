use parquet;
use parquet::basic::{LogicalType, ConvertedType};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{DataType, BoolType};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::column::reader::ColumnReader;
use parquet::schema::types::ColumnDescriptor;
use std::fmt::{Display, Debug, Formatter};

fn print_col_info<T: DataType<T = T2>, T2: Default + Clone + ParquetTypeFormat>(col_name: &str, col: &ColumnDescriptor, reader: &mut ColumnReaderImpl<T>) {
	let batch_size = 300;
	let mut data: Vec<T2> = vec![<T as DataType>::T::default(); batch_size];
	let mut dls = vec![0; batch_size];
	let mut rls = vec![0; batch_size];
	let (valuecount, totalcount) = reader.read_batch(batch_size, Some(&mut dls), Some(&mut rls), &mut data).unwrap();

	let data_display = DisplayDataRow { vec: data[0..valuecount].to_vec(), lt: col.logical_type(), ct: col.converted_type() };
	println!("{}: {:?} {}", col_name, (valuecount, totalcount), data_display);

	if dls.iter().any(|x| *x != 0) {
		println!("dls: {:?}", dls[0..totalcount].to_vec());
	}
	if rls.iter().any(|x| *x != 0) {
		println!("rls: {:?}", rls[0..totalcount].to_vec());
	}
}

#[cfg(not(debug_assertions))]
pub fn print_parquet_info(path: &std::path::PathBuf) {
	println!("Disabled in release build")
}

#[cfg(debug_assertions)]
pub fn print_parquet_info(path: &std::path::PathBuf) {
	let file = std::fs::File::open(path).unwrap();
	let reader = SerializedFileReader::new(file).unwrap();
	let meta = reader.metadata();
	let schema = meta.file_metadata().schema_descr();

	for row_group_i in 0..reader.num_row_groups() {
		let rg = reader.get_row_group(row_group_i).unwrap();
		for column_i in 0..rg.num_columns() {

			let column = rg.get_column_reader(column_i).unwrap();
			let column_meta = rg.metadata().columns()[column_i].clone();
			let column_type = schema.column(column_i);
			let name = column_meta.column_path().string();
			println!("column: {} {:?}", name, column_meta);

			match column {
				ColumnReader::BoolColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
				ColumnReader::Int32ColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
				ColumnReader::Int64ColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
				ColumnReader::Int96ColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
				ColumnReader::FloatColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
				ColumnReader::DoubleColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
				ColumnReader::ByteArrayColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
				ColumnReader::FixedLenByteArrayColumnReader(mut c) => print_col_info(&name, &column_type, &mut c),
			}
		}
	}
}

trait ParquetTypeFormat {
	fn show(&self, _lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result;
}

impl ParquetTypeFormat for bool {
    fn show(&self, _lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
impl ParquetTypeFormat for i32 {
	fn show(&self, _lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self, f)
	}
}
impl ParquetTypeFormat for i64 {
	fn show(&self, _lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self, f)
	}
}
impl ParquetTypeFormat for parquet::data_type::Int96 {
	fn show(&self, _lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self, f)
	}
}
impl ParquetTypeFormat for f32 {
	fn show(&self, _lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self, f)
	}
}
impl ParquetTypeFormat for f64 {
	fn show(&self, _lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self, f)
	}
}
impl ParquetTypeFormat for parquet::data_type::ByteArray {
	fn show(&self, lt: &Option<LogicalType>, ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
		match lt {
			Some(LogicalType::String) | Some(LogicalType::Json) | Some(LogicalType::Enum) => {
				let s = std::str::from_utf8(self.data()).unwrap();
				f.write_str(&s)
			},
			_ => {
				match ct {
					ConvertedType::UTF8 | ConvertedType::ENUM | ConvertedType::JSON => {
						let s = std::str::from_utf8(self.data()).unwrap();
						f.write_str(&s)
					},
					_ => {
						write!(f, "{:x?}", self.data())
					}
				}
			}
		}
	}
}
impl ParquetTypeFormat for parquet::data_type::FixedLenByteArray {
	fn show(&self, lt: &Option<LogicalType>, _ct: &ConvertedType, f: &mut Formatter<'_>) -> std::fmt::Result {
		match lt {
			_ => {
				write!(f, "{:x?}", self.data())
			}
		}
	}
}

struct DisplayDataRow<T: ParquetTypeFormat> {
	pub vec: Vec<T>,
	pub lt: Option<LogicalType>,
	pub ct: ConvertedType,
}
impl<T: ParquetTypeFormat> Display for DisplayDataRow<T> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "[")?;
		for (i, v) in self.vec.iter().enumerate() {
			if i != 0 {
				write!(f, ", ")?;
			}
			v.show(&self.lt, &self.ct, f)?;
		}
		write!(f, "]")
	}
}
