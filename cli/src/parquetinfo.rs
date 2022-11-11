use parquet;
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::DataType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::column::reader::ColumnReader;

fn print_col_info<T: DataType<T = T2>, T2: Default + Clone + std::fmt::Debug>(col_name: &str, col: &mut ColumnReaderImpl<T>) {
	let batch_size = 300;
	let mut data: Vec<T2> = vec![<T as DataType>::T::default(); batch_size];
	let mut dls = vec![0; batch_size];
	let mut rls = vec![0; batch_size];
	let (valuecount, totalcount) = col.read_batch(batch_size, Some(&mut dls), Some(&mut rls), &mut data).unwrap();

	println!("{}: {:?} {:?}", col_name, (valuecount, totalcount), data[0..valuecount].to_vec());

	if dls.iter().any(|x| *x != 0) {
		println!("dls: {:?}", dls[0..valuecount].to_vec());
	}
	if rls.iter().any(|x| *x != 0) {
		println!("rls: {:?}", rls[0..valuecount].to_vec());
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
	// let meta = reader.metadata();
	for row_group_i in 0..reader.num_row_groups() {
		let rg = reader.get_row_group(row_group_i).unwrap();
		for column_i in 0..rg.num_columns() {

			let column = rg.get_column_reader(column_i).unwrap();
			let column_meta = rg.metadata().columns()[column_i].clone();
			let name = column_meta.column_path().string();
			println!("column: {} {:?}", name, column_meta);

			match column {
				ColumnReader::BoolColumnReader(mut c) => print_col_info(&name, &mut c),
				ColumnReader::Int32ColumnReader(mut c) =>print_col_info(&name, &mut c),
				ColumnReader::Int64ColumnReader(mut c) => print_col_info(&name, &mut c),
				ColumnReader::Int96ColumnReader(mut c) => print_col_info(&name, &mut c),
				ColumnReader::FloatColumnReader(mut c) => print_col_info(&name, &mut c),
				ColumnReader::DoubleColumnReader(mut c) => print_col_info(&name, &mut c),
				ColumnReader::ByteArrayColumnReader(mut c) => print_col_info(&name, &mut c),
				ColumnReader::FixedLenByteArrayColumnReader(mut c) => print_col_info(&name, &mut c),
			}
		}
	}
}
