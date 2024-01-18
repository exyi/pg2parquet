use parquet::data_type::{Int32Type, Int64Type, ByteArrayType};
use parquet::file::{writer::SerializedFileWriter, properties::WriterProperties};
use parquet::schema::types::Type;
use parquet::basic::{Encoding, Compression, self, Repetition, ConvertedType};
use parquet::format::{self, StringType};
use std::mem;
use std::{fs, sync::Arc};

#[cfg(not(debug_assertions))]
pub fn create_something(_file_path: &std::path::PathBuf) {
	println!("Disabled in release build")
}

#[cfg(debug_assertions)]
pub fn create_something(file_path: &std::path::PathBuf) {
	let file = fs::File::create(&file_path).unwrap();
	let props =
		WriterProperties::builder()
			.set_compression(Compression::SNAPPY)
		.build().arc();

	let schema =
		Type::group_type_builder("root")
			.with_fields(&mut vec![
				Type::primitive_type_builder("id", basic::Type::INT32).build().unwrap().arc(),

				Type::group_type_builder("nested")
					.with_fields(&mut vec![
						Type::primitive_type_builder("x", basic::Type::INT32)
							.with_repetition(Repetition::REPEATED).build().unwrap().arc(),
						Type::primitive_type_builder("y", basic::Type::INT64)
							.with_converted_type(ConvertedType::TIMESTAMP_MICROS)
							.build().unwrap().arc(),
					])
					.with_repetition(Repetition::OPTIONAL)
					.build().unwrap().arc(),

			])
			.build().unwrap().arc();

	let mut writer =
		SerializedFileWriter::new(file, schema, props).unwrap();

	let mut rg = writer.next_row_group().unwrap();

	let mut col_id = rg.next_column().unwrap().unwrap();
	col_id.typed::<Int32Type>().write_batch(
		&vec![1, 0, 2],
		Some(&vec![1, 0, 1]),
		None
	).unwrap();
	col_id.close().unwrap();

	let mut col_nested_x = rg.next_column().unwrap().unwrap();
	col_nested_x.typed::<Int32Type>().write_batch(
		&vec![     1, 2, 3, 4,       10, 0,     0],
		Some(&vec![2, 2, 2, 2,        2, 1,     0]),
		Some(&vec![0, 1, 1, 1,        0, 1,     0])
	).unwrap();
	col_nested_x.close().unwrap();

	let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
	let mut y = rg.next_column().unwrap().unwrap();
	y.typed::<Int64Type>().write_batch(
		&vec![     now_ms,   0,     0],
		Some(&vec![2,        1,     0]),
		None
	).unwrap();
	y.close().unwrap();
	rg.close().unwrap();
	writer.close().unwrap();

	// todo!()
}


trait IntoArc<T> {
	fn arc(self) -> Arc<T>;
}
impl <T> IntoArc<T> for T {
	fn arc(self) -> Arc<T> {
		Arc::new(self)
	}
}
