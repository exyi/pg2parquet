use arrow::array::{Array, BinaryArray, Int32Array, ListArray, StructArray};
use arrow::datatypes::{DataType, Field, UInt32Type};
use parquet::data_type::Int32Type;
use super::*;
use super::dictionary::DictionaryColumnAppender;

#[test]
fn nested_nulls_empty_lists_and_repeated_batches() {
	let values = ArrayColumnAppender::new(new_autoconv_generic_appender::<i32, Int32Type>(), true, true);
	let mut appender = DynamicMergedAppender::new(
		vec![Box::new(values) as DynColumnAppender<Vec<Option<i32>>>],
		vec![("values".into(), true)],
	);
	for _ in 0..2 {
		appender.copy_value(&vec![Some(7), None]).unwrap();
		appender.write_null().unwrap();
		appender.copy_value(&vec![]).unwrap();
		let array = appender.to_arrow_array().unwrap();
		let rows = array.as_any().downcast_ref::<StructArray>().unwrap();
		assert_eq!(rows.len(), 3);
		assert!(rows.is_null(1));
		assert_eq!(rows.fields()[0].name(), "values");
		let lists = rows.column(0).as_any().downcast_ref::<ListArray>().unwrap();
		assert_eq!(lists.value_offsets(), &[0, 2, 2, 2]);
		assert!(lists.is_null(1));
		assert!(lists.is_valid(2));
		let values = lists.values().as_any().downcast_ref::<Int32Array>().unwrap();
		assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(7), None]);
	}
	assert_eq!(appender.to_arrow_array().unwrap().len(), 0);
}

#[test]
fn static_struct_fields_and_required_children_survive_null_parents() {
	let mut appender = new_static_merged_appender::<i32>()
		.add_appender("key", false, new_autoconv_generic_appender::<i32, Int32Type>())
		.add_appender("value", true, PrimitiveColumnAppender::<_, Int32Type, _>::new(|v: &i32| v * 2));
	for _ in 0..2 {
		appender.write_null().unwrap();
		appender.copy_value(&3).unwrap();
		let array = appender.to_arrow_array().unwrap();
		let rows = array.as_any().downcast_ref::<StructArray>().unwrap();
		assert_eq!(rows.len(), 2);
		assert!(rows.is_null(0));
		assert_eq!(rows.fields()[0].as_ref(), &Field::new("key", DataType::Int32, false));
		assert_eq!(rows.fields()[1].as_ref(), &Field::new("value", DataType::Int32, true));
		assert_eq!(rows.column(1).as_any().downcast_ref::<Int32Array>().unwrap().value(1), 6);
	}
}

#[test]
fn list_of_structs_preserves_map_element_metadata() {
	let inner = new_static_merged_appender::<i32>()
		.add_appender("key", false, new_autoconv_generic_appender::<i32, Int32Type>());
	let mut appender = ArrayColumnAppender::new(inner, true, false).with_element_name("key_value");
	appender.copy_value_opt(&None::<Vec<i32>>).unwrap();
	appender.copy_value(&vec![1, 2]).unwrap();
	let array = appender.to_arrow_array().unwrap();
	let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
	assert_eq!(lists.value_offsets(), &[0, 0, 2]);
	assert!(lists.is_null(0));
	let DataType::List(field) = lists.data_type() else { panic!() };
	assert_eq!(field.name(), "key_value");
	assert!(!field.is_nullable());
}

#[test]
fn byte_conversion_nulls_append_rows_and_reset() {
	let mut appender = ByteArrayColumnAppender::new(Vec::as_slice);
	for _ in 0..2 {
		assert_eq!(appender.copy_value_opt(&Some(vec![1, 2])).unwrap(), 6);
		assert_eq!(appender.copy_value_opt(&None).unwrap(), 4);
		assert_eq!(appender.write_null().unwrap(), 4);
		let array = appender.to_arrow_array().unwrap();
		let bytes = array.as_any().downcast_ref::<BinaryArray>().unwrap();
		assert_eq!(bytes.len(), 3);
		assert_eq!(bytes.value(0), &[1, 2]);
		assert!(bytes.is_null(1));
		assert!(bytes.is_null(2));
	}
}

#[test]
fn nulls_account_for_arrow_value_entries() {
	let mut ints = new_autoconv_generic_appender::<i32, Int32Type>();
	assert_eq!(ints.copy_value(&7).unwrap(), 4);
	assert_eq!(ints.write_null().unwrap(), 4);

	let mut binary = ByteArrayColumnAppender::new(|value: &Vec<u8>| value.as_slice());
	assert_eq!(binary.copy_value(&vec![1, 2]).unwrap(), 6);
	assert_eq!(binary.write_null().unwrap(), 4);

	let mut lists = ArrayColumnAppender::new(
		new_autoconv_generic_appender::<i32, Int32Type>(),
		true,
		true,
	);
	assert_eq!(lists.copy_value(&Vec::<Option<i32>>::new()).unwrap(), 4);
	assert_eq!(lists.write_null().unwrap(), 4);
}

#[test]
fn empty_struct_retains_row_count() {
	let mut appender = DynamicMergedAppender::<i32>::new(vec![], vec![]);
	appender.copy_value(&1).unwrap();
	appender.write_null().unwrap();
	let array = appender.to_arrow_array().unwrap();
	assert_eq!(array.len(), 2);
	assert!(array.is_valid(0));
	assert!(array.is_null(1));
}

#[test]
fn appenders_accept_non_clone_inputs() {
	struct NonClone(Vec<u8>);

	let inner = ByteArrayColumnAppender::new(|value: &Vec<u8>| value.as_slice());
	let mut appender = inner.preprocess_ref(|value: &NonClone| &value.0);
	appender.copy_value(&NonClone(vec![1, 2, 3])).unwrap();

	let array = appender.to_arrow_array().unwrap();
	let values = array.as_any().downcast_ref::<BinaryArray>().unwrap();
	assert_eq!(values.value(0), &[1, 2, 3]);
}

#[test]
fn dictionary_appender_deduplicates_values_and_resets() {
	let mut appender = DictionaryColumnAppender::new(String::as_bytes);
	for _ in 0..2 {
		appender.write_null().unwrap();
		assert_eq!(appender.copy_value(&"monday".to_string()).unwrap(), 14);
		assert_eq!(appender.copy_value(&"monday".to_string()).unwrap(), 4);
		assert_eq!(appender.copy_value(&"tuesday".to_string()).unwrap(), 15);

		let array = appender.to_arrow_array().unwrap();
		let dictionary = array.as_any()
			.downcast_ref::<arrow::array::DictionaryArray<UInt32Type>>()
			.unwrap();
		assert_eq!(dictionary.len(), 4);
		assert_eq!(dictionary.values().len(), 2);
		assert!(dictionary.is_null(0));
	}
}
