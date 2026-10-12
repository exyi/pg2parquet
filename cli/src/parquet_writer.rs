use std::sync::Arc;

use parquet::basic::{ConvertedType, LogicalType, Type};
use parquet::file::properties::{EnabledStatistics, WriterPropertiesPtr};
use parquet::schema::types::{SchemaDescriptor, TypePtr};

/// Workaround for https://github.com/apache/arrow-rs/issues/11073
pub(crate) fn disable_binary_decimal_statistics(
	schema: TypePtr,
	props: WriterPropertiesPtr,
) -> WriterPropertiesPtr {
	let descriptor = SchemaDescriptor::new(schema);
	let mut builder = props.as_ref().clone().into_builder();
	for column in descriptor.columns() {
		if column.physical_type() == Type::BYTE_ARRAY
			&& (matches!(column.logical_type_ref(), Some(LogicalType::Decimal(_)))
				|| column.converted_type() == ConvertedType::DECIMAL)
		{
			builder = builder.set_column_statistics_enabled(column.path().clone(), EnabledStatistics::None);
		}
	}
	Arc::new(builder.build())
}

/// Statistics for parquet writing operations.
#[derive(Debug, Clone, Default)]
pub struct WriterStats {
	pub rows: usize,
	pub bytes: u64,
	pub bytes_out: usize,
	pub groups: usize,
}

/// Settings for parquet writer behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterSettings {
	pub row_group_byte_limit: usize,
	pub row_group_row_limit: usize,
}

impl WriterSettings {
	/// Construct settings with explicit row and byte limits.
	pub fn new(row_group_byte_limit: usize, row_group_row_limit: usize) -> Self {
		Self {
			row_group_byte_limit,
			row_group_row_limit,
		}
	}
}
