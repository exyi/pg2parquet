/// Statistics for parquet writing operations.
#[derive(Debug, Clone, Default)]
pub struct WriterStats {
	pub rows: usize,
	pub bytes: usize,
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
