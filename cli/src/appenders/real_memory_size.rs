
pub trait RealMemorySize {
	fn real_memory_size(&self) -> usize;
}

impl RealMemorySize for bool {
	fn real_memory_size(&self) -> usize { 1 }
}
impl RealMemorySize for i8 {
	fn real_memory_size(&self) -> usize { 1 }
}
impl RealMemorySize for u8 {
	fn real_memory_size(&self) -> usize { 1 }
}
impl RealMemorySize for i16 {
	fn real_memory_size(&self) -> usize { 2 }
}
impl RealMemorySize for u16 {
	fn real_memory_size(&self) -> usize { 2 }
}
impl RealMemorySize for i32 {
	fn real_memory_size(&self) -> usize { 4 }
}
impl RealMemorySize for u32 {
	fn real_memory_size(&self) -> usize { 4 }
}
impl RealMemorySize for i64 {
	fn real_memory_size(&self) -> usize { 8 }
}
impl RealMemorySize for u64 {
	fn real_memory_size(&self) -> usize { 8 }
}
impl RealMemorySize for f32 {
	fn real_memory_size(&self) -> usize { 4 }
}
impl RealMemorySize for f64 {
	fn real_memory_size(&self) -> usize { 8 }
}
impl RealMemorySize for [u8] {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for Vec<u8> {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for str {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for parquet::data_type::Decimal {
	fn real_memory_size(&self) -> usize { self.data().len() }
}
impl RealMemorySize for parquet::data_type::ByteArray {
	fn real_memory_size(&self) -> usize { self.data().len() }
}
impl RealMemorySize for parquet::data_type::FixedLenByteArray {
	fn real_memory_size(&self) -> usize { self.len() }
}
impl RealMemorySize for parquet::data_type::Int96 {
	fn real_memory_size(&self) -> usize { 12 }
}

