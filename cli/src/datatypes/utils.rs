pub fn read_i32(buf: &[u8]) -> i32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(buf);
    i32::from_be_bytes(b)
}

pub fn read_i64(buf: &[u8]) -> i64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(buf);
    i64::from_be_bytes(b)
}
