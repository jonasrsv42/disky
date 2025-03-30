#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Brotli,
    Zstd,
    Snappy,
}

impl CompressionType {
    pub(crate) fn as_byte(&self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Brotli => b'b',
            CompressionType::Zstd => b'z',
            CompressionType::Snappy => b's',
        }
    }
}
