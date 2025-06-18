#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Zstd,
}

impl CompressionType {
    pub(crate) fn as_byte(&self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Zstd => b'z',
        }
    }
}
