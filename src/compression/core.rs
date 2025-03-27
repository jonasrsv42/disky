pub(crate) enum CompressionType {
    None,
}

impl CompressionType {
    pub(crate) fn as_byte(&self) -> u8 {
        match self {
            CompressionType::None => 0,
        }
    }
}
