use crate::error::Result;
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    #[cfg(feature = "zstd")]
    Zstd,
}

impl CompressionType {
    pub(crate) fn as_byte(&self) -> u8 {
        match self {
            CompressionType::None => 0,
            #[cfg(feature = "zstd")]
            CompressionType::Zstd => b'z',
        }
    }
}

/// Trait for compression algorithms used in disky chunk writers
///
/// Compressors maintain internal buffers and return references to compressed data.
/// This allows for efficient buffer reuse and zero-copy operation for NoCompression.
pub trait Compressor: Send + Sync {
    /// Compress the input data and return a reference to compressed bytes.
    /// The returned slice is valid as long as both the compressor and input data live.
    /// For NoCompression, this returns the input data directly (zero-copy).
    /// For real compression, this returns a reference to the compressor's internal buffer.
    fn compress<'a>(&'a mut self, data: &'a [u8]) -> Result<&'a [u8]>;
}

/// Trait for decompression algorithms used in disky chunk readers
///
/// Decompressors work with Bytes objects to enable zero-copy operation for NoCompression
/// while still supporting owned data for real compression algorithms.
pub trait Decompressor: Send {
    /// Decompress the input data and return decompressed bytes.
    /// 
    /// # Arguments
    /// * `compressed_data` - The compressed data to decompress
    /// * `expected_output_size` - Known size of decompressed output for efficiency and validation
    /// 
    /// For NoCompression, this is zero-copy (returns the input unchanged).
    /// For real compression, this uses the size hint for efficient buffer allocation.
    fn decompress(&mut self, compressed_data: Bytes, expected_output_size: usize) -> Result<Bytes>;
}

/// No-op implementation that just passes data through unchanged
///
/// This implementation has true zero-copy behavior - it simply returns
/// references to the input data without any copying or allocation.
/// It implements both Compressor and Decompressor traits.
#[derive(Debug, Clone)]
pub struct NoCompression;

impl Compressor for NoCompression {
    fn compress<'a>(&'a mut self, data: &'a [u8]) -> Result<&'a [u8]> {
        Ok(data) // Zero copy - just return input slice
    }
}

impl Decompressor for NoCompression {
    fn decompress(&mut self, compressed_data: Bytes, _expected_output_size: usize) -> Result<Bytes> {
        Ok(compressed_data) // Zero copy - just return input Bytes unchanged
    }
}

/// Factory function to create a compressor for a given compression type
pub fn create_compressor(compression_type: CompressionType) -> Result<Box<dyn Compressor>> {
    match compression_type {
        CompressionType::None => Ok(Box::new(NoCompression)),
        #[cfg(feature = "zstd")]
        CompressionType::Zstd => Ok(Box::new(crate::compression::zstd::ZstdCompressor::new()?)),
    }
}

/// Create a map of all available decompressors
///
/// Returns a BTreeMap with pre-initialized decompressors for all supported compression types.
/// BTreeMap is more efficient than HashMap for the small number of compression types (~2-4 items).
/// Keys are the compression type bytes (0 = None, b'z' = Zstd, etc.).
/// Only includes compression types that are actually implemented.
pub fn create_decompressors_map() -> BTreeMap<u8, Box<dyn Decompressor>> {
    let mut map = BTreeMap::new();

    // Add all implemented decompressors using their byte values
    map.insert(
        CompressionType::None.as_byte(),
        Box::new(NoCompression) as Box<dyn Decompressor>,
    );

    #[cfg(feature = "zstd")]
    map.insert(
        CompressionType::Zstd.as_byte(),
        Box::new(crate::compression::zstd::ZstdDecompressor::new()) as Box<dyn Decompressor>,
    );

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression_compress() {
        let mut compressor = NoCompression;
        let data = b"hello world";

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data);

        // Verify it's actually the same slice (zero copy)
        assert_eq!(compressed.as_ptr(), data.as_ptr());
    }

    #[test]
    fn test_no_compression_decompress() {
        let mut decompressor = NoCompression;
        let data = Bytes::from_static(b"hello world");

        let decompressed = decompressor.decompress(data.clone(), data.len()).unwrap();
        assert_eq!(decompressed, data);

        // Verify it's actually the same Bytes object (zero copy)
        assert_eq!(decompressed.as_ptr(), data.as_ptr());
    }

    #[test]
    fn test_create_compressor_none() {
        let compressor = create_compressor(super::super::core::CompressionType::None);
        assert!(compressor.is_ok());
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_create_compressor_zstd() {
        let compressor = create_compressor(CompressionType::Zstd);
        assert!(compressor.is_ok());
    }
}
