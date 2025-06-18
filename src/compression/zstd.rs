//! Zstd compression implementation for disky.
//!
//! This module provides Zstd compression and decompression using the zstd-rs crate.
//! The implementation maintains internal buffers for efficient reuse.

use crate::compression::core::{Compressor, Decompressor};
use crate::error::{DiskyError, Result};
use bytes::Bytes;

/// Zstd compressor implementation
///
/// Maintains internal buffers for efficient compression operations.
/// The compression level can be configured during creation.
#[derive(Debug)]
pub struct ZstdCompressor {
    /// Internal buffer for compressed data
    compressed_buffer: Vec<u8>,
    /// Compression level (1-22, higher = better compression but slower)
    compression_level: i32,
}

impl ZstdCompressor {
    /// Create a new ZstdCompressor with specified compression level
    ///
    /// # Arguments
    /// * `level` - Compression level (1-22). Higher values provide better compression but are slower.
    pub fn with_level(level: i32) -> Result<Self> {
        if !(1..=22).contains(&level) {
            return Err(DiskyError::Other(format!(
                "Invalid zstd compression level: {}. Must be between 1 and 22.",
                level
            )));
        }

        Ok(Self {
            compressed_buffer: Vec::new(),
            compression_level: level,
        })
    }
}

impl Compressor for ZstdCompressor {
    fn compress<'a>(&'a mut self, data: &'a [u8]) -> Result<&'a [u8]> {
        // Clear buffer and ensure it has adequate size
        self.compressed_buffer.clear();
        
        // Estimate maximum compressed size using zstd's compress_bound
        let max_compressed_size = zstd::zstd_safe::compress_bound(data.len());
        
        // Pre-allocate buffer to avoid reallocation during compression
        self.compressed_buffer.resize(max_compressed_size, 0);
        
        // Compress data using zstd
        match zstd::bulk::compress_to_buffer(data, &mut self.compressed_buffer, self.compression_level) {
            Ok(compressed_size) => {
                // compress_to_buffer resizes the vector to the exact compressed size
                // But let's be explicit about the slice size
                Ok(&self.compressed_buffer[..compressed_size])
            }
            Err(e) => Err(DiskyError::Other(format!("Zstd compression failed: {}", e))),
        }
    }
}

/// Zstd decompressor implementation
///
/// Maintains internal buffer for efficient decompression operations.
#[derive(Debug)]
pub struct ZstdDecompressor {
    /// Reusable buffer for decompressed data to avoid allocations
    buffer: Vec<u8>,
}

impl ZstdDecompressor {
    /// Create a new ZstdDecompressor
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
        }
    }
}

impl Decompressor for ZstdDecompressor {
    fn decompress(&mut self, compressed_data: Bytes, expected_output_size: usize) -> Result<Bytes> {
        // Resize buffer to expected size (reusing existing capacity when possible)
        self.buffer.resize(expected_output_size, 0);
        
        // Decompress directly into our reusable buffer
        match zstd::bulk::decompress_to_buffer(&compressed_data[..], &mut self.buffer) {
            Ok(actual_size) => {
                // Validate that actual size matches expected size
                if actual_size != expected_output_size {
                    return Err(DiskyError::Other(format!(
                        "Zstd decompression size mismatch: expected {}, got {}",
                        expected_output_size, actual_size
                    )));
                }
                
                // Move data out of buffer and return as Bytes
                // mem::take replaces buffer with empty Vec, ready for next use
                let decompressed_data = std::mem::take(&mut self.buffer);
                Ok(Bytes::from(decompressed_data))
            }
            Err(e) => Err(DiskyError::Other(format!("Zstd decompression failed: {}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_decompressor_creation() {
        let _decompressor = ZstdDecompressor::new();
        // ZstdDecompressor uses automatic buffer management, no fields to check
    }

    #[test]
    fn test_zstd_compressor_creation() {
        let compressor = ZstdCompressor::with_level(6);
        assert!(compressor.is_ok());
        
        let compressor = compressor.unwrap();
        assert_eq!(compressor.compression_level, 6);
    }

    #[test]
    fn test_zstd_compressor_with_level() {
        assert!(ZstdCompressor::with_level(1).is_ok());
        assert!(ZstdCompressor::with_level(22).is_ok());
        assert!(ZstdCompressor::with_level(0).is_err());
        assert!(ZstdCompressor::with_level(23).is_err());
    }

    #[test]
    fn test_round_trip() {
        // This test will need to be implemented once we fix the Compressor trait
        // to return owned data instead of borrowed slices
    }
}
