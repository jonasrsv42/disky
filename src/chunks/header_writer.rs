// Copyright 2024
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Writer for Riegeli chunk headers.
//!
//! This module provides functionality for creating and writing Riegeli chunk headers
//! according to the file format specification.

use bytes::{BufMut, Bytes, BytesMut};
use crate::hash::highway_hash;
use crate::error::{Result, DiskyError};

/// The size of the Riegeli chunk header in bytes.
pub const CHUNK_HEADER_SIZE: usize = 40;

/// Enumeration of available chunk types in Riegeli.
///
/// Each chunk type is represented by a single byte in the chunk header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkType {
    /// File signature chunk (0x73, 's')
    Signature = 0x73,
    /// Padding chunk (0x70, 'p')
    Padding = 0x70,
    /// Simple chunk with records (0x72, 'r')
    SimpleRecords = 0x72,
}

impl ChunkType {
    /// Convert the ChunkType enum to its byte representation.
    pub fn as_byte(&self) -> u8 {
        *self as u8
    }
}

/// Writes a Riegeli chunk header to a Bytes object.
///
/// According to the Riegeli file format spec, a chunk header is 40 bytes:
/// - header_hash (8 bytes) — hash of the rest of the header
/// - data_size (8 bytes) — size of data (excluding intervening block headers)
/// - data_hash (8 bytes) — hash of data
/// - chunk_type (1 byte) — determines how to interpret data
/// - num_records (7 bytes) — number of records after decoding
/// - decoded_data_size (8 bytes) — sum of record sizes after decoding
///
/// # Arguments
///
/// * `data_size` - Size of the chunk data (excluding block headers)
/// * `data_hash` - Hash of the chunk data
/// * `chunk_type` - Type of the chunk
/// * `num_records` - Number of records in the chunk
/// * `decoded_data_size` - Total size of decoded records
///
/// # Returns
///
/// A `Result` containing a `Bytes` object with the 40-byte chunk header, or an error if
/// `num_records` exceeds the maximum allowed value.
///
/// # Example
///
/// ```
/// use disky::chunks::header_writer::{write_chunk_header, ChunkType};
/// use disky::hash::highway_hash;
///
/// // Create simple chunk data
/// let chunk_data = b"Some chunk data";
/// let data_hash = highway_hash(chunk_data);
///
/// // Write a header for a simple records chunk
/// let header = write_chunk_header(
///     chunk_data.len() as u64,
///     data_hash,
///     ChunkType::SimpleRecords,
///     1, // One record
///     chunk_data.len() as u64, // Decoded size equals original size
/// ).unwrap();
///
/// // header can now be written to a file followed by chunk_data
/// ```
pub fn write_chunk_header(
    data_size: u64,
    data_hash: u64,
    chunk_type: ChunkType,
    num_records: u64,
    decoded_data_size: u64,
) -> Result<Bytes> {
    // Check num_records limit before allocating any memory
    if num_records > 0x00FF_FFFF_FFFF_FFFF {
        return Err(DiskyError::Other(
            format!("num_records ({}) exceeds maximum allowed value (0x00FFFFFFFFFFFFFF)", num_records)
        ));
    }
    
    // Create a buffer for the chunk header excluding the header_hash
    let mut header_data = BytesMut::with_capacity(CHUNK_HEADER_SIZE - 8);
    
    // Write data_size (8 bytes)
    header_data.put_u64_le(data_size);
    
    // Write data_hash (8 bytes)
    header_data.put_u64_le(data_hash);
    
    // Write chunk_type (1 byte)
    header_data.put_u8(chunk_type.as_byte());
    
    // Write 7 bytes for num_records in little-endian order
    // First byte of u64 is most significant, we skip it for 7 bytes
    for i in 0..7 {
        header_data.put_u8(((num_records >> (i * 8)) & 0xFF) as u8);
    }
    
    // Write decoded_data_size (8 bytes)
    header_data.put_u64_le(decoded_data_size);
    
    // Calculate header_hash
    let header_hash = highway_hash(&header_data);
    
    // Create the final header bytes
    let mut full_header = BytesMut::with_capacity(CHUNK_HEADER_SIZE);
    
    // Write header_hash (8 bytes)
    full_header.put_u64_le(header_hash);
    
    // Add the rest of the header
    full_header.extend_from_slice(&header_data);
    
    Ok(full_header.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_chunk_header_size() {
        let header = write_chunk_header(
            123,
            456,
            ChunkType::SimpleRecords,
            789,
            1011
        ).unwrap();
        
        // Check that the header is exactly 40 bytes
        assert_eq!(header.len(), CHUNK_HEADER_SIZE);
    }
    
    #[test]
    fn test_header_field_values() {
        // Create a header with known values
        let data_size = 1234567890;
        let data_hash = 9876543210;
        let chunk_type = ChunkType::SimpleRecords;
        let num_records = 42;
        let decoded_data_size = 987654321;
        
        let header = write_chunk_header(
            data_size,
            data_hash,
            chunk_type,
            num_records,
            decoded_data_size
        ).unwrap();
        
        // Extract header fields from the resulting bytes
        // Skip first 8 bytes (header_hash) since it's calculated
        
        // data_size (8 bytes)
        let header_data_size = u64::from_le_bytes([
            header[8], header[9], header[10], header[11],
            header[12], header[13], header[14], header[15]
        ]);
        
        // data_hash (8 bytes)
        let header_data_hash = u64::from_le_bytes([
            header[16], header[17], header[18], header[19],
            header[20], header[21], header[22], header[23]
        ]);
        
        // chunk_type (1 byte)
        let header_chunk_type = header[24];
        
        // num_records (7 bytes)
        let header_num_records = 
            (header[25] as u64) |
            ((header[26] as u64) << 8) |
            ((header[27] as u64) << 16) |
            ((header[28] as u64) << 24) |
            ((header[29] as u64) << 32) |
            ((header[30] as u64) << 40) |
            ((header[31] as u64) << 48);
        
        // decoded_data_size (8 bytes)
        let header_decoded_data_size = u64::from_le_bytes([
            header[32], header[33], header[34], header[35],
            header[36], header[37], header[38], header[39]
        ]);
        
        // Verify the fields match the input values
        assert_eq!(header_data_size, data_size);
        assert_eq!(header_data_hash, data_hash);
        assert_eq!(header_chunk_type, chunk_type.as_byte());
        assert_eq!(header_num_records, num_records);
        assert_eq!(header_decoded_data_size, decoded_data_size);
    }
    
    #[test]
    fn test_header_hash_calculation() {
        // Create two headers with the same fields
        let header1 = write_chunk_header(100, 200, ChunkType::SimpleRecords, 5, 300).unwrap();
        let header2 = write_chunk_header(100, 200, ChunkType::SimpleRecords, 5, 300).unwrap();
        
        // Headers should be identical, including the calculated hash
        assert_eq!(header1, header2);
        
        // Create a header with different fields
        let header3 = write_chunk_header(101, 200, ChunkType::SimpleRecords, 5, 300).unwrap();
        
        // Headers should be different due to different data_size
        assert_ne!(header1, header3);
        
        // Extract header_hash to verify it's calculated correctly
        let extracted_header_hash = u64::from_le_bytes([
            header1[0], header1[1], header1[2], header1[3],
            header1[4], header1[5], header1[6], header1[7]
        ]);
        
        // Calculate hash of the rest of the header manually
        let manual_hash = highway_hash(&header1[8..]);
        
        // The extracted hash should match our manually calculated hash
        assert_eq!(extracted_header_hash, manual_hash);
    }
    
    #[test]
    fn test_num_records_limit() {
        // This should return an error because num_records is too large for 7 bytes
        let result = write_chunk_header(
            100,
            200,
            ChunkType::SimpleRecords,
            0x0100_0000_0000_0000, // First bit set in the 8th byte, exceeding 7 bytes
            300
        );
        
        assert!(result.is_err());
        if let Err(DiskyError::Other(msg)) = result {
            assert!(msg.contains("num_records"));
            assert!(msg.contains("exceeds maximum allowed value"));
        } else {
            panic!("Expected Other error with message about num_records, got: {:?}", result);
        }
    }
    
    #[test]
    fn test_all_chunk_types() {
        // Test that all chunk types can be used and are correctly encoded
        let types = [
            ChunkType::Signature,
            ChunkType::Padding,
            ChunkType::SimpleRecords,
        ];
        
        for chunk_type in types {
            let header = write_chunk_header(1, 2, chunk_type, 3, 4).unwrap();
            assert_eq!(header[24], chunk_type.as_byte());
        }
    }
}