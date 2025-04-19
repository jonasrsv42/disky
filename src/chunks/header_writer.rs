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
//! This module provides functionality for serializing Riegeli chunk headers
//! according to the file format specification.

use bytes::{BufMut, Bytes, BytesMut};
use crate::hash::highway_hash;
use crate::error::{Result, DiskyError};
use crate::chunks::header::{ChunkHeader, CHUNK_HEADER_SIZE};

/// Serializes a Riegeli chunk header to a Bytes object.
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
/// * `header` - The ChunkHeader to serialize
///
/// # Returns
///
/// A `Result` containing a `Bytes` object with the 40-byte chunk header, or an error if
/// `num_records` exceeds the maximum allowed value.
///
/// # Example
///
/// ```
/// use disky::chunks::header::{ChunkHeader, ChunkType};
/// use disky::chunks::header_writer::write_chunk_header;
/// use disky::hash::highway_hash;
///
/// // Create simple chunk data
/// let chunk_data = b"Some chunk data";
/// let data_hash = highway_hash(chunk_data);
///
/// // Create a header for a simple records chunk
/// let header = ChunkHeader::new(
///     chunk_data.len() as u64,
///     data_hash,
///     ChunkType::SimpleRecords,
///     1, // One record
///     chunk_data.len() as u64, // Decoded size equals original size
/// );
///
/// // Serialize the header
/// let serialized = write_chunk_header(&header).unwrap();
///
/// // serialized can now be written to a file followed by chunk_data
/// ```
pub fn write_chunk_header(header: &ChunkHeader) -> Result<Bytes> {
    // Check num_records limit before allocating any memory
    if header.num_records > 0x00FF_FFFF_FFFF_FFFF {
        return Err(DiskyError::Other(
            format!("num_records ({}) exceeds maximum allowed value (0x00FFFFFFFFFFFFFF)", header.num_records)
        ));
    }
    
    // Create a buffer for the chunk header excluding the header_hash
    let mut header_data = BytesMut::with_capacity(CHUNK_HEADER_SIZE - 8);
    
    // Write data_size (8 bytes)
    header_data.put_u64_le(header.data_size);
    
    // Write data_hash (8 bytes)
    header_data.put_u64_le(header.data_hash);
    
    // Write chunk_type (1 byte)
    header_data.put_u8(header.chunk_type.as_byte());
    
    // Write 7 bytes for num_records in little-endian order
    // First byte of u64 is most significant, we skip it for 7 bytes
    for i in 0..7 {
        header_data.put_u8(((header.num_records >> (i * 8)) & 0xFF) as u8);
    }
    
    // Write decoded_data_size (8 bytes)
    header_data.put_u64_le(header.decoded_data_size);
    
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
    use crate::chunks::header::ChunkType;
    
    #[test]
    fn test_chunk_header_size() {
        let header = ChunkHeader::new(
            123,
            456,
            ChunkType::SimpleRecords,
            789,
            1011
        );
        
        let serialized = write_chunk_header(&header).unwrap();
        
        // Check that the header is exactly 40 bytes
        assert_eq!(serialized.len(), CHUNK_HEADER_SIZE);
    }
    
    #[test]
    fn test_header_field_values() {
        // Create a header with known values
        let header = ChunkHeader::new(
            1234567890,
            9876543210,
            ChunkType::SimpleRecords,
            42,
            987654321
        );
        
        let serialized = write_chunk_header(&header).unwrap();
        
        // Extract header fields from the resulting bytes
        // Skip first 8 bytes (header_hash) since it's calculated
        
        // data_size (8 bytes)
        let header_data_size = u64::from_le_bytes([
            serialized[8], serialized[9], serialized[10], serialized[11],
            serialized[12], serialized[13], serialized[14], serialized[15]
        ]);
        
        // data_hash (8 bytes)
        let header_data_hash = u64::from_le_bytes([
            serialized[16], serialized[17], serialized[18], serialized[19],
            serialized[20], serialized[21], serialized[22], serialized[23]
        ]);
        
        // chunk_type (1 byte)
        let header_chunk_type = serialized[24];
        
        // num_records (7 bytes)
        let header_num_records = 
            (serialized[25] as u64) |
            ((serialized[26] as u64) << 8) |
            ((serialized[27] as u64) << 16) |
            ((serialized[28] as u64) << 24) |
            ((serialized[29] as u64) << 32) |
            ((serialized[30] as u64) << 40) |
            ((serialized[31] as u64) << 48);
        
        // decoded_data_size (8 bytes)
        let header_decoded_data_size = u64::from_le_bytes([
            serialized[32], serialized[33], serialized[34], serialized[35],
            serialized[36], serialized[37], serialized[38], serialized[39]
        ]);
        
        // Verify the fields match the input values
        assert_eq!(header_data_size, header.data_size);
        assert_eq!(header_data_hash, header.data_hash);
        assert_eq!(header_chunk_type, header.chunk_type.as_byte());
        assert_eq!(header_num_records, header.num_records);
        assert_eq!(header_decoded_data_size, header.decoded_data_size);
    }
    
    #[test]
    fn test_header_hash_calculation() {
        // Create two headers with the same fields
        let header1 = ChunkHeader::new(100, 200, ChunkType::SimpleRecords, 5, 300);
        let header2 = ChunkHeader::new(100, 200, ChunkType::SimpleRecords, 5, 300);
        
        let serialized1 = write_chunk_header(&header1).unwrap();
        let serialized2 = write_chunk_header(&header2).unwrap();
        
        // Headers should be identical, including the calculated hash
        assert_eq!(serialized1, serialized2);
        
        // Create a header with different fields
        let header3 = ChunkHeader::new(101, 200, ChunkType::SimpleRecords, 5, 300);
        let serialized3 = write_chunk_header(&header3).unwrap();
        
        // Headers should be different due to different data_size
        assert_ne!(serialized1, serialized3);
        
        // Extract header_hash to verify it's calculated correctly
        let extracted_header_hash = u64::from_le_bytes([
            serialized1[0], serialized1[1], serialized1[2], serialized1[3],
            serialized1[4], serialized1[5], serialized1[6], serialized1[7]
        ]);
        
        // Calculate hash of the rest of the header manually
        let manual_hash = highway_hash(&serialized1[8..]);
        
        // The extracted hash should match our manually calculated hash
        assert_eq!(extracted_header_hash, manual_hash);
    }
    
    #[test]
    fn test_num_records_limit() {
        // This should return an error because num_records is too large for 7 bytes
        let header = ChunkHeader::new(
            100,
            200,
            ChunkType::SimpleRecords,
            0x0100_0000_0000_0000, // First bit set in the 8th byte, exceeding 7 bytes
            300
        );
        
        let result = write_chunk_header(&header);
        
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
            let header = ChunkHeader::new(1, 2, chunk_type, 3, 4);
            let serialized = write_chunk_header(&header).unwrap();
            assert_eq!(serialized[24], chunk_type.as_byte());
        }
    }
}