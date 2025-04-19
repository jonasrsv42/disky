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

//! Parser for Riegeli chunk headers.
//!
//! This module provides functionality for parsing Riegeli chunk headers
//! according to the file format specification.

use bytes::{Buf, Bytes};
use crate::hash::highway_hash;
use crate::error::{Result, DiskyError};
use crate::chunks::header::{ChunkHeader, ChunkType, CHUNK_HEADER_SIZE};

/// Parses a Riegeli chunk header from a Bytes object.
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
/// * `bytes` - Mutable reference to a Bytes object containing the chunk header and potentially more data.
///             The buffer will be advanced past the header after parsing.
///
/// # Returns
///
/// A `Result` containing the parsed `ChunkHeader`. The bytes buffer position will be advanced 
/// past the header, so it will only contain the data after the header when this function returns.
///
/// # Errors
///
/// Returns an error if:
/// - There are not enough bytes to parse the header
/// - The header hash doesn't match the calculated hash
/// - The chunk type is unknown
///
/// # Example
///
/// ```
/// use disky::chunks::header_parser::parse_chunk_header;
/// use disky::chunks::header::{ChunkHeader, ChunkType};
/// use disky::chunks::header_writer::write_chunk_header;
/// use bytes::Bytes;
///
/// // Create a valid chunk header
/// let header = ChunkHeader::new(
///     100,                    // data_size
///     200,                    // data_hash
///     ChunkType::SimpleRecords, // chunk_type
///     5,                      // num_records
///     300                     // decoded_data_size
/// );
/// let header_bytes = write_chunk_header(&header).unwrap();
/// 
/// // Add some data after the header
/// let mut full_bytes = Vec::new();
/// full_bytes.extend_from_slice(&header_bytes);
/// full_bytes.extend_from_slice(b"some data");
/// let mut bytes = Bytes::from(full_bytes);
///
/// // Parse the header - this will advance the bytes buffer past the header
/// let parsed_header = parse_chunk_header(&mut bytes).unwrap();
///
/// // Now bytes only contains data after the header
/// assert_eq!(parsed_header.data_size, 100);
/// assert_eq!(parsed_header.num_records, 5);
/// assert_eq!(bytes, Bytes::from_static(b"some data"));
/// ```
pub fn parse_chunk_header(bytes: &mut Bytes) -> Result<ChunkHeader> {
    // Ensure we have enough bytes for a complete header
    if bytes.remaining() < CHUNK_HEADER_SIZE {
        return Err(DiskyError::UnexpectedEof);
    }

    // Make a copy of the header portion for hash verification
    let header_copy = bytes.slice(0..CHUNK_HEADER_SIZE);
    
    // Extract header_hash (first 8 bytes)
    let header_hash = bytes.get_u64_le();

    // Calculate the hash of the rest of the header for verification
    let calculated_hash = highway_hash(&header_copy[8..]);
    if calculated_hash != header_hash {
        return Err(DiskyError::ChunkHeaderHashMismatch);
    }

    // Extract data_size (8 bytes)
    let data_size = bytes.get_u64_le();

    // Extract data_hash (8 bytes)
    let data_hash = bytes.get_u64_le();

    // Extract chunk_type (1 byte)
    let chunk_type_byte = bytes.get_u8();
    let chunk_type = match ChunkType::from_byte(chunk_type_byte) {
        Some(ct) => ct,
        None => return Err(DiskyError::UnknownChunkType(chunk_type_byte)),
    };

    // Extract num_records (7 bytes in little-endian order)
    let mut num_records: u64 = 0;
    for i in 0..7 {
        num_records |= (bytes.get_u8() as u64) << (i * 8);
    }

    // Extract decoded_data_size (8 bytes)
    let decoded_data_size = bytes.get_u64_le();

    // Create the ChunkHeader structure
    let header = ChunkHeader::new(
        data_size,
        data_hash,
        chunk_type,
        num_records,
        decoded_data_size,
    );

    // At this point, bytes has been advanced past the header
    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunks::header_writer::write_chunk_header;
    use bytes::{BytesMut, BufMut};

    #[test]
    fn test_parse_valid_header() {
        // Create a header with known values
        let original_header = ChunkHeader::new(
            1234567890,
            9876543210,
            ChunkType::SimpleRecords,
            42,
            987654321
        );
        
        let header_bytes = write_chunk_header(&original_header).unwrap();
        
        // Add some extra data to test that only the header is consumed
        let mut bytes = BytesMut::with_capacity(header_bytes.len() + 10);
        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(b"extradata");
        let mut bytes = bytes.freeze();
        
        // Parse the header
        let parsed_header = parse_chunk_header(&mut bytes).unwrap();
        
        // Verify the parsed fields match the original values
        assert_eq!(parsed_header, original_header);
        
        // Verify the remaining bytes are correct (bytes now only contains data after the header)
        assert_eq!(bytes, Bytes::from_static(b"extradata"));
    }
    
    #[test]
    fn test_parse_header_not_enough_bytes() {
        // Create a header but truncate it
        let header = ChunkHeader::new(1, 2, ChunkType::SimpleRecords, 3, 4);
        let header_bytes = write_chunk_header(&header).unwrap();
        let mut truncated = header_bytes.slice(0..CHUNK_HEADER_SIZE - 1);
        
        // Parsing should fail with UnexpectedEof
        let result = parse_chunk_header(&mut truncated);
        assert!(matches!(result, Err(DiskyError::UnexpectedEof)));
    }
    
    #[test]
    fn test_parse_header_invalid_hash() {
        // Create a valid header
        let header = ChunkHeader::new(100, 200, ChunkType::SimpleRecords, 5, 300);
        let header_bytes = write_chunk_header(&header).unwrap();
        
        // Corrupt the header data (but not the hash)
        let mut corrupted = BytesMut::with_capacity(CHUNK_HEADER_SIZE);
        corrupted.extend_from_slice(&header_bytes[0..9]); // Include hash and one more byte
        corrupted.put_u8(header_bytes[9] ^ 0x01); // Flip one bit
        corrupted.extend_from_slice(&header_bytes[10..]); // Add the rest
        let mut corrupted = corrupted.freeze();
        
        // Parsing should fail with ChunkHeaderHashMismatch
        let result = parse_chunk_header(&mut corrupted);
        assert!(matches!(result, Err(DiskyError::ChunkHeaderHashMismatch)));
    }
    
    #[test]
    fn test_parse_header_unknown_chunk_type() {
        // Create a valid header
        let header = ChunkHeader::new(100, 200, ChunkType::SimpleRecords, 5, 300);
        let header_bytes = write_chunk_header(&header).unwrap();
        
        // Corrupt the chunk type byte
        let mut corrupted = BytesMut::with_capacity(CHUNK_HEADER_SIZE);
        corrupted.extend_from_slice(&header_bytes[0..24]); // Up to chunk type
        corrupted.put_u8(0xFF); // Invalid chunk type
        corrupted.extend_from_slice(&header_bytes[25..]); // Add the rest
        
        // Recalculate the header hash
        let header_hash = highway_hash(&corrupted[8..]);
        
        // Replace the header hash
        let mut final_bytes = BytesMut::with_capacity(CHUNK_HEADER_SIZE);
        final_bytes.put_u64_le(header_hash);
        final_bytes.extend_from_slice(&corrupted[8..]);
        let mut final_bytes = final_bytes.freeze();
        
        // Parsing should fail with UnknownChunkType
        let result = parse_chunk_header(&mut final_bytes);
        assert!(matches!(result, Err(DiskyError::UnknownChunkType(0xFF))));
    }
    
    #[test]
    fn test_parse_all_chunk_types() {
        // Test parsing each valid chunk type
        let types = [
            ChunkType::Signature,
            ChunkType::Padding,
            ChunkType::SimpleRecords,
        ];
        
        for expected_type in types {
            let original_header = ChunkHeader::new(1, 2, expected_type, 3, 4);
            let header_bytes = write_chunk_header(&original_header).unwrap();
            let mut header_bytes = header_bytes.clone();
            
            let parsed_header = parse_chunk_header(&mut header_bytes).unwrap();
            assert_eq!(parsed_header.chunk_type, expected_type);
        }
    }
    
    #[test]
    fn test_exact_header_length() {
        // Create a header with exactly CHUNK_HEADER_SIZE bytes
        let header = ChunkHeader::new(100, 200, ChunkType::SimpleRecords, 5, 300);
        let header_bytes = write_chunk_header(&header).unwrap();
        
        assert_eq!(header_bytes.len(), CHUNK_HEADER_SIZE);
        
        // Parse should succeed and consume all bytes
        let mut header_bytes_clone = header_bytes.clone();
        let _ = parse_chunk_header(&mut header_bytes_clone).unwrap();
        assert_eq!(header_bytes_clone.len(), 0);
    }
}