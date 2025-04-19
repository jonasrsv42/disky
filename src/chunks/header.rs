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

//! Common definitions for Riegeli chunk headers.
//!
//! This module provides shared structures and constants for working with Riegeli chunk headers.

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
    
    /// Convert a byte to ChunkType, if valid.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x73 => Some(ChunkType::Signature),
            0x70 => Some(ChunkType::Padding),
            0x72 => Some(ChunkType::SimpleRecords),
            _ => None,
        }
    }
}

/// Represents a Riegeli chunk header.
///
/// This structure contains all the data fields needed for a Riegeli chunk header
/// as specified in the file format. According to the specification, a chunk header is 40 bytes:
/// - header_hash (8 bytes) — hash of the rest of the header (not stored in this struct as it's calculated during serialization)
/// - data_size (8 bytes) — size of data (excluding intervening block headers)
/// - data_hash (8 bytes) — hash of data
/// - chunk_type (1 byte) — determines how to interpret data
/// - num_records (7 bytes) — number of records after decoding
/// - decoded_data_size (8 bytes) — sum of record sizes after decoding
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkHeader {
    /// Size of chunk data (excluding block headers)
    pub data_size: u64,
    /// Hash of the chunk data
    pub data_hash: u64,
    /// Type of the chunk
    pub chunk_type: ChunkType,
    /// Number of records in the chunk
    pub num_records: u64,
    /// Total size of decoded records
    pub decoded_data_size: u64,
}

impl ChunkHeader {
    /// Creates a new ChunkHeader with the given values.
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
    /// A new ChunkHeader instance.
    pub fn new(
        data_size: u64,
        data_hash: u64,
        chunk_type: ChunkType,
        num_records: u64,
        decoded_data_size: u64,
    ) -> Self {
        ChunkHeader {
            data_size,
            data_hash,
            chunk_type,
            num_records,
            decoded_data_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_chunk_type_conversion() {
        let types = [
            ChunkType::Signature,
            ChunkType::Padding,
            ChunkType::SimpleRecords,
        ];
        
        for chunk_type in types {
            let byte = chunk_type.as_byte();
            let converted = ChunkType::from_byte(byte).unwrap();
            assert_eq!(chunk_type, converted);
        }
    }
    
    #[test]
    fn test_chunk_type_invalid_byte() {
        let invalid_byte = 0xFF;
        assert!(ChunkType::from_byte(invalid_byte).is_none());
    }
    
    #[test]
    fn test_chunk_header_creation() {
        let header = ChunkHeader::new(
            100,
            200,
            ChunkType::SimpleRecords,
            5,
            300,
        );
        
        assert_eq!(header.data_size, 100);
        assert_eq!(header.data_hash, 200);
        assert_eq!(header.chunk_type, ChunkType::SimpleRecords);
        assert_eq!(header.num_records, 5);
        assert_eq!(header.decoded_data_size, 300);
    }
}