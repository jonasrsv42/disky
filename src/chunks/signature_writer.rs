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

//! Writer for the Riegeli file signature chunk header.
//!
//! This module provides a writer for the signature chunk header that is required
//! at the beginning of every Riegeli file. The signature header is a 40-byte sequence
//! that identifies the file as a Riegeli file and includes format information.
//!
//! ## Relationship to Riegeli Specification
//!
//! According to the Riegeli specification, the first 64 bytes of a Riegeli file consist of:
//! - A 24-byte block header
//! - A 40-byte chunk header
//!
//! This module is specifically responsible for the 40-byte chunk header portion.
//! The blocks/writer module will handle adding the 24-byte block header when writing
//! the complete 64-byte signature to a file, maintaining separation of concerns between
//! chunk-related and block-related logic.

use crate::chunks::ChunkWriter;
use crate::chunks::signature::FILE_SIGNATURE_HEADER;
use crate::error::{Result, DiskyError};

/// A writer for the Riegeli file signature chunk header.
///
/// This implements the `ChunkWriter` trait for writing the signature chunk header
/// at the beginning of a Riegeli file. The signature header is a 40-byte sequence 
/// that identifies the file format and version. The block writer will add a 24-byte 
/// block header when writing a complete 64-byte signature to a file.
///
/// The signature chunk should be the first chunk written to a new Riegeli file.
///
/// # Example
///
/// ```
/// use disky::chunks::signature_writer::SignatureWriter;
/// use disky::chunks::ChunkWriter;
///
/// let mut writer = SignatureWriter::new();
/// let signature_header = writer.try_serialize_chunk().unwrap();
/// // Write signature_header to a file or block writer...
/// ```
#[derive(Debug, Default)]
pub struct SignatureWriter {
    // Flag to track if the signature has been written
    signature_written: bool,
    // Store the signature header for returning as a slice
    signature_buffer: Vec<u8>,
}

impl SignatureWriter {
    /// Creates a new SignatureWriter.
    pub fn new() -> Self {
        Self {
            signature_written: false,
            signature_buffer: FILE_SIGNATURE_HEADER.to_vec(),
        }
    }
    
    /// Attempts to serialize the Riegeli file signature chunk header, with error handling.
    ///
    /// The signature header is a 40-byte sequence as defined in the Riegeli specification.
    /// This method will return an error if called more than once.
    ///
    /// # Returns
    ///
    /// * `Ok(&[u8])` - A slice containing the 40-byte signature header
    /// * `Err(DiskyError)` - If the signature has already been written
    pub fn try_serialize_chunk(&mut self) -> Result<&[u8]> {
        if self.signature_written {
            return Err(DiskyError::Other(
                "Signature has already been written. The signature chunk should only be written once at the beginning of a file.".to_string()
            ));
        }
        
        // Mark the signature as written
        self.signature_written = true;
        
        // Return the signature header as a slice (already filled in constructor)
        Ok(&self.signature_buffer[..])
    }
}

impl ChunkWriter for SignatureWriter {
    /// Serializes the Riegeli file signature chunk header.
    ///
    /// This implementation calls `try_serialize_chunk` and returns the result.
    /// In production code, it's recommended to use `try_serialize_chunk` directly
    /// for proper error handling.
    ///
    /// # Returns
    ///
    /// A slice containing the 40-byte signature header.
    ///
    /// # Panics
    ///
    /// This method will panic if the signature has already been written.
    fn serialize_chunk(&mut self) -> Result<&[u8]> {
        self.try_serialize_chunk()
    }
}

#[cfg(test)]
mod tests {
    use crate::chunks::header::{ChunkHeader, ChunkType};
    use crate::chunks::header_writer::write_chunk_header;
    use crate::chunks::signature::FILE_SIGNATURE_HEADER;
    use crate::chunks::signature_writer::SignatureWriter;
    use crate::error::DiskyError;
    use crate::hash::highway_hash;

    #[test]
    fn test_signature_writer_creates_correct_signature() {
        let mut writer = SignatureWriter::new();
        let signature = writer.try_serialize_chunk().unwrap();
        
        // Verify the signature is 40 bytes (chunk header only)
        assert_eq!(signature.len(), 40);
        
        // Verify the signature matches the expected constant
        assert_eq!(signature.as_ref(), FILE_SIGNATURE_HEADER.as_slice());
    }
    
    #[test]
    fn test_signature_writer_errors_on_second_call() {
        let mut writer = SignatureWriter::new();
        
        // First call should succeed
        let _ = writer.try_serialize_chunk().unwrap();
        
        // Second call should return an error
        let result = writer.try_serialize_chunk();
        assert!(result.is_err());
        
        if let Err(DiskyError::Other(msg)) = result {
            assert!(msg.contains("Signature has already been written"));
        } else {
            panic!("Expected DiskyError::Other, got: {:?}", result);
        }
    }
    
    #[test]
    fn test_signature_hash_consistency() {
        // This test verifies that the signature is consistent when 
        // created multiple times
        
        let mut writer1 = SignatureWriter::new();
        let mut writer2 = SignatureWriter::new();
        
        let signature1 = writer1.try_serialize_chunk().unwrap();
        let signature2 = writer2.try_serialize_chunk().unwrap();
        
        // The signatures should be identical
        assert_eq!(signature1, signature2);
        
        // Both signatures should match the constant
        assert_eq!(signature1.as_ref(), FILE_SIGNATURE_HEADER.as_slice());
        assert_eq!(signature2.as_ref(), FILE_SIGNATURE_HEADER.as_slice());
    }
    
    #[test]
    fn test_compare_header_writer_with_constant() {
        // Get the constant signature header
        let constant_signature = &FILE_SIGNATURE_HEADER[..];
        
        // Create a signature header using header_writer
        let data_hash = highway_hash(&[]);
        let header = ChunkHeader::new(
            0,                    // data_size (no data for signature chunk)
            data_hash,            // hash of empty data
            ChunkType::Signature, // signature chunk type ('s')
            0,                    // num_records (no records in signature)
            0                     // decoded_data_size (no records)
        );
        let generated_header = write_chunk_header(&header).unwrap();
        
        // Print both headers for comparison
        println!("FILE_SIGNATURE_HEADER constant bytes:");
        for i in 0..constant_signature.len() {
            print!("{:02x} ", constant_signature[i]);
            if (i + 1) % 8 == 0 {
                println!();
            }
        }
        
        println!("\nGenerated header bytes:");
        for i in 0..generated_header.len() {
            print!("{:02x} ", generated_header[i]);
            if (i + 1) % 8 == 0 {
                println!();
            }
        }
        
        // The generated header should match the constant
        assert_eq!(generated_header.as_ref(), constant_signature,
                 "Our header_writer should generate the same signature header as the constant");
    }
    
    #[test]
    fn test_structure_validation() {
        // Get the constant signature header
        let signature = &FILE_SIGNATURE_HEADER;
        
        // Validate key positions
        
        // Position 24 contains 's' (0x73) - signature chunk type
        assert_eq!(signature[24], ChunkType::Signature as u8);
        
        // Data size should be 0
        let data_size = u64::from_le_bytes([
            signature[8], signature[9], signature[10], signature[11],
            signature[12], signature[13], signature[14], signature[15]
        ]);
        assert_eq!(data_size, 0);
        
        // Num records should be 0
        let num_records = 
            (signature[25] as u64) |
            ((signature[26] as u64) << 8) |
            ((signature[27] as u64) << 16) |
            ((signature[28] as u64) << 24) |
            ((signature[29] as u64) << 32) |
            ((signature[30] as u64) << 40) |
            ((signature[31] as u64) << 48);
        assert_eq!(num_records, 0);
        
        // Decoded data size should be 0
        let decoded_size = u64::from_le_bytes([
            signature[32], signature[33], signature[34], signature[35],
            signature[36], signature[37], signature[38], signature[39]
        ]);
        assert_eq!(decoded_size, 0);
    }
    
    #[test]
    fn test_relationship_to_riegeli_spec() {
        // This test explains and demonstrates the relationship between:
        // 1. Our 40-byte FILE_SIGNATURE_HEADER constant (chunk header only)
        // 2. The 64-byte complete signature described in the Riegeli specification
        
        // Here's a reconstruction of the complete 64-byte signature from the Riegeli spec
        // The first 24 bytes are the block header, followed by our 40-byte chunk header
        let complete_signature = [
            // Block header (24 bytes)
            0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            
            // Chunk header (40 bytes) - this should match our FILE_SIGNATURE_HEADER constant
            0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        
        // Print the structure for educational purposes
        println!("Riegeli file signature structure:");
        println!("Block header (first 24 bytes):");
        for i in 0..24 {
            print!("{:02x} ", complete_signature[i]);
            if (i + 1) % 8 == 0 {
                println!();
            }
        }
        
        println!("\nChunk header (next 40 bytes - our FILE_SIGNATURE_HEADER):");
        for i in 24..64 {
            print!("{:02x} ", complete_signature[i]);
            if (i + 1) % 8 == 0 {
                println!();
            }
        }
        
        // Verify our FILE_SIGNATURE_HEADER matches the chunk header portion
        // of the complete signature
        assert_eq!(
            &complete_signature[24..64], 
            FILE_SIGNATURE_HEADER.as_slice(),
            "Our FILE_SIGNATURE_HEADER should match the chunk header portion of the complete signature"
        );
        
        // Note: The first 24 bytes (block header) will be added by the blocks/writer module
        // when writing a complete signature to a file. This separation of concerns allows
        // for better modularity in the codebase.
        
        // For completeness, verify the signature chunk type is at position 48 in the complete signature
        // (which is position 24 in our chunk header)
        assert_eq!(complete_signature[48], ChunkType::Signature as u8);
    }
}