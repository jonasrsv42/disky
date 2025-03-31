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

//! Writer for the Riegeli file signature.
//!
//! This module provides a writer for the signature chunk that is required
//! at the beginning of every Riegeli file. The signature is a 64-byte sequence
//! that identifies the file as a Riegeli file and holds specific format information.

use bytes::{BufMut, Bytes, BytesMut};

use crate::chunks::ChunkWriter;
use crate::error::{Result, RiegeliError};

/// The default file signature for Riegeli files (first 64 bytes).
/// 
/// This signature identifies a file as a Riegeli file and includes metadata
/// about the file format. It should be written at the beginning of every
/// Riegeli file.
pub const FILE_SIGNATURE: [u8; 64] = [
    0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72,
    0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// A writer for the Riegeli file signature.
///
/// This implements the `ChunkWriter` trait for writing the file signature
/// chunk at the beginning of a Riegeli file. The signature is a 64-byte sequence 
/// that identifies the file format and version.
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
/// let signature_chunk = writer.try_serialize_chunk().unwrap();
/// // Write signature_chunk to a file or block writer...
/// ```
#[derive(Debug, Default)]
pub struct SignatureWriter {
    // Flag to track if the signature has been written
    signature_written: bool,
}

impl SignatureWriter {
    /// Creates a new SignatureWriter.
    pub fn new() -> Self {
        Self {
            signature_written: false,
        }
    }
    
    /// Attempts to serialize the Riegeli file signature as a chunk, with error handling.
    ///
    /// The signature is a 64-byte sequence as defined in the Riegeli specification.
    /// This method will return an error if called more than once.
    ///
    /// # Returns
    ///
    /// * `Ok(Bytes)` - A `Bytes` object containing the 64-byte signature
    /// * `Err(RiegeliError)` - If the signature has already been written
    pub fn try_serialize_chunk(&mut self) -> Result<Bytes> {
        if self.signature_written {
            return Err(RiegeliError::Other(
                "Signature has already been written. The signature chunk should only be written once at the beginning of a file.".to_string()
            ));
        }
        
        // Mark the signature as written
        self.signature_written = true;
        
        // Create a buffer with the signature
        let mut buffer = BytesMut::with_capacity(FILE_SIGNATURE.len());
        buffer.put_slice(&FILE_SIGNATURE);
        
        // Return the signature as Bytes
        Ok(buffer.freeze())
    }
}

impl ChunkWriter for SignatureWriter {
    /// Serializes the Riegeli file signature as a chunk.
    ///
    /// This implementation calls `try_serialize_chunk` and unwraps the result.
    /// In production code, it's recommended to use `try_serialize_chunk` directly
    /// for proper error handling.
    ///
    /// # Returns
    ///
    /// A `Bytes` object containing the 64-byte signature.
    ///
    /// # Panics
    ///
    /// This method will panic if the signature has already been written.
    fn serialize_chunk(&mut self) -> Bytes {
        match self.try_serialize_chunk() {
            Ok(signature) => signature,
            Err(e) => panic!("Failed to serialize signature chunk: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_signature_writer_creates_correct_signature() {
        let mut writer = SignatureWriter::new();
        let signature = writer.try_serialize_chunk().unwrap();
        
        // Verify the signature matches the expected constant
        assert_eq!(signature.as_ref(), FILE_SIGNATURE.as_slice());
        assert_eq!(signature.len(), 64);
    }
    
    #[test]
    fn test_signature_writer_errors_on_second_call() {
        let mut writer = SignatureWriter::new();
        
        // First call should succeed
        let _ = writer.try_serialize_chunk().unwrap();
        
        // Second call should return an error
        let result = writer.try_serialize_chunk();
        assert!(result.is_err());
        
        if let Err(RiegeliError::Other(msg)) = result {
            assert!(msg.contains("Signature has already been written"));
        } else {
            panic!("Expected RiegeliError::Other, got: {:?}", result);
        }
    }
    
    #[test]
    #[should_panic(expected = "Failed to serialize signature chunk")]
    fn test_serialize_chunk_panics_on_second_call() {
        let mut writer = SignatureWriter::new();
        
        // First call should succeed
        let _ = writer.serialize_chunk();
        
        // Second call should panic
        let _ = writer.serialize_chunk();
    }
}