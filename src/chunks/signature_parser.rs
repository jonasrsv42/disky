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

//! Parser for Riegeli file signatures.
//!
//! This module provides functionality for validating Riegeli file signatures.

use bytes::{Buf, Bytes};
use crate::error::{Result, DiskyError};
use crate::chunks::signature::SIGNATURE_HEADER_SIZE;
use crate::chunks::header::ChunkType;
use crate::chunks::header_parser::parse_chunk_header;

/// Validates a Riegeli file signature chunk.
///
/// This function checks if the provided bytes start with a valid Riegeli file signature.
/// A valid signature must have:
/// 1. The correct header hash
/// 2. The chunk type set to 's' (Signature)
/// 3. The right size and format
///
/// # Arguments
///
/// * `bytes` - Mutable reference to a Bytes object containing the signature chunk and potentially more data.
///             The buffer will be advanced past the signature header after validation.
///
/// # Returns
///
/// A `Result<()>` indicating success. The bytes buffer will be advanced past the signature
/// header, so it will only contain the data after the signature when this function returns.
///
/// # Errors
///
/// Returns an error if:
/// - There are not enough bytes to parse the header
/// - The signature does not match the expected format
/// - The chunk type is not 's' (Signature)
///
/// # Example
///
/// ```
/// use disky::chunks::signature_parser::validate_signature;
/// use disky::chunks::FILE_SIGNATURE_HEADER;
/// use bytes::Bytes;
///
/// // Create a bytes object with a valid signature header and some extra data
/// let mut data = Vec::new();
/// data.extend_from_slice(&FILE_SIGNATURE_HEADER);
/// data.extend_from_slice(b"some extra data");
/// let mut bytes = Bytes::from(data);
///
/// // Validate the signature - this will advance the bytes buffer past the signature
/// validate_signature(&mut bytes).unwrap();
///
/// // If validation succeeds, bytes will only contain data after the signature
/// assert_eq!(bytes, Bytes::from_static(b"some extra data"));
/// ```
pub fn validate_signature(bytes: &mut Bytes) -> Result<()> {
    // Make sure we have enough bytes
    if bytes.remaining() < SIGNATURE_HEADER_SIZE {
        return Err(DiskyError::UnexpectedEof);
    }
    
    // Use the header parser to parse the header
    let header = parse_chunk_header(bytes)?;
    
    // Verify this is a signature chunk
    if header.chunk_type != ChunkType::Signature {
        return Err(DiskyError::InvalidFileSignature(
            format!("Expected chunk type 's' (Signature), got '{}'", 
                    header.chunk_type as u8 as char)
        ));
    }
    
    // Verify data_size is 0
    if header.data_size != 0 {
        return Err(DiskyError::InvalidFileSignature(
            format!("Expected data_size to be 0, got {}", 
                    header.data_size)
        ));
    }
    
    // Verify num_records is 0
    if header.num_records != 0 {
        return Err(DiskyError::InvalidFileSignature(
            format!("Expected num_records to be 0, got {}", 
                    header.num_records)
        ));
    }
    
    // Verify decoded_data_size is 0
    if header.decoded_data_size != 0 {
        return Err(DiskyError::InvalidFileSignature(
            format!("Expected decoded_data_size to be 0, got {}", 
                    header.decoded_data_size)
        ));
    }
    
    // If we reach here, the signature is valid
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BytesMut, BufMut};
    use crate::chunks::{FILE_SIGNATURE_HEADER, ChunkType};
    use crate::hash::highway_hash;
    
    #[test]
    fn test_validate_valid_signature() {
        // Create a valid signature with some extra data
        let mut data = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE + 10);
        data.extend_from_slice(&FILE_SIGNATURE_HEADER);
        data.extend_from_slice(b"extra data");
        let mut bytes = data.freeze();
        
        // Validate the signature
        validate_signature(&mut bytes).unwrap();
        
        // The bytes buffer should now only contain the extra data
        assert_eq!(bytes, Bytes::from_static(b"extra data"));
    }
    
    #[test]
    fn test_validate_invalid_signature() {
        // Create an invalid signature by modifying one byte
        let mut invalid_sig = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        invalid_sig.extend_from_slice(&FILE_SIGNATURE_HEADER);
        // Modify a byte in the signature
        invalid_sig[10] ^= 0x01;
        let mut bytes = invalid_sig.freeze();
        
        // Validation should fail with ChunkHeaderHashMismatch
        let result = validate_signature(&mut bytes);
        assert!(matches!(result, Err(DiskyError::ChunkHeaderHashMismatch)));
    }
    
    #[test]
    fn test_validate_wrong_chunk_type() {
        // Create a valid header bytes
        let mut invalid_sig = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        invalid_sig.extend_from_slice(&FILE_SIGNATURE_HEADER);
        
        // Modify the chunk type byte (position 24) to 'r' (SimpleRecords)
        invalid_sig[24] = ChunkType::SimpleRecords as u8;
        
        // Recalculate header hash after modification
        let header_hash = highway_hash(&invalid_sig[8..]);
        
        // Put the new header hash at the beginning
        let mut final_bytes = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        final_bytes.put_u64_le(header_hash);
        final_bytes.extend_from_slice(&invalid_sig[8..]);
        let mut bytes = final_bytes.freeze();
        
        // Validation should fail with InvalidFileSignature mentioning wrong chunk type
        let result = validate_signature(&mut bytes);
        if let Err(DiskyError::InvalidFileSignature(msg)) = result {
            assert!(msg.contains("Expected chunk type 's'"));
            assert!(msg.contains("got 'r'"));
        } else {
            panic!("Expected InvalidFileSignature with message, got: {:?}", result);
        }
    }
    
    #[test]
    fn test_validate_wrong_data_size() {
        // Create a header with non-zero data_size
        let mut invalid_sig = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        invalid_sig.extend_from_slice(&FILE_SIGNATURE_HEADER);
        
        // Change data_size to 42 (bytes 8-15)
        invalid_sig[8] = 42;  // Set first byte to 42, rest remain 0
        
        // Recalculate header hash after modification
        let header_hash = highway_hash(&invalid_sig[8..]);
        
        // Put the new header hash at the beginning
        let mut final_bytes = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        final_bytes.put_u64_le(header_hash);
        final_bytes.extend_from_slice(&invalid_sig[8..]);
        let mut bytes = final_bytes.freeze();
        
        // Validation should fail with InvalidFileSignature mentioning wrong data_size
        let result = validate_signature(&mut bytes);
        if let Err(DiskyError::InvalidFileSignature(msg)) = result {
            assert!(msg.contains("Expected data_size to be 0"));
            assert!(msg.contains("got 42"));
        } else {
            panic!("Expected InvalidFileSignature with message, got: {:?}", result);
        }
    }
    
    #[test]
    fn test_validate_wrong_num_records() {
        // Create a header with non-zero num_records
        let mut invalid_sig = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        invalid_sig.extend_from_slice(&FILE_SIGNATURE_HEADER);
        
        // Change num_records to 5 (bytes 25-31)
        invalid_sig[25] = 5;  // Set first byte to 5, rest remain 0
        
        // Recalculate header hash after modification
        let header_hash = highway_hash(&invalid_sig[8..]);
        
        // Put the new header hash at the beginning
        let mut final_bytes = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        final_bytes.put_u64_le(header_hash);
        final_bytes.extend_from_slice(&invalid_sig[8..]);
        let mut bytes = final_bytes.freeze();
        
        // Validation should fail with InvalidFileSignature mentioning wrong num_records
        let result = validate_signature(&mut bytes);
        if let Err(DiskyError::InvalidFileSignature(msg)) = result {
            assert!(msg.contains("Expected num_records to be 0"));
            assert!(msg.contains("got 5"));
        } else {
            panic!("Expected InvalidFileSignature with message, got: {:?}", result);
        }
    }
    
    #[test]
    fn test_validate_wrong_decoded_data_size() {
        // Create a header with non-zero decoded_data_size
        let mut invalid_sig = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        invalid_sig.extend_from_slice(&FILE_SIGNATURE_HEADER);
        
        // Change decoded_data_size to 100 (bytes 32-39)
        invalid_sig[32] = 100;  // Set first byte to 100, rest remain 0
        
        // Recalculate header hash after modification
        let header_hash = highway_hash(&invalid_sig[8..]);
        
        // Put the new header hash at the beginning
        let mut final_bytes = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        final_bytes.put_u64_le(header_hash);
        final_bytes.extend_from_slice(&invalid_sig[8..]);
        let mut bytes = final_bytes.freeze();
        
        // Validation should fail with InvalidFileSignature mentioning wrong decoded_data_size
        let result = validate_signature(&mut bytes);
        if let Err(DiskyError::InvalidFileSignature(msg)) = result {
            assert!(msg.contains("Expected decoded_data_size to be 0"));
            assert!(msg.contains("got 100"));
        } else {
            panic!("Expected InvalidFileSignature with message, got: {:?}", result);
        }
    }
    
    #[test]
    fn test_validate_not_enough_bytes() {
        // Create a truncated signature
        let mut truncated = Bytes::from(&FILE_SIGNATURE_HEADER[0..SIGNATURE_HEADER_SIZE - 1]);
        
        // Validation should fail with UnexpectedEof
        let result = validate_signature(&mut truncated);
        assert!(matches!(result, Err(DiskyError::UnexpectedEof)));
    }
    
    #[test]
    fn test_validate_exact_size() {
        // Create a signature with exactly the header size
        let mut bytes = Bytes::from_static(&FILE_SIGNATURE_HEADER);
        
        // Validation should succeed and consume all bytes
        validate_signature(&mut bytes).unwrap();
        assert_eq!(bytes.len(), 0);
    }
}