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

use bytes::Bytes;
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
/// * `bytes` - Bytes object containing the signature chunk and potentially more data
///
/// # Returns
///
/// A `Result` containing a tuple of:
/// - A unit value () indicating success
/// - The remaining bytes after the signature header
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
/// let bytes = Bytes::from(data);
///
/// // Validate the signature
/// let ((), remaining) = validate_signature(bytes).unwrap();
///
/// // If validation succeeds, we can use the remaining bytes
/// assert_eq!(remaining, Bytes::from_static(b"some extra data"));
/// ```
pub fn validate_signature(bytes: Bytes) -> Result<((), Bytes)> {
    // Make sure we have enough bytes
    if bytes.len() < SIGNATURE_HEADER_SIZE {
        return Err(DiskyError::UnexpectedEof);
    }
    
    // Use the header parser to parse the header
    let (header, remaining) = parse_chunk_header(bytes)?;
    
    // Verify this is a signature chunk
    if header.chunk_type != ChunkType::Signature {
        return Err(DiskyError::InvalidFileSignature);
    }
    
    // Verify other signature properties
    if header.data_size != 0 || 
       header.num_records != 0 || 
       header.decoded_data_size != 0 {
        return Err(DiskyError::InvalidFileSignature);
    }
    
    // If we reach here, the signature is valid
    Ok(((), remaining))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use crate::chunks::FILE_SIGNATURE_HEADER;
    
    #[test]
    fn test_validate_valid_signature() {
        // Create a valid signature with some extra data
        let mut data = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE + 10);
        data.extend_from_slice(&FILE_SIGNATURE_HEADER);
        data.extend_from_slice(b"extra data");
        let bytes = data.freeze();
        
        // Validate the signature
        let ((), remaining) = validate_signature(bytes).unwrap();
        assert_eq!(remaining, Bytes::from_static(b"extra data"));
    }
    
    #[test]
    fn test_validate_invalid_signature() {
        // Create an invalid signature by modifying one byte
        let mut invalid_sig = BytesMut::with_capacity(SIGNATURE_HEADER_SIZE);
        invalid_sig.extend_from_slice(&FILE_SIGNATURE_HEADER);
        // Modify a byte in the signature
        invalid_sig[10] ^= 0x01;
        let bytes = invalid_sig.freeze();
        
        // Validation should fail
        assert!(validate_signature(bytes).is_err());
    }
    
    #[test]
    fn test_validate_not_enough_bytes() {
        // Create a truncated signature
        let truncated = Bytes::from(&FILE_SIGNATURE_HEADER[0..SIGNATURE_HEADER_SIZE - 1]);
        
        // Validation should fail with UnexpectedEof
        let result = validate_signature(truncated);
        assert!(matches!(result, Err(DiskyError::UnexpectedEof)));
    }
    
    #[test]
    fn test_validate_exact_size() {
        // Create a signature with exactly the header size
        let bytes = Bytes::from_static(&FILE_SIGNATURE_HEADER);
        
        // Validation should succeed with empty remaining bytes
        let ((), remaining) = validate_signature(bytes).unwrap();
        assert_eq!(remaining.len(), 0);
    }
}