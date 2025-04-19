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

//! Common definitions for Riegeli file signatures.
//!
//! This module provides shared constants for working with Riegeli file signatures.


/// The Riegeli file signature chunk header (40 bytes).
///
/// This is the standard signature header that marks the beginning of a Riegeli file.
/// It contains specific values for header fields that identify the file format.
pub const FILE_SIGNATURE_HEADER: [u8; 40] = [
    0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// The size of the Riegeli file signature chunk header in bytes.
pub const SIGNATURE_HEADER_SIZE: usize = 40;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunks::ChunkType;
    
    #[test]
    fn test_signature_structure() {
        // Verify the signature chunk type is at position 24 in the header
        assert_eq!(FILE_SIGNATURE_HEADER[24], ChunkType::Signature as u8);
        
        // Data size should be 0
        let data_size = u64::from_le_bytes([
            FILE_SIGNATURE_HEADER[8], FILE_SIGNATURE_HEADER[9], 
            FILE_SIGNATURE_HEADER[10], FILE_SIGNATURE_HEADER[11],
            FILE_SIGNATURE_HEADER[12], FILE_SIGNATURE_HEADER[13], 
            FILE_SIGNATURE_HEADER[14], FILE_SIGNATURE_HEADER[15]
        ]);
        assert_eq!(data_size, 0);
        
        // Num records should be 0
        let num_records = 
            (FILE_SIGNATURE_HEADER[25] as u64) |
            ((FILE_SIGNATURE_HEADER[26] as u64) << 8) |
            ((FILE_SIGNATURE_HEADER[27] as u64) << 16) |
            ((FILE_SIGNATURE_HEADER[28] as u64) << 24) |
            ((FILE_SIGNATURE_HEADER[29] as u64) << 32) |
            ((FILE_SIGNATURE_HEADER[30] as u64) << 40) |
            ((FILE_SIGNATURE_HEADER[31] as u64) << 48);
        assert_eq!(num_records, 0);
        
        // Decoded data size should be 0
        let decoded_size = u64::from_le_bytes([
            FILE_SIGNATURE_HEADER[32], FILE_SIGNATURE_HEADER[33], 
            FILE_SIGNATURE_HEADER[34], FILE_SIGNATURE_HEADER[35],
            FILE_SIGNATURE_HEADER[36], FILE_SIGNATURE_HEADER[37], 
            FILE_SIGNATURE_HEADER[38], FILE_SIGNATURE_HEADER[39]
        ]);
        assert_eq!(decoded_size, 0);
    }
}