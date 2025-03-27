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

//! Constants used in the Riegeli file format.

/// Size of a block in bytes (64 KiB).
pub const BLOCK_SIZE: usize = 1 << 16;

/// Size of a block header in bytes.
pub const BLOCK_HEADER_SIZE: usize = 24;

/// Size of the usable part of a block (block size minus block header size).
pub const USABLE_BLOCK_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;

/// Size of a chunk header in bytes.
pub const CHUNK_HEADER_SIZE: usize = 40;

/// Chunk type for file signature.
pub const CHUNK_TYPE_FILE_SIGNATURE: u8 = b's';

/// Chunk type for file metadata.
pub const CHUNK_TYPE_FILE_METADATA: u8 = b'm';

/// Chunk type for padding.
pub const CHUNK_TYPE_PADDING: u8 = b'p';

/// Chunk type for simple records.
pub const CHUNK_TYPE_SIMPLE_RECORDS: u8 = b'r';

/// Chunk type for transposed records.
pub const CHUNK_TYPE_TRANSPOSED_RECORDS: u8 = b't';

/// Compression type: None
pub const COMPRESSION_TYPE_NONE: u8 = 0;

/// Compression type: Brotli
pub const COMPRESSION_TYPE_BROTLI: u8 = b'b';

/// Compression type: Zstd
pub const COMPRESSION_TYPE_ZSTD: u8 = b'z';

/// Compression type: Snappy
pub const COMPRESSION_TYPE_SNAPPY: u8 = b's';

/// The default file signature for Riegeli files (first 64 bytes).
pub const FILE_SIGNATURE: [u8; 64] = [
    0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72,
    0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// The HighwayHash key used for hashing in Riegeli.
/// This is 'Riegeli/', 'records\n', 'Riegeli/', 'records\n' in 64-bit chunks.
pub const HIGHWAY_HASH_KEY: [u64; 4] = [
    0x2f696c6567656952, 
    0x0a7364726f636572, 
    0x2f696c6567656952, 
    0x0a7364726f636572
];
