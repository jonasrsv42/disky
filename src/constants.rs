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
    0x0a7364726f636572,
];
