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


/// The HighwayHash key used for hashing in Riegeli.
/// This is 'Riegeli/', 'records\n', 'Riegeli/', 'records\n' in 64-bit chunks.
pub const HIGHWAY_HASH_KEY: [u64; 4] = [
    0x2f696c6567656952,
    0x0a7364726f636572,
    0x2f696c6567656952,
    0x0a7364726f636572,
];
