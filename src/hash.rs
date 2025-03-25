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

//! Hashing functionality for Riegeli files.

use highway::{HighwayHasher, HighwayHash, Key};
use crate::constants::HIGHWAY_HASH_KEY;

/// Calculate the HighwayHash for a chunk of bytes.
///
/// Riegeli uses HighwayHash with a specific key.
pub fn highway_hash(data: &[u8]) -> u64 {
    let mut hasher = HighwayHasher::new(Key(HIGHWAY_HASH_KEY));
    hasher.append(data);
    hasher.finalize64()
}