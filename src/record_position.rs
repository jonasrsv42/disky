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

//! Handling of record positions in a Riegeli file.

/// Position information for a record in a Riegeli file.
///
/// A record position consists of the chunk's beginning position and the record's
/// index within the chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordPosition {
    /// Position of the beginning of the chunk.
    pub chunk_begin: u64,
    
    /// Index of the record within the chunk.
    pub record_index: u64,
}

impl RecordPosition {
    /// Creates a new RecordPosition.
    pub fn new(chunk_begin: u64, record_index: u64) -> Self {
        Self { chunk_begin, record_index }
    }

    /// Returns the numeric position value.
    ///
    /// This is the chunk begin position plus the record index.
    pub fn numeric(&self) -> u64 {
        self.chunk_begin + self.record_index
    }
}

impl From<(u64, u64)> for RecordPosition {
    fn from((chunk_begin, record_index): (u64, u64)) -> Self {
        Self { chunk_begin, record_index }
    }
}

impl From<RecordPosition> for u64 {
    fn from(pos: RecordPosition) -> Self {
        pos.numeric()
    }
}