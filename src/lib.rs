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

//! Disky is a Rust implementation of the Riegeli file format.
//!
//! Riegeli is a file format for storing records (arbitrary byte sequences, often serialized
//! protocol buffers). It supports high compression ratios and efficient reading/writing.

pub mod blocks;
pub mod chunks;
pub mod compression;
pub mod constants;
pub mod error;
pub mod hash;
pub mod reader;
pub(crate) mod varint;
pub mod writer;

// Conditionally include parallel module behind the 'parallel' feature flag
#[cfg(feature = "parallel")]
pub mod parallel;

#[cfg(test)]
mod tests;

// Re-exports for a cleaner API
//pub use reader::RecordReader;
//pub use writer::RecordWriter;
