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

//! Utility functions for tests

/// Helper function to print bytes in a format that's easy to copy for assertions
pub fn format_bytes_for_assert(bytes: &[u8]) -> String {
    let mut result = String::from("&[\n    ");
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && i % 8 == 0 {
            result.push_str(",\n    ");
        } else if i > 0 {
            result.push_str(", ");
        }
        result.push_str(&format!("0x{:02x}", b));
    }
    result.push_str("\n]");
    result
}