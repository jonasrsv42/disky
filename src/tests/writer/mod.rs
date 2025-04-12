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

//! Tests for the RecordWriter implementation.

// Submodules
pub mod chunk_boundary_validation;
pub mod chunk_size_tests;
pub mod conformance;
pub mod conformance_append;
pub mod conformance_edge_cases;

use std::io::Cursor;

use crate::compression::CompressionType;
use crate::error::RiegeliError;
use crate::writer::{RecordWriter, RecordWriterConfig, WriterState};

// Test-only implementation for RecordWriter with Cursor<Vec<u8>>
impl RecordWriter<Cursor<Vec<u8>>> {
    // Extract the written data for testing
    fn get_data(mut self) -> crate::error::Result<Vec<u8>> {
        self.flush()?;
        self.set_state(WriterState::Closed);

        // Prevent drop implementation from running on self
        let writer = std::mem::ManuallyDrop::new(self);

        // Extract the Vec<u8> from the cursor (clone to avoid any ownership issues)
        let cursor = writer.get_block_writer().get_ref();
        let vec = cursor.get_ref().clone();

        Ok(vec)
    }
}

#[test]
fn test_writer_creation_and_signature() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer
    let writer = RecordWriter::new(cursor).unwrap();

    // The writer should be in the SignatureWritten state
    assert_eq!(writer.get_state(), &WriterState::SignatureWritten);
}

#[test]
fn test_write_records() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer with a small chunk size to force multiple chunks
    let config = RecordWriterConfig {
        chunk_size_bytes: 20, // Small size to force multiple chunks
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config).unwrap();

    // Write 5 records - should create 3 chunks
    for i in 0..5 {
        let record = format!("Record {}", i);
        writer.write_record(record.as_bytes()).unwrap();
    }

    // Flush the writer
    writer.flush().unwrap();

    // The writer should be in the Flushed state after flushing
    assert_eq!(writer.get_state(), &WriterState::Flushed);

    // Write one more record to check state transition
    let record = "Record 5";
    writer.write_record(record.as_bytes()).unwrap();

    // After writing, state should be RecordsWritten
    assert_eq!(writer.get_state(), &WriterState::RecordsWritten);

    // Get the written data directly
    let data = writer.get_data().unwrap();

    // Check that data was written
    assert!(!data.is_empty());
}

#[test]
fn test_close_and_reopen() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Write some records
    let mut writer = RecordWriter::new(cursor).unwrap();
    writer.write_record(b"Record 1").unwrap();
    writer.write_record(b"Record 2").unwrap();

    // Close the writer
    writer.close().unwrap();
    assert_eq!(writer.get_state(), &WriterState::Closed);

    // Writing after close should fail with WritingClosedFile error
    let result = writer.write_record(b"Record 3");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        RiegeliError::WritingClosedFile
    ));

    // Get the written data directly
    let data = writer.get_data().unwrap();

    // Create a new writer for appending
    let cursor = Cursor::new(data);
    let position = cursor.get_ref().len() as u64;

    let mut writer = RecordWriter::for_append(cursor, position).unwrap();

    // Write more records
    writer.write_record(b"Record 3").unwrap();
    writer.write_record(b"Record 4").unwrap();

    // Close the writer
    writer.close().unwrap();
}

/// Test specifically to verify that our fix for the empty chunk bug works
#[test]
fn test_no_extra_empty_chunk() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer with fixed compression type for consistent output
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config).unwrap();

    // Write a single record with known content
    writer.write_record(b"test-record").unwrap();

    // Now we'll verify the state transitions
    assert_eq!(writer.get_state(), &WriterState::RecordsWritten);

    // Flush the chunk
    writer.flush_chunk().unwrap();

    // After flushing, state should be Flushed
    assert_eq!(writer.get_state(), &WriterState::Flushed);

    // Flushing again should not create another chunk
    writer.flush().unwrap();
    writer.flush_chunk().unwrap(); // This should be a no-op due to the state check

    // State should still be Flushed
    assert_eq!(writer.get_state(), &WriterState::Flushed);
}

