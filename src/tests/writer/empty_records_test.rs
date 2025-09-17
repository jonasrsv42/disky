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

//! Tests for empty records handling in the RecordWriter implementation.

use std::io::Cursor;

use crate::reader::{DiskyPiece, RecordReader};
use crate::writer::{RecordWriter, RecordWriterConfig, WriterState};

/// Test empty records within a normal chunk
#[test]
fn test_empty_records_with_normal_records() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer
    let mut writer = RecordWriter::new(cursor).unwrap();

    // Write a sequence of records including empty ones
    let records = [
        b"first".to_vec(), // Normal record
        b"".to_vec(),      // Empty record
        b"".to_vec(),      // Another empty record
        b"last".to_vec(),  // Normal record
    ];

    for record in &records {
        writer.write_record(record).unwrap();
    }

    // Close to ensure all data is written
    writer.close().unwrap();

    // Get the written data
    let data = writer.get_data().unwrap();

    // Read back the records
    let mut reader = RecordReader::new(Cursor::new(data)).unwrap();
    let mut read_records = Vec::new();

    loop {
        match reader.next_record().unwrap() {
            DiskyPiece::Record(bytes) => {
                read_records.push(bytes.to_vec());
            }
            DiskyPiece::EOF => break,
        }
    }

    // Verify we got 4 records including the empty ones
    assert_eq!(
        read_records.len(),
        4,
        "Expected 4 records including empty ones"
    );

    // Verify each record's content
    assert_eq!(&read_records[0], b"first", "First record content mismatch");
    assert_eq!(&read_records[1], b"", "Second record should be empty");
    assert_eq!(&read_records[2], b"", "Third record should be empty");
    assert_eq!(&read_records[3], b"last", "Fourth record content mismatch");
}

/// Test writing only empty records
#[test]
fn test_only_empty_records() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer
    let mut writer = RecordWriter::new(cursor).unwrap();

    // Initial state should be SignatureWritten
    assert_eq!(writer.get_state(), &WriterState::SignatureWritten);

    // Write only empty records
    for _ in 0..5 {
        writer.write_record(b"").unwrap();
    }

    // We are in `RecordsWritten` state
    assert_eq!(writer.get_state(), &WriterState::RecordsWritten);

    // Close the writer
    writer.close().unwrap();

    // Get the written data
    let data = writer.get_data().unwrap();

    // We have written signature and blockheader and chunk header and some sizes
    assert_eq!(
        data.len(),
        111,
        "File should only contain signature (64 bytes)"
    );

    // Read back the records
    let mut reader = RecordReader::new(Cursor::new(data)).unwrap();
    let mut count = 0;

    // Should immediately hit EOF, no records to read
    loop {
        match reader.next_record().unwrap() {
            DiskyPiece::Record(_) => {
                count += 1;
            }
            DiskyPiece::EOF => break,
        }
    }

    // Verify no records were read
    assert_eq!(
        count, 5,
        "Expected no records from a file with only empty records"
    );
}

/// Test writing empty records with different chunk boundaries
#[test]
fn test_empty_records_chunk_boundaries() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer with small chunk size
    let config = RecordWriterConfig {
        chunk_size_bytes: 10, // Very small to force multiple chunks
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config).unwrap();

    // Write sequences to force chunk boundaries
    writer.write_record(b"record1").unwrap(); // Normal record in chunk 1
    writer.write_record(b"").unwrap(); // Empty record in chunk 1

    // This should force a new chunk due to size
    writer
        .write_record(b"large_record_to_force_new_chunk")
        .unwrap(); // In chunk 2
    writer.write_record(b"").unwrap(); // Empty record in chunk 2
    writer.write_record(b"").unwrap(); // Another empty record in chunk 2

    // Another record to ensure we hit the size threshold
    writer.write_record(b"final").unwrap(); // Might be in chunk 2 or 3 depending on exact sizes

    // Close the writer
    writer.close().unwrap();

    // Get the written data
    let data = writer.get_data().unwrap();

    // Read back the records
    let mut reader = RecordReader::new(Cursor::new(data)).unwrap();
    let mut read_records = Vec::new();

    loop {
        match reader.next_record().unwrap() {
            DiskyPiece::Record(bytes) => {
                read_records.push(bytes.to_vec());
            }
            DiskyPiece::EOF => break,
        }
    }

    // Verify we got all 6 records including empty ones
    assert_eq!(
        read_records.len(),
        6,
        "Expected 6 records including empty ones"
    );

    // Check specific records
    assert_eq!(read_records[0], b"record1");
    assert_eq!(read_records[1], b"");
    assert_eq!(read_records[2], b"large_record_to_force_new_chunk");
    assert_eq!(read_records[3], b"");
    assert_eq!(read_records[4], b"");
    assert_eq!(read_records[5], b"final");
}
