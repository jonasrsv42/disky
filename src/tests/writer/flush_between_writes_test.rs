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

//! Tests for writing records with intermediate flushes.

use bytes::Bytes;
use std::io::Cursor;

use crate::compression::CompressionType;
use crate::reader::RecordReader;
use crate::writer::{RecordWriter, RecordWriterConfig, WriterState};

/// Test writing records, flushing, then writing more records.
///
/// This verifies that the writer correctly handles the intermediate flush state
/// and can write more records after flushing without losing data or causing corruption.
#[test]
fn test_write_flush_write() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer with fixed compression type for consistent output
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config).unwrap();

    // Write first batch of records
    let batch1 = vec![
        b"record_1".to_vec(),
        b"record_2".to_vec(),
        b"record_3".to_vec(),
    ];

    for record in &batch1 {
        writer.write_record(record).unwrap();
    }

    // After writing, state should be RecordsWritten
    assert_eq!(writer.get_state(), &WriterState::RecordsWritten);

    // Flush after writing the first batch
    writer.flush().unwrap();

    // After flushing, state should be Flushed
    assert_eq!(writer.get_state(), &WriterState::Flushed);

    // Write second batch of records
    let batch2 = vec![b"record_4".to_vec(), b"record_5".to_vec()];

    for record in &batch2 {
        writer.write_record(record).unwrap();
    }

    // After writing again, state should be RecordsWritten
    assert_eq!(writer.get_state(), &WriterState::RecordsWritten);

    // Close the writer to ensure all data is written
    writer.close().unwrap();

    // After closing, state should be Closed
    assert_eq!(writer.get_state(), &WriterState::Closed);

    // Get the written data
    let data = writer.get_data().unwrap();

    // Create a reader to verify all records were written correctly
    let cursor = Cursor::new(&data);
    let reader = RecordReader::new(cursor).unwrap();

    // Read back and verify all records
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify we got all 5 records with the expected data
    assert_eq!(records.len(), 5);
    assert_eq!(&records[0][..], b"record_1");
    assert_eq!(&records[1][..], b"record_2");
    assert_eq!(&records[2][..], b"record_3");
    assert_eq!(&records[3][..], b"record_4");
    assert_eq!(&records[4][..], b"record_5");
}

/// Test multiple flush cycles with writes in between.
///
/// This test writes records, flushes, writes more records, flushes again,
/// and repeats several times to verify that the writer can handle
/// multiple cycles of writing and flushing.
#[test]
fn test_multiple_flush_cycles() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer with fixed compression type for consistent output
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        chunk_size_bytes: 1024, // Ensure all records can fit in a single chunk if needed
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config).unwrap();

    // Write records with multiple flush cycles
    for i in 0..5 {
        // Write a batch of 3 records
        for j in 0..3 {
            let record_id = i * 3 + j;
            let record = format!("record_{}", record_id);
            writer.write_record(record.as_bytes()).unwrap();

            // After writing, state should be RecordsWritten
            assert_eq!(writer.get_state(), &WriterState::RecordsWritten);
        }

        // Flush after each batch
        writer.flush().unwrap();

        // After flushing, state should be Flushed
        assert_eq!(writer.get_state(), &WriterState::Flushed);
    }

    // Close the writer to ensure all data is written
    writer.close().unwrap();

    // After closing, state should be Closed
    assert_eq!(writer.get_state(), &WriterState::Closed);

    // Get the written data
    let data = writer.get_data().unwrap();

    // Create a reader to verify all records were written correctly
    let cursor = Cursor::new(&data);
    let reader = RecordReader::new(cursor).unwrap();

    // Read back and verify all records
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify we got all 15 records (5 batches of 3 records each)
    assert_eq!(records.len(), 15);

    // Verify the content of each record
    for i in 0..15 {
        let expected = format!("record_{}", i);
        assert_eq!(&records[i][..], expected.as_bytes());
    }
}

/// Test flushing with empty records between flushes.
///
/// This test writes empty records, flushes, then writes more empty records
/// to ensure that empty records are handled correctly with intermediate flushes.
#[test]
fn test_empty_records_with_flush() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer with fixed compression type for consistent output
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config).unwrap();

    // Write a normal record
    writer.write_record(b"normal_record").unwrap();

    // Write two empty records
    writer.write_record(b"").unwrap();
    writer.write_record(b"").unwrap();

    // Flush after the first batch
    writer.flush().unwrap();

    // Write another normal record
    writer.write_record(b"another_record").unwrap();

    // Write two more empty records
    writer.write_record(b"").unwrap();
    writer.write_record(b"").unwrap();

    // Close the writer to ensure all data is written
    writer.close().unwrap();

    // Get the written data
    let data = writer.get_data().unwrap();

    // Create a reader to verify all records were written correctly
    let cursor = Cursor::new(&data);
    let reader = RecordReader::new(cursor).unwrap();

    // Read back and verify all records
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify we got all 6 records (2 normal, 4 empty)
    assert_eq!(records.len(), 6);

    // Verify record content
    assert_eq!(&records[0][..], b"normal_record");
    assert_eq!(&records[1][..], b"");
    assert_eq!(&records[2][..], b"");
    assert_eq!(&records[3][..], b"another_record");
    assert_eq!(&records[4][..], b"");
    assert_eq!(&records[5][..], b"");
}

/// Test flush_chunk vs flush behavior with multiple writes.
///
/// This test compares the behavior of flush_chunk and flush,
/// verifying that both methods allow writing more records afterward.
#[test]
fn test_flush_chunk_vs_flush() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());

    // Create a writer with fixed compression type for consistent output
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config).unwrap();

    // Write first record
    writer.write_record(b"first_record").unwrap();

    // Use flush_chunk directly
    writer.flush_chunk().unwrap();

    // After flush_chunk, state should be Flushed
    assert_eq!(writer.get_state(), &WriterState::Flushed);

    // Write second record
    writer.write_record(b"second_record").unwrap();

    // Use flush (which calls flush_chunk internally)
    writer.flush().unwrap();

    // After flush, state should be Flushed
    assert_eq!(writer.get_state(), &WriterState::Flushed);

    // Write third record
    writer.write_record(b"third_record").unwrap();

    // Close the writer to ensure all data is written
    writer.close().unwrap();

    // Get the written data
    let data = writer.get_data().unwrap();

    // Create a reader to verify all records were written correctly
    let cursor = Cursor::new(&data);
    let reader = RecordReader::new(cursor).unwrap();

    // Read back and verify all records
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify we got all 3 records
    assert_eq!(records.len(), 3);
    assert_eq!(&records[0][..], b"first_record");
    assert_eq!(&records[1][..], b"second_record");
    assert_eq!(&records[2][..], b"third_record");
}
