// Tests for SimpleChunkParser

use crate::chunks::header_parser::parse_chunk_header;
use crate::chunks::writer::ChunkWriter;
use crate::chunks::SimpleChunkParser;
use crate::chunks::SimpleChunkWriter;
use crate::compression::core::CompressionType;
use crate::error::DiskyError;
use bytes::{Bytes, BytesMut};

// Import RecordResult from the SimpleChunkParser module
use crate::chunks::simple_chunk_parser::RecordResult;

#[test]
fn test_basic_read_write() {
    // Create a chunk with test records
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    writer.write_record(b"Record 2").unwrap();
    writer.write_record(b"Record 3").unwrap();

    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse the header and get the chunk data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // Create a parser
    let mut reader = SimpleChunkParser::new(header, chunk_data).unwrap();

    // Check initial state
    assert_eq!(reader.records_read(), 0);
    assert_eq!(reader.total_records(), 3);
    assert_eq!(reader.compression_type(), CompressionType::None);

    // Read records
    let record1 = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record1, Bytes::from_static(b"Record 1"));

    let record2 = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record2, Bytes::from_static(b"Record 2"));

    let record3 = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record3, Bytes::from_static(b"Record 3"));

    // No more records, should return Done
    let remaining = match reader.next().unwrap() {
        RecordResult::Done(remaining) => remaining,
        _ => panic!("Expected Done"),
    };
    assert_eq!(remaining.len(), 0);
}

#[test]
fn test_multiple_chunks() {
    // Create two chunks
    let mut writer1 = SimpleChunkWriter::new(CompressionType::None);
    writer1.write_record(b"Chunk1 Record1").unwrap();
    writer1.write_record(b"Chunk1 Record2").unwrap();
    let chunk1 = writer1.serialize_chunk().unwrap();

    let mut writer2 = SimpleChunkWriter::new(CompressionType::None);
    writer2.write_record(b"Chunk2 Record1").unwrap();
    writer2.write_record(b"Chunk2 Record2").unwrap();
    writer2.write_record(b"Chunk2 Record3").unwrap();
    let chunk2 = writer2.serialize_chunk().unwrap();

    // Concatenate the chunks
    let mut concatenated = BytesMut::with_capacity(chunk1.len() + chunk2.len());
    concatenated.extend_from_slice(&chunk1);
    concatenated.extend_from_slice(&chunk2);
    let mut combined_chunks = concatenated.freeze();

    // Parse the first chunk header and data
    let header1 = parse_chunk_header(&mut combined_chunks).unwrap();

    // Create parser for the first chunk
    let mut reader1 = SimpleChunkParser::new(header1, combined_chunks).unwrap();

    // Read records from first chunk
    let record1 = match reader1.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record1, Bytes::from_static(b"Chunk1 Record1"));

    let record2 = match reader1.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record2, Bytes::from_static(b"Chunk1 Record2"));

    // Done with first chunk, should return remaining bytes (second chunk)
    let mut remaining_bytes = match reader1.next().unwrap() {
        RecordResult::Done(remaining) => remaining,
        _ => panic!("Expected Done with remaining bytes"),
    };

    // The remaining bytes should be exactly the second chunk
    assert_eq!(remaining_bytes.len(), chunk2.len());

    // Parse the second chunk header and data
    let header2 = parse_chunk_header(&mut remaining_bytes).unwrap();

    // Create parser for the second chunk
    let mut reader2 = SimpleChunkParser::new(header2, remaining_bytes).unwrap();

    // Read records from second chunk
    let record1 = match reader2.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record1, Bytes::from_static(b"Chunk2 Record1"));

    let record2 = match reader2.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record2, Bytes::from_static(b"Chunk2 Record2"));

    let record3 = match reader2.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record3, Bytes::from_static(b"Chunk2 Record3"));

    // Done with second chunk, no more remaining bytes
    let remaining = match reader2.next().unwrap() {
        RecordResult::Done(remaining) => remaining,
        _ => panic!("Expected Done with no remaining bytes"),
    };
    assert_eq!(remaining.len(), 0);
}

#[test]
fn test_empty_chunk() {
    // Create an empty chunk (no records)
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse the header and get the chunk data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // Create a parser
    let mut reader = SimpleChunkParser::new(header, chunk_data).unwrap();

    // Check initial state
    assert_eq!(reader.records_read(), 0);
    assert_eq!(reader.total_records(), 0);

    // No records, should return Done immediately
    let remaining = match reader.next().unwrap() {
        RecordResult::Done(remaining) => remaining,
        _ => panic!("Expected Done"),
    };
    assert_eq!(remaining.len(), 0);
}

#[test]
fn test_large_records() {
    // Create a chunk with one large record
    let mut writer = SimpleChunkWriter::new(CompressionType::None);

    // Create a 1 MB record
    let large_record = vec![0xAA; 1_000_000];
    writer.write_record(&large_record).unwrap();

    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse and read
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();
    let mut reader = SimpleChunkParser::new(header, chunk_data).unwrap();

    // Read the large record
    let record = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };

    assert_eq!(record.len(), 1_000_000);
    assert_eq!(record[0], 0xAA);
    assert_eq!(record[999_999], 0xAA);
}

#[test]
fn test_mixed_record_sizes() {
    // Create a chunk with records of various sizes
    let mut writer = SimpleChunkWriter::new(CompressionType::None);

    // Empty record
    writer.write_record(b"").unwrap();

    // Small record
    writer.write_record(b"Small").unwrap();

    // Medium record (1 KB)
    let medium_record = vec![0xBB; 1024];
    writer.write_record(&medium_record).unwrap();

    // Large record (100 KB)
    let large_record = vec![0xCC; 100_000];
    writer.write_record(&large_record).unwrap();

    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse and read
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();
    let mut reader = SimpleChunkParser::new(header, chunk_data).unwrap();

    // Read and verify each record
    // Empty record
    let empty = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(empty.len(), 0);

    // Small record
    let small = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(small, Bytes::from_static(b"Small"));

    // Medium record
    let medium = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(medium.len(), 1024);
    assert_eq!(medium[0], 0xBB);

    // Large record
    let large = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(large.len(), 100_000);
    assert_eq!(large[0], 0xCC);
}

#[test]
fn test_incomplete_chunk_data() {
    // Create a valid writer
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test").unwrap();
    let full_chunk = writer.serialize_chunk().unwrap();

    // Parse header
    let mut chunk_data = full_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // Truncate the data to simulate incomplete chunk
    let truncated_data = chunk_data.slice(0..5); // Not enough data

    // Try to create a parser with truncated data
    let result = SimpleChunkParser::new(header, truncated_data);

    // Should fail with appropriate error
    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::Other(msg) => {
                assert!(msg.contains("Chunk data incomplete"));
            }
            _ => panic!("Expected Other error"),
        }
    }
}

#[test]
fn test_iteration_pattern() {
    // Create a chunk with a few records
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    let expected_records = vec![
        b"First record".to_vec(),
        b"Second record".to_vec(),
        b"Third record".to_vec(),
    ];

    for record in &expected_records {
        writer.write_record(record).unwrap();
    }

    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse the header and data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();
    let mut reader = SimpleChunkParser::new(header, chunk_data).unwrap();

    // Iterate over records using a common pattern
    let mut actual_records = Vec::new();

    loop {
        match reader.next().unwrap() {
            RecordResult::Record(record) => {
                actual_records.push(record.to_vec());
            }
            RecordResult::Done(_) => {
                break;
            }
        }
    }

    // Verify all records were read correctly
    assert_eq!(actual_records.len(), expected_records.len());
    for (i, (actual, expected)) in actual_records
        .iter()
        .zip(expected_records.iter())
        .enumerate()
    {
        assert_eq!(actual, expected, "Record {} mismatch", i);
    }
}

#[test]
fn test_with_trailing_data() {
    // Create a chunk with a record
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let chunk = writer.serialize_chunk().unwrap();

    // Add some arbitrary trailing data
    let trailing_data = b"This is trailing data not part of any chunk";
    let mut combined = BytesMut::with_capacity(chunk.len() + trailing_data.len());
    combined.extend_from_slice(&chunk);
    combined.extend_from_slice(trailing_data);

    // Parse the header and data
    let mut data = combined.freeze();
    let header = parse_chunk_header(&mut data).unwrap();
    let mut reader = SimpleChunkParser::new(header, data).unwrap();

    // Read the record
    let record = match reader.next().unwrap() {
        RecordResult::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record, Bytes::from_static(b"Test record"));

    // Done should return the trailing data
    let remaining = match reader.next().unwrap() {
        RecordResult::Done(remaining) => remaining,
        _ => panic!("Expected Done with remaining bytes"),
    };
    assert_eq!(remaining, Bytes::from_static(trailing_data));
}

#[test]
fn test_records_read_counter() {
    // Create a chunk with several records
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    for i in 0..5 {
        writer
            .write_record(format!("Record {}", i).as_bytes())
            .unwrap();
    }

    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse the header and data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();
    let mut reader = SimpleChunkParser::new(header, chunk_data).unwrap();

    // Initially 0 records read
    assert_eq!(reader.records_read(), 0);

    // Read records one by one and check counter
    for i in 1..=5 {
        let _ = reader.next().unwrap();
        assert_eq!(reader.records_read(), i);
    }

    // Reading Done shouldn't increment counter
    let _ = reader.next().unwrap();
    assert_eq!(reader.records_read(), 5);
}

