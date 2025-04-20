// Tests for SimpleChunkParser

use crate::chunks::header_parser::parse_chunk_header;
use crate::chunks::writer::ChunkWriter;
use crate::chunks::SimpleChunkParser;
use crate::chunks::SimpleChunkWriter;
use crate::compression::core::CompressionType;
use crate::error::DiskyError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::hash::highway_hash;

// Import SimpleChunkPiece from the SimpleChunkParser module
use crate::chunks::simple_chunk_parser::SimpleChunkPiece;

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

    // Create a parser with mutable reference to chunk_data
    let mut reader = SimpleChunkParser::new(header, chunk_data.clone()).unwrap();

    // Check initial state
    assert_eq!(reader.records_read(), 0);
    assert_eq!(reader.total_records(), 3);
    assert_eq!(reader.compression_type(), CompressionType::None);

    // Read records
    let record1 = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record1, Bytes::from_static(b"Record 1"));

    let record2 = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record2, Bytes::from_static(b"Record 2"));

    let record3 = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record3, Bytes::from_static(b"Record 3"));

    // No more records, should return EndOfChunk and have advanced the buffer
    match reader.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    };

    // Calling next() again after EndOfChunk should return an error
    let error_result = reader.next();
    assert!(
        error_result.is_err(),
        "Expected an error when calling next() after EndOfChunk"
    );
    if let Err(err) = error_result {
        match err {
            DiskyError::Other(msg) => {
                assert!(msg.contains(
                    "Cannot invoke next() after SimpleChunkParser has returned EndOfChunk"
                ));
            }
            _ => panic!("Expected Other error"),
        }
    }

    chunk_data.advance(reader.header().data_size as usize);

    // Now we can use chunk_data again
    assert_eq!(
        chunk_data.remaining(),
        0,
        "Buffer should have been fully advanced"
    );
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

    // Store original length to verify advancement later
    let _combined_length = combined_chunks.len();

    // Parse the first chunk header
    let header1 = parse_chunk_header(&mut combined_chunks).unwrap();

    // Create parser for the first chunk - it gets a mutable reference to the buffer
    let mut reader1 = SimpleChunkParser::new(header1, combined_chunks.clone()).unwrap();

    // Read records from first chunk
    let record1 = match reader1.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record1, Bytes::from_static(b"Chunk1 Record1"));

    let record2 = match reader1.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record2, Bytes::from_static(b"Chunk1 Record2"));

    // Done with first chunk, should advance the buffer past the first chunk
    match reader1.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    };

    combined_chunks.advance(reader1.header().data_size as usize);

    // The buffer should now be positioned at the start of the second chunk
    assert_eq!(
        combined_chunks.remaining(),
        chunk2.len(),
        "Buffer should be advanced past first chunk"
    );

    // Parse the second chunk header
    let header2 = parse_chunk_header(&mut combined_chunks).unwrap();

    // Create parser for the second chunk
    let mut reader2 = SimpleChunkParser::new(header2, combined_chunks.clone()).unwrap();

    // Read records from second chunk
    let record1 = match reader2.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record1, Bytes::from_static(b"Chunk2 Record1"));

    let record2 = match reader2.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record2, Bytes::from_static(b"Chunk2 Record2"));

    let record3 = match reader2.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record3, Bytes::from_static(b"Chunk2 Record3"));

    // Done with second chunk, should be end of buffer
    match reader2.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    };

    combined_chunks.advance(reader2.header().data_size as usize);

    // Buffer should be fully advanced now
    assert_eq!(
        combined_chunks.remaining(),
        0,
        "Buffer should be fully advanced after reading all chunks"
    );
}

#[test]
fn test_empty_chunk() {
    // Create an empty chunk (no records)
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse the header and get the chunk data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // Store the initial length to verify advancement
    let _initial_length = chunk_data.len();

    // Create a parser
    let mut reader = SimpleChunkParser::new(header, chunk_data.clone()).unwrap();

    // Check initial state
    assert_eq!(reader.records_read(), 0);
    assert_eq!(reader.total_records(), 0);

    // No records, should return EndOfChunk immediately
    match reader.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    };

    chunk_data.advance(reader.header().data_size as usize);

    // Verify buffer was advanced past the chunk
    assert_eq!(
        chunk_data.remaining(),
        0,
        "Buffer should be fully advanced after EndOfChunk"
    );
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

    let mut reader = SimpleChunkParser::new(header, chunk_data.clone()).unwrap();

    // Read the large record
    let record = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };

    assert_eq!(record.len(), 1_000_000);
    assert_eq!(record[0], 0xAA);
    assert_eq!(record[999_999], 0xAA);

    // Verify EndOfChunk and buffer advancement
    match reader.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    };

    chunk_data.advance(reader.header().data_size as usize);
    // Buffer should be fully advanced
    assert_eq!(chunk_data.remaining(), 0, "Buffer should be fully advanced");
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

    let mut reader = SimpleChunkParser::new(header, chunk_data.clone()).unwrap();

    // Read and verify each record
    // Empty record
    let empty = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(empty.len(), 0);

    // Small record
    let small = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(small, Bytes::from_static(b"Small"));

    // Medium record
    let medium = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(medium.len(), 1024);
    assert_eq!(medium[0], 0xBB);

    // Large record
    let large = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(large.len(), 100_000);
    assert_eq!(large[0], 0xCC);

    // Verify EndOfChunk and buffer advancement
    match reader.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    };

    chunk_data.advance(reader.header().data_size as usize);

    // Buffer should be fully advanced
    assert_eq!(chunk_data.remaining(), 0, "Buffer should be fully advanced");
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

    // Use a block scope for the reader
    let actual_records = {
        let mut reader = SimpleChunkParser::new(header, chunk_data.clone()).unwrap();
        let mut records = Vec::new();

        // Iterate over records using a common pattern
        loop {
            match reader.next().unwrap() {
                SimpleChunkPiece::Record(record) => {
                    records.push(record.to_vec());
                }
                SimpleChunkPiece::EndOfChunk => {
                    break;
                }
            }
        }

        chunk_data.advance(reader.header().data_size as usize);
        records
    }; // reader is dropped here, releasing mutable borrow

    // Verify all records were read correctly
    assert_eq!(actual_records.len(), expected_records.len());
    for (i, (actual, expected)) in actual_records
        .iter()
        .zip(expected_records.iter())
        .enumerate()
    {
        assert_eq!(actual, expected, "Record {} mismatch", i);
    }

    // Buffer should be fully advanced
    assert_eq!(chunk_data.remaining(), 0, "Buffer should be fully advanced");
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

    // Get the size of just the chunk (for debugging if needed)
    let _chunk_size = chunk.len();

    // Parse the header and data
    let mut data = combined.freeze();
    let header = parse_chunk_header(&mut data).unwrap();

    let mut reader = SimpleChunkParser::new(header, data.clone()).unwrap();

    // Read the record
    let record = match reader.next().unwrap() {
        SimpleChunkPiece::Record(record) => record,
        _ => panic!("Expected Record"),
    };
    assert_eq!(record, Bytes::from_static(b"Test record"));

    // EndOfChunk should advance the buffer past the chunk
    match reader.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    };

    data.advance(reader.header().data_size as usize);

    // Buffer should now point to the trailing data
    assert_eq!(
        data.remaining(),
        trailing_data.len(),
        "Buffer should be advanced past chunk but not trailing data"
    );

    // Copy the remaining data and verify it matches the trailing data
    let remaining_bytes = data.slice(0..data.remaining());
    assert_eq!(
        remaining_bytes,
        Bytes::from_static(trailing_data),
        "Remaining buffer should contain trailing data"
    );
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

    let mut reader = SimpleChunkParser::new(header, chunk_data.clone()).unwrap();

    // Initially 0 records read
    assert_eq!(reader.records_read(), 0);

    // Read records one by one and check counter
    for i in 1..=5 {
        let result = reader.next().unwrap();
        match result {
            SimpleChunkPiece::Record(_) => {
                assert_eq!(reader.records_read(), i);
            }
            SimpleChunkPiece::EndOfChunk => {
                panic!("Unexpected EndOfChunk before all records are read");
            }
        }
    }

    // Reading EndOfChunk shouldn't increment counter
    match reader.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {}
        _ => panic!("Expected EndOfChunk"),
    }
    assert_eq!(reader.records_read(), 5);

    chunk_data.advance(reader.header().data_size as usize);

    // Buffer should be fully advanced
    assert_eq!(chunk_data.remaining(), 0, "Buffer should be fully advanced");
}

#[test]
fn test_invalid_chunk_type() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header and modify it to have an invalid chunk type
    let mut chunk_data = serialized_chunk.clone();
    let mut header = parse_chunk_header(&mut chunk_data).unwrap();
    header.chunk_type = crate::chunks::header::ChunkType::Padding; // Not a SimpleRecords type

    // Try to create parser with invalid chunk type
    let result = SimpleChunkParser::new(header, chunk_data);

    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::Other(msg) => {
                assert!(msg.contains("Expected simple records chunk"));
            }
            _ => panic!("Expected Other error"),
        }
    }
}

#[test]
fn test_valid_data_hash() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse the header and data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // This should succeed because the hash in the header matches the data
    let result = SimpleChunkParser::new(header, chunk_data);
    assert!(result.is_ok(), "Parser creation should succeed with valid hash");
}

#[test]
fn test_invalid_data_hash() {
    
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header
    let mut chunk_data = serialized_chunk.clone();
    let mut header = parse_chunk_header(&mut chunk_data).unwrap();
    
    // Modify the data hash to an incorrect value
    header.data_hash = header.data_hash ^ 0xDEADBEEF; // XOR to change the hash

    // Try to create parser with invalid hash
    let result = SimpleChunkParser::new(header, chunk_data);

    // Should fail due to hash mismatch
    assert!(result.is_err(), "Parser creation should fail with invalid hash");
    if let Err(err) = result {
        match err {
            DiskyError::Corruption(msg) => {
                assert!(msg.contains("Chunk data hash mismatch"));
            }
            _ => panic!("Expected Corruption error, got: {:?}", err),
        }
    }
}

#[test]
fn test_modified_data_hash_verification() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();
    
    // Create a modified copy of the chunk data
    let mut modified_data = chunk_data.clone();
    
    // Modify one byte of the data (the compression type byte is easiest to change)
    if modified_data.len() > 0 {
        let mut bytes_mut = BytesMut::with_capacity(modified_data.len());
        bytes_mut.extend_from_slice(&modified_data);
        bytes_mut[0] = bytes_mut[0] ^ 0x01; // Toggle one bit in the first byte
        modified_data = bytes_mut.freeze();
    }

    // Try to create parser with modified data (should fail hash verification)
    let result = SimpleChunkParser::new(header, modified_data);

    // Should fail due to hash mismatch
    assert!(result.is_err(), "Parser creation should fail with modified data");
    if let Err(err) = result {
        match err {
            DiskyError::Corruption(msg) => {
                assert!(msg.contains("Chunk data hash mismatch"));
            }
            _ => panic!("Expected Corruption error, got: {:?}", err),
        }
    }
}

#[test]
fn test_empty_chunk_data() {
    // Create a valid header but empty data
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header but provide empty data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // Try to create parser with empty data
    let empty_bytes = Bytes::new();
    let result = SimpleChunkParser::new(header, empty_bytes);

    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::Other(msg) => {
                assert!(
                    msg.contains("Chunk data incomplete") || msg.contains("Chunk data is empty")
                );
            }
            _ => panic!("Expected Other error"),
        }
    }
}

#[test]
fn test_corrupted_sizes_length() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // Corrupt the data by modifying the sizes length varint
    // We know the first byte is compression type, skip it
    let mut corrupted_data = BytesMut::new();
    corrupted_data.extend_from_slice(&chunk_data[0..1]); // Keep compression type
    corrupted_data.put_u8(0xFF); // Invalid varint (will keep reading)
    corrupted_data.put_u8(0xFF); // Invalid varint continuation
                                 // Add rest of data
    if chunk_data.len() > 3 {
        corrupted_data.extend_from_slice(&chunk_data[3..]);
    }

    // Convert to Bytes and make mutable
    let corrupted_bytes = corrupted_data.freeze();

    // Try to create a parser with corrupted data
    let result = SimpleChunkParser::new(header, corrupted_bytes);

    assert!(result.is_err());
}

#[test]
fn test_invalid_sizes_length() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header
    let mut chunk_data = serialized_chunk.clone();
    let mut header = parse_chunk_header(&mut chunk_data).unwrap();

    // Corrupt the data by modifying the sizes length to be larger than available data
    let mut corrupted_data = BytesMut::new();
    corrupted_data.extend_from_slice(&chunk_data[0..1]); // Keep compression type
                                                         // Insert a varint that's too large (larger than remaining data)
    corrupted_data.put_u8(0x80); // Start of varint
    corrupted_data.put_u8(0x80); // Continuation
    corrupted_data.put_u8(0x04); // End of varint, value too large

    // Add rest of data
    if chunk_data.len() > 4 {
        corrupted_data.extend_from_slice(&chunk_data[4..]);
    }

    // Calculate the hash of the corrupted data to pass hash verification
    let corrupted_bytes = corrupted_data.freeze();
    let data_hash = highway_hash(&corrupted_bytes);
    
    // Update the header with the new hash
    header.data_hash = data_hash;

    // Try to create a parser with corrupted data
    let result = SimpleChunkParser::new(header, corrupted_bytes);

    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::Corruption(msg) => {
                assert!(msg.contains("Sizes data length"));
            }
            _ => panic!("Expected Corruption error"),
        }
    }
}

#[test]
fn test_unsupported_compression_type() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header
    let mut chunk_data = serialized_chunk.clone();
    let mut header = parse_chunk_header(&mut chunk_data).unwrap();

    // Modify the compression type byte to an unsupported value
    let mut modified_data = BytesMut::new();
    modified_data.put_u8(0x99); // Invalid compression type
    if chunk_data.len() > 1 {
        modified_data.extend_from_slice(&chunk_data[1..]);
    }

    // Calculate hash for the modified data
    let modified_bytes = modified_data.freeze();
    let data_hash = highway_hash(&modified_bytes);
    
    // Update the header with the correct hash for the modified data
    header.data_hash = data_hash;

    // Try to create a parser with invalid compression type
    let result = SimpleChunkParser::new(header, modified_bytes);

    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::UnsupportedCompressionType(t) => {
                assert_eq!(t, 0x99);
            }
            _ => panic!("Expected UnsupportedCompressionType error"),
        }
    }
}

#[test]
fn test_reading_after_error() {
    // Create a chunk with some records
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    writer.write_record(b"Record 2").unwrap();
    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse header
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();
    let mut reader = SimpleChunkParser::new(header, chunk_data).unwrap();

    // Read one record successfully
    let _ = reader.next().unwrap();

    // Read second record
    let record = reader.next();
    assert!(record.is_ok()); // Second record should be ok

    // We've read both records at this point, so we should be done
    // Let's check that we get the EndOfChunk state
    match reader.next().unwrap() {
        SimpleChunkPiece::EndOfChunk => {
            // Now try calling next again - should return an error
            let error_result = reader.next();
            assert!(
                error_result.is_err(),
                "Expected an error when calling next() after EndOfChunk"
            );
            if let Err(err) = error_result {
                match err {
                    DiskyError::Other(msg) => {
                        assert!(msg.contains(
                            "Cannot invoke next() after SimpleChunkParser has returned EndOfChunk"
                        ));
                    }
                    _ => panic!("Expected Other error"),
                }
            }
        }
        _ => panic!("Expected EndOfChunk after reading all records"),
    }
}

#[test]
fn test_record_size_exceeds_available_data() {
    // Create a chunk with test records
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    writer.write_record(b"Record 2").unwrap();

    let serialized_chunk = writer.serialize_chunk().unwrap();

    // Parse the header and get the chunk data
    let mut chunk_data = serialized_chunk.clone();
    let header = parse_chunk_header(&mut chunk_data).unwrap();

    // For this test, we'll modify an existing chunk:
    // 1. Create a valid chunk but modify its data size to be much larger

    // Make a small buffer with just enough data to pass creation
    // but will fail on record read due to size mismatch
    let mut minimal_chunk = BytesMut::new();

    // Compression type - None (0)
    minimal_chunk.put_u8(0);

    // Create a small sizes section - 2 bytes, containing one large size
    minimal_chunk.put_u8(2); // Size of sizes section

    // Put a size that's larger than our data section will be
    minimal_chunk.put_u8(0xFF); // Start of large varint - 255 bytes
    minimal_chunk.put_u8(0x01); // 255 + 128 = 383 bytes (much larger than our data)

    // Add just a few bytes of data section
    minimal_chunk.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05]);

    // Calculate hash for our custom chunk data
    let chunk_bytes = minimal_chunk.freeze();
    let data_hash = highway_hash(&chunk_bytes);

    // We need a header with matching data_size and correct hash
    let mut custom_header = header.clone();
    custom_header.data_size = chunk_bytes.len() as u64;
    custom_header.data_hash = data_hash;

    // Create parser with custom data
    let mut reader = SimpleChunkParser::new(custom_header, chunk_bytes).unwrap();

    // First read should fail because record size is too large
    let result = reader.next();
    assert!(result.is_err());

    if let Err(err) = result {
        match err {
            DiskyError::UnexpectedEndOfChunk(msg) => {
                assert!(msg.contains("Record extends beyond data boundary"));
            }
            _ => panic!("Expected Corruption error, got: {:?}", err),
        }
    }
}
