// Tests for ChunksParser

use crate::chunks::chunks_parser::{Chunk, ChunksParser};
use crate::chunks::header::{ChunkHeader, ChunkType};
use crate::chunks::header_writer::write_chunk_header;
use crate::chunks::signature::FILE_SIGNATURE_HEADER;
use crate::chunks::writer::ChunkWriter;
use crate::chunks::SimpleChunkWriter;
use crate::compression::core::CompressionType;
use crate::error::DiskyError;
use bytes::{Bytes, BytesMut, BufMut};

#[test]
fn test_parse_signature_chunk() {
    // Create a signature chunk by using the signature header
    let signature_chunk = Bytes::copy_from_slice(&FILE_SIGNATURE_HEADER);
    
    // Create parser
    let mut parser = ChunksParser::new(signature_chunk);
    
    // Parse and verify it's a signature chunk
    match parser.next_chunk().unwrap() {
        Chunk::Signature => {
            // Success - it's a signature chunk
        },
        other => panic!("Expected Signature chunk, got {:?}", other),
    }
}

#[test]
fn test_parse_simple_records_chunk() {
    // Create a simple records chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    writer.write_record(b"Record 2").unwrap();
    let chunk = writer.serialize_chunk().unwrap();
    
    // Create parser
    let mut parser = ChunksParser::new(chunk);
    
    // Parse and verify it's a simple records chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            assert_eq!(simple_chunk.record_count(), 2);
            assert_eq!(simple_chunk.decoded_size(), b"Record 1".len() as u64 + b"Record 2".len() as u64);
            
            // Convert to iterator and read records
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 2);
            assert_eq!(records[0], Bytes::from_static(b"Record 1"));
            assert_eq!(records[1], Bytes::from_static(b"Record 2"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_parse_padding_chunk() {
    // Create a padding chunk header
    let header = ChunkHeader::new(
        10, // data_size
        0,  // data_hash
        ChunkType::Padding, // chunk_type
        0,  // num_records
        0   // decoded_data_size
    );
    
    let header_bytes = write_chunk_header(&header).unwrap();
    
    // Create padding data (10 bytes as specified in data_size)
    let mut chunk_data = BytesMut::with_capacity(header_bytes.len() + 10);
    chunk_data.extend_from_slice(&header_bytes);
    chunk_data.extend_from_slice(&[0u8; 10]); // 10 bytes of zeros
    
    // Create parser
    let mut parser = ChunksParser::new(chunk_data.freeze());
    
    // Parsing a padding chunk should return an error since it's not implemented yet
    let result = parser.next_chunk();
    assert!(result.is_err());
    if let Err(DiskyError::Other(msg)) = result {
        assert!(msg.contains("Padding chunk parsing not yet implemented"));
    } else {
        panic!("Expected Other error");
    }
    
    // However, we should be able to recover to the next chunk using recover_to_next_chunk
    // In this case, there's no next chunk so it should error
    let result = parser.recover_to_next_chunk();
    assert!(result.is_err());
}

#[test]
fn test_multiple_chunks() {
    // Create a signature chunk
    let signature_chunk = Bytes::copy_from_slice(&FILE_SIGNATURE_HEADER);
    
    // Create a simple records chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    writer.write_record(b"Record 2").unwrap();
    let simple_chunk = writer.serialize_chunk().unwrap();
    
    // Create a second records chunk
    let mut writer2 = SimpleChunkWriter::new(CompressionType::None);
    writer2.write_record(b"Record 3").unwrap();
    let simple_chunk2 = writer2.serialize_chunk().unwrap();
    
    // Combine all chunks
    let mut combined = BytesMut::with_capacity(
        signature_chunk.len() + simple_chunk.len() + simple_chunk2.len()
    );
    combined.extend_from_slice(&signature_chunk);
    combined.extend_from_slice(&simple_chunk);
    combined.extend_from_slice(&simple_chunk2);
    
    // Create parser for the combined stream
    let mut parser = ChunksParser::new(combined.freeze());
    
    // Parse and verify first chunk (signature)
    match parser.next_chunk().unwrap() {
        Chunk::Signature => {
            // Success - it's a signature chunk
        },
        other => panic!("Expected Signature chunk, got {:?}", other),
    }
    
    // Parse and verify second chunk (simple records)
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            assert_eq!(simple_chunk.record_count(), 2);
            
            // Convert to iterator and read records
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 2);
            assert_eq!(records[0], Bytes::from_static(b"Record 1"));
            assert_eq!(records[1], Bytes::from_static(b"Record 2"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
    
    // Parse and verify third chunk (another simple records chunk)
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            assert_eq!(simple_chunk.record_count(), 1);
            
            // Convert to iterator and read records
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 1);
            assert_eq!(records[0], Bytes::from_static(b"Record 3"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_empty_buffer() {
    // Create an empty buffer
    let empty_buffer = Bytes::new();
    
    // Create parser with empty buffer
    let mut parser = ChunksParser::new(empty_buffer);
    
    // Trying to parse should return UnexpectedEof
    let result = parser.next_chunk();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), DiskyError::UnexpectedEof));
}

#[test]
fn test_truncated_header() {
    // Create a partial header (less than CHUNK_HEADER_SIZE)
    let partial_header = Bytes::from_static(&FILE_SIGNATURE_HEADER[0..20]); // Just 20 bytes
    
    // Create parser
    let mut parser = ChunksParser::new(partial_header);
    
    // Trying to parse should return UnexpectedEof
    let result = parser.next_chunk();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), DiskyError::UnexpectedEof));
}

#[test]
fn test_truncated_chunk_data() {
    // Create a simple records chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    writer.write_record(b"Record 2").unwrap();
    let full_chunk = writer.serialize_chunk().unwrap();
    
    // Truncate the chunk data (keep header but not enough data)
    let truncated_size = 45; // Header is 40 bytes, add just a few bytes of data
    let truncated_chunk = full_chunk.slice(0..truncated_size);
    
    // Create parser
    let mut parser = ChunksParser::new(truncated_chunk);
    
    // Parsing should fail with a corruption error about insufficient data
    let result = parser.next_chunk();
    assert!(result.is_err());
    match result.unwrap_err() {
        DiskyError::Corruption(msg) => {
            assert!(msg.contains("Chunk data is smaller than expected"));
        },
        other => panic!("Expected Corruption error, got: {:?}", other),
    }
}

#[test]
fn test_corrupted_header() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    let valid_chunk = writer.serialize_chunk().unwrap();
    
    // Corrupt the header hash
    let mut corrupted = BytesMut::with_capacity(valid_chunk.len());
    corrupted.extend_from_slice(&[0u8; 8]); // Invalid header hash
    corrupted.extend_from_slice(&valid_chunk.slice(8..)); // Rest of chunk
    
    // Create parser
    let mut parser = ChunksParser::new(corrupted.freeze());
    
    // Parsing should fail with ChunkHeaderHashMismatch
    let result = parser.next_chunk();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), DiskyError::ChunkHeaderHashMismatch));
}

#[test]
fn test_unknown_chunk_type() {
    // Create a valid chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Record 1").unwrap();
    let valid_chunk = writer.serialize_chunk().unwrap();
    
    // Extract header and modify chunk type to an unknown value
    let mut chunk_data = valid_chunk.clone();
    let original_header = crate::chunks::header_parser::parse_chunk_header(&mut chunk_data).unwrap();
    
    // Recreate header with invalid chunk type byte (0x99)
    let mut invalid_header = BytesMut::with_capacity(40);
    // Skip header hash (first 8 bytes) for now
    invalid_header.extend_from_slice(&[0u8; 8]); // Placeholder for header hash
    invalid_header.put_u64_le(original_header.data_size); // data_size
    invalid_header.put_u64_le(original_header.data_hash); // data_hash
    invalid_header.put_u8(0x99); // Invalid chunk type
    
    // Copy remaining header fields (num_records and decoded_data_size)
    invalid_header.extend_from_slice(&valid_chunk[25..40]);
    
    // Calculate correct header hash for modified header
    let header_bytes = invalid_header.freeze();
    let hash = crate::hash::highway_hash(&header_bytes.slice(8..));
    
    // Update header hash
    let mut final_header = BytesMut::with_capacity(40);
    final_header.put_u64_le(hash); // Correct header hash
    final_header.extend_from_slice(&header_bytes.slice(8..)); // Rest of modified header
    
    // Create full corrupted chunk
    let mut corrupted_chunk = BytesMut::with_capacity(40 + chunk_data.len());
    corrupted_chunk.extend_from_slice(&final_header);
    corrupted_chunk.extend_from_slice(&chunk_data);
    
    // Create parser
    let mut parser = ChunksParser::new(corrupted_chunk.freeze());
    
    // Parsing should fail with UnknownChunkType
    let result = parser.next_chunk();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), DiskyError::UnknownChunkType(0x99)));
}

#[test]
fn test_consume_buffer() {
    // Create two chunks
    let mut writer1 = SimpleChunkWriter::new(CompressionType::None);
    writer1.write_record(b"Record 1").unwrap();
    let chunk1 = writer1.serialize_chunk().unwrap();
    
    let mut writer2 = SimpleChunkWriter::new(CompressionType::None);
    writer2.write_record(b"Record 2").unwrap();
    let chunk2 = writer2.serialize_chunk().unwrap();
    
    // Combine the chunks
    let mut combined = BytesMut::with_capacity(chunk1.len() + chunk2.len());
    combined.extend_from_slice(&chunk1);
    combined.extend_from_slice(&chunk2);
    
    // Create parser
    let mut parser = ChunksParser::new(combined.freeze());
    
    // Parse first chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            // Convert to iterator and read record
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 1);
            assert_eq!(records[0], Bytes::from_static(b"Record 1"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
    
    // Parse second chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            // Convert to iterator and read record
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 1);
            assert_eq!(records[0], Bytes::from_static(b"Record 2"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_zero_records() {
    // Create a chunk with zero records
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    let chunk = writer.serialize_chunk().unwrap();
    
    // Create parser
    let mut parser = ChunksParser::new(chunk);
    
    // Parse and verify
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            assert_eq!(simple_chunk.record_count(), 0);
            assert_eq!(simple_chunk.decoded_size(), 0);
            
            // Convert to iterator - should be empty
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 0);
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_parse_supported_chunk_types() {
    // Create supported chunk types
    
    // 1. Signature chunk
    let signature_chunk = Bytes::copy_from_slice(&FILE_SIGNATURE_HEADER);
    
    // 2. Simple records chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let records_chunk = writer.serialize_chunk().unwrap();
    
    // Create parsers for each type
    let mut signature_parser = ChunksParser::new(signature_chunk);
    let mut records_parser = ChunksParser::new(records_chunk);
    
    // Verify each parser produces the expected chunk type
    assert!(matches!(signature_parser.next_chunk().unwrap(), Chunk::Signature));
    assert!(matches!(records_parser.next_chunk().unwrap(), Chunk::SimpleRecords(_)));
}

#[test]
fn test_padding_chunk_error_and_recovery() {
    // Create a padding chunk
    let padding_header = ChunkHeader::new(
        5, // data_size
        0, // data_hash
        ChunkType::Padding, // chunk_type
        0, // num_records
        0  // decoded_data_size
    );
    
    let padding_header_bytes = write_chunk_header(&padding_header).unwrap();
    let mut padding_chunk = BytesMut::with_capacity(padding_header_bytes.len() + 5);
    padding_chunk.extend_from_slice(&padding_header_bytes);
    padding_chunk.extend_from_slice(&[0u8; 5]); // 5 bytes of zeros
    
    // Create a simple records chunk to append after the padding
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"After padding").unwrap();
    let records_chunk = writer.serialize_chunk().unwrap();
    
    // Combine the chunks
    let mut combined = BytesMut::with_capacity(padding_chunk.len() + records_chunk.len());
    combined.extend_from_slice(&padding_chunk.freeze());
    combined.extend_from_slice(&records_chunk);
    
    // Create parser
    let mut parser = ChunksParser::new(combined.freeze());
    
    // First chunk (padding) should error
    let result = parser.next_chunk();
    assert!(result.is_err());
    
    // But we should be able to recover to the next chunk
    parser.recover_to_next_chunk().unwrap();
    
    // Now we should be able to read the simple records chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 1);
            assert_eq!(records[0], Bytes::from_static(b"After padding"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_large_records_chunk() {
    // Create a chunk with one large record
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    
    // Create a 1 MB record
    let large_record = vec![0xAA; 1_000_000];
    writer.write_record(&large_record).unwrap();
    
    let chunk = writer.serialize_chunk().unwrap();
    
    // Create parser
    let mut parser = ChunksParser::new(chunk);
    
    // Parse and verify
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            assert_eq!(simple_chunk.record_count(), 1);
            assert_eq!(simple_chunk.decoded_size(), 1_000_000);
            
            // Convert to iterator and read record
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].len(), 1_000_000);
            assert_eq!(records[0][0], 0xAA);
            assert_eq!(records[0][999_999], 0xAA);
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_recover_to_next_chunk() {
    // Create two chunks
    let mut writer1 = SimpleChunkWriter::new(CompressionType::None);
    writer1.write_record(b"First chunk record").unwrap();
    let chunk1 = writer1.serialize_chunk().unwrap();
    
    let mut writer2 = SimpleChunkWriter::new(CompressionType::None);
    writer2.write_record(b"Second chunk record").unwrap();
    let chunk2 = writer2.serialize_chunk().unwrap();
    
    // Combine the chunks
    let mut combined = BytesMut::with_capacity(chunk1.len() + chunk2.len());
    combined.extend_from_slice(&chunk1);
    combined.extend_from_slice(&chunk2);
    let combined_chunks = combined.freeze();
    
    // Create parser with the combined chunks
    let mut parser = ChunksParser::new(combined_chunks);
    
    // Get the first chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(_simple_chunk) => {
            // Don't read any records - this simulates an error during iteration
            // The buffer is now in an inconsistent state
            
            // Note: at this point, the buffer in parser is referring to the simple chunk's data
            // and not advanced to the next chunk yet
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
    
    // Recover to the next chunk
    parser.recover_to_next_chunk().unwrap();
    
    // We should now be positioned at the second chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            // Read the records from this chunk
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            // Verify it's the second chunk
            assert_eq!(records.len(), 1);
            assert_eq!(records[0], Bytes::from_static(b"Second chunk record"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_recover_after_partial_iteration() {
    // Create two chunks
    let mut writer1 = SimpleChunkWriter::new(CompressionType::None);
    writer1.write_record(b"Record 1.1").unwrap();
    writer1.write_record(b"Record 1.2").unwrap();
    writer1.write_record(b"Record 1.3").unwrap();
    let chunk1 = writer1.serialize_chunk().unwrap();
    
    let mut writer2 = SimpleChunkWriter::new(CompressionType::None);
    writer2.write_record(b"Record 2.1").unwrap();
    let chunk2 = writer2.serialize_chunk().unwrap();
    
    // Combine the chunks
    let mut combined = BytesMut::with_capacity(chunk1.len() + chunk2.len());
    combined.extend_from_slice(&chunk1);
    combined.extend_from_slice(&chunk2);
    let combined_chunks = combined.freeze();
    
    // Create parser with the combined chunks
    let mut parser = ChunksParser::new(combined_chunks);
    
    // Get the first chunk and partially iterate through it
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            // Create iterator but only consume part of the records
            let mut iter = simple_chunk.into_records().unwrap();
            
            // Read just the first record
            let record1 = iter.next().unwrap().unwrap();
            assert_eq!(record1, Bytes::from_static(b"Record 1.1"));
            
            // Now imagine an error happens and we don't read the rest
            // Drop the iterator here, leaving buffer in an inconsistent state
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
    
    // Recover to the next chunk
    parser.recover_to_next_chunk().unwrap();
    
    // We should now be positioned at the second chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(simple_chunk) => {
            // Read the records from this chunk
            let records: Vec<_> = simple_chunk.into_records().unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            
            // Verify it's the second chunk
            assert_eq!(records.len(), 1);
            assert_eq!(records[0], Bytes::from_static(b"Record 2.1"));
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
}

#[test]
fn test_recover_with_no_backup() {
    // Create single chunk
    let mut writer = SimpleChunkWriter::new(CompressionType::None);
    writer.write_record(b"Test record").unwrap();
    let chunk = writer.serialize_chunk().unwrap();
    
    // Create parser
    let mut parser = ChunksParser::new(chunk);
    
    // Before calling next_chunk, there should be no backup
    let result = parser.recover_to_next_chunk();
    assert!(result.is_err());
    
    // Get the chunk
    match parser.next_chunk().unwrap() {
        Chunk::SimpleRecords(_) => {
            // As expected
        },
        other => panic!("Expected SimpleRecords chunk, got {:?}", other),
    }
    
    // Try to recover - there's no next chunk, so this should fail
    let result = parser.recover_to_next_chunk();
    assert!(result.is_err());
}