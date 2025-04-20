// Tests for ChunksParser

use crate::chunks::chunks_parser::{ChunkPiece, ChunksParser};
use crate::chunks::header::ChunkType;
use crate::chunks::header::{ChunkHeader, CHUNK_HEADER_SIZE};
use crate::chunks::header_writer::write_chunk_header;
use crate::chunks::simple_chunk_writer::SimpleChunkWriter;
use crate::chunks::writer::ChunkWriter;
use crate::compression::core::CompressionType;
use crate::error::DiskyError;
use bytes::{BufMut, Bytes, BytesMut};

// Helper function to create a simple chunk
fn create_simple_chunk<'a, T>(records: &[T]) -> Bytes
where
    T: AsRef<[u8]> + 'a,
{
    let mut writer = SimpleChunkWriter::new(CompressionType::None);

    for record in records {
        writer.write_record(record.as_ref()).unwrap();
    }

    writer.serialize_chunk().unwrap()
}

// Helper function to create a signature chunk
fn create_signature_chunk() -> Bytes {
    // Create a chunk header for a signature chunk
    let header = ChunkHeader::new(
        0,                    // data_size (signature has no data)
        0,                    // data_hash
        ChunkType::Signature, // signature chunk type
        0,                    // num_records
        0,                    // decoded_data_size
    );

    // Write the header to bytes
    write_chunk_header(&header).unwrap()
}

// Helper function to create corrupted/invalid chunk header
fn create_invalid_chunk_header() -> Bytes {
    // Create a header with known values but with an unrecognized chunk type
    let data_size = 20; // Small data size
    let data_hash = 12345;

    // Using crate's header module to create header
    let header = ChunkHeader::new(
        data_size,
        data_hash,
        ChunkType::Padding, // Padding chunks are not implemented in the parser
        0,
        0,
    );

    // Write the header
    let header_bytes = write_chunk_header(&header).unwrap();

    // Add some data to match the data_size
    let mut buffer = BytesMut::with_capacity(header_bytes.len() + data_size as usize);
    buffer.extend_from_slice(&header_bytes);

    // Add random data to match data_size
    for i in 0..data_size {
        buffer.put_u8((i % 256) as u8);
    }

    buffer.freeze()
}

#[test]
fn test_empty_buffer() {
    // Create an empty buffer
    let empty_buffer = Bytes::new();

    // Create a parser with the empty buffer
    let mut parser = ChunksParser::new(empty_buffer);

    // Parsing should return ChunksEnd immediately
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd for empty buffer"),
    }
}

#[test]
fn test_signature_chunk() {
    // Create a signature chunk
    let signature_chunk = create_signature_chunk();

    // Create a parser with the signature chunk
    let mut parser = ChunksParser::new(signature_chunk);

    // Should return Signature chunk piece with header
    match parser.next().unwrap() {
        ChunkPiece::Signature(header) => {
            assert_eq!(header.chunk_type, ChunkType::Signature);
            assert_eq!(header.data_size, 0);
            assert_eq!(header.num_records, 0);
            assert_eq!(header.decoded_data_size, 0);
        }
        _ => panic!("Expected Signature chunk piece with header"),
    }

    // Next call should return ChunksEnd
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd after signature"),
    }
}

#[test]
fn test_simple_chunk_with_records() {
    // Create a simple chunk with records
    let records = [b"Record 1", b"Record 2", b"Record 3"];
    let simple_chunk = create_simple_chunk(&records);

    // Create a parser with the simple chunk
    let mut parser = ChunksParser::new(simple_chunk);

    // Should return SimpleChunkStart
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart"),
    }

    // Read records
    for expected_record in &records {
        match parser.next().unwrap() {
            ChunkPiece::Record(record) => {
                assert_eq!(record, Bytes::copy_from_slice(*expected_record));
            }
            _ => panic!("Expected Record"),
        }
    }

    // Should return SimpleChunkEnd
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd"),
    }

    // Next call should return ChunksEnd
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}

#[test]
fn test_multiple_chunks() {
    // Create a signature chunk followed by a simple chunk
    let signature_chunk = create_signature_chunk();
    let records = [b"Record 1", b"Record 2"];
    let simple_chunk = create_simple_chunk(&records);

    // Combine chunks
    let mut combined = BytesMut::with_capacity(signature_chunk.len() + simple_chunk.len());
    combined.extend_from_slice(&signature_chunk);
    combined.extend_from_slice(&simple_chunk);
    let combined_chunks = combined.freeze();

    // Create a parser with combined chunks
    let mut parser = ChunksParser::new(combined_chunks);

    // First chunk: signature
    match parser.next().unwrap() {
        ChunkPiece::Signature(header) => {
            assert_eq!(header.chunk_type, ChunkType::Signature);
        }
        _ => panic!("Expected Signature chunk piece with header"),
    }

    // Second chunk: simple records
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart"),
    }

    // Read records from simple chunk
    for expected_record in &records {
        match parser.next().unwrap() {
            ChunkPiece::Record(record) => {
                assert_eq!(record, Bytes::copy_from_slice(*expected_record));
            }
            _ => panic!("Expected Record"),
        }
    }

    // End of simple chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd"),
    }

    // End of all chunks
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}

#[test]
fn test_padding_chunk_error() {
    // Create a padding chunk (not implemented in the parser)
    let padding_chunk = create_invalid_chunk_header();

    // Create a parser with the padding chunk
    let mut parser = ChunksParser::new(padding_chunk);

    // Should return an error for unimplemented chunk type
    let result = parser.next();
    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::UnsupportedChunkType(chunk_type) => {
                assert_eq!(chunk_type, ChunkType::Padding.as_byte());
            }
            _ => panic!("Expected Other error"),
        }
    }
}

#[test]
fn test_skip_chunk_after_error() {
    // Create a padding chunk followed by a simple chunk
    let padding_chunk = create_invalid_chunk_header();
    let records = [b"Record 1", b"Record 2"];
    let simple_chunk = create_simple_chunk(&records);

    // Combine chunks
    let mut combined = BytesMut::with_capacity(padding_chunk.len() + simple_chunk.len());
    combined.extend_from_slice(&padding_chunk);
    combined.extend_from_slice(&simple_chunk);
    let combined_chunks = combined.freeze();

    // Create a parser with combined chunks
    let mut parser = ChunksParser::new(combined_chunks);

    // First chunk: padding (should error)
    let result = parser.next();
    assert!(result.is_err());

    // Refresh the parser state
    parser.skip_chunk();

    // Try to parse the next chunk (should succeed with SimpleChunkStart)
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart after refresh"),
    }

    // Read records from simple chunk
    for expected_record in &records {
        match parser.next().unwrap() {
            ChunkPiece::Record(record) => {
                assert_eq!(record, Bytes::copy_from_slice(*expected_record));
            }
            _ => panic!("Expected Record"),
        }
    }

    // End of simple chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd"),
    }

    // End of all chunks
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}

#[test]
fn test_corrupted_chunk_data() {
    // Create a simple chunk
    let records = [b"Record 1", b"Record 2"];
    let simple_chunk = create_simple_chunk(&records);

    // Truncate the chunk data to make it invalid
    let truncated_chunk = simple_chunk.slice(0..CHUNK_HEADER_SIZE + 5); // Not enough data for the full chunk

    // Create a parser with the truncated chunk
    let mut parser = ChunksParser::new(truncated_chunk);

    // Should return an error due to corruption
    let result = parser.next();
    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::MissingChunkData(_) => {
                // Expected error
            }
            _ => panic!("Expected MissingChunkData error, got: {:?}", err),
        }
    }
}

#[test]
fn test_simple_chunk_no_records() {
    // Create a simple chunk with no records
    let empty_records: [&[u8]; 0] = [];
    let simple_chunk = create_simple_chunk(&empty_records);

    // Create a parser with the simple chunk
    let mut parser = ChunksParser::new(simple_chunk);

    // Should return SimpleChunkStart
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart"),
    }

    // Should return SimpleChunkEnd (no records to read)
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd"),
    }

    // Next call should return ChunksEnd
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}

#[test]
fn test_finishing_state() {
    // Create an empty buffer to immediately reach ChunksEnd
    let empty_buffer = Bytes::new();
    let mut parser = ChunksParser::new(empty_buffer);

    // First call returns ChunksEnd
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd for empty buffer"),
    }

    // Second call should return an error
    let result = parser.next();
    assert!(result.is_err());
    if let Err(err) = result {
        match err {
            DiskyError::Other(msg) => {
                assert!(msg.contains("Cannot advance on a finished ChunksParser"));
            }
            _ => panic!("Expected Other error"),
        }
    }
}

#[test]
fn test_large_records() {
    // Create a simple chunk with one large record
    let large_record = vec![0xAA; 100_000];
    let records = [&large_record[..]];
    let simple_chunk = create_simple_chunk(&records);

    // Create a parser with the simple chunk
    let mut parser = ChunksParser::new(simple_chunk);

    // Should return SimpleChunkStart
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart"),
    }

    // Read the large record
    match parser.next().unwrap() {
        ChunkPiece::Record(record) => {
            assert_eq!(record.len(), 100_000);
            assert_eq!(record[0], 0xAA);
            assert_eq!(record[99_999], 0xAA);
        }
        _ => panic!("Expected Record"),
    }

    // Should return SimpleChunkEnd
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd"),
    }

    // Next call should return ChunksEnd
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}

#[test]
fn test_multiple_simple_chunks() {
    // Create two simple chunks with different records
    let records1 = [b"Chunk1 Record1", b"Chunk1 Record2"];
    let simple_chunk1 = create_simple_chunk(&records1);

    let records2 = [b"Chunk2 Record1", b"Chunk2 Record2", b"Chunk2 Record3"];
    let simple_chunk2 = create_simple_chunk(&records2);

    // Combine chunks
    let mut combined = BytesMut::with_capacity(simple_chunk1.len() + simple_chunk2.len());
    combined.extend_from_slice(&simple_chunk1);
    combined.extend_from_slice(&simple_chunk2);
    let combined_chunks = combined.freeze();

    // Create a parser with combined chunks
    let mut parser = ChunksParser::new(combined_chunks);

    // First chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart for first chunk"),
    }

    // Read records from first chunk
    for expected_record in &records1 {
        match parser.next().unwrap() {
            ChunkPiece::Record(record) => {
                assert_eq!(record, Bytes::copy_from_slice(*expected_record));
            }
            _ => panic!("Expected Record from first chunk"),
        }
    }

    // End of first chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd for first chunk"),
    }

    // Second chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart for second chunk"),
    }

    // Read records from second chunk
    for expected_record in &records2 {
        match parser.next().unwrap() {
            ChunkPiece::Record(record) => {
                assert_eq!(record, Bytes::copy_from_slice(*expected_record));
            }
            _ => panic!("Expected Record from second chunk"),
        }
    }

    // End of second chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd for second chunk"),
    }

    // End of all chunks
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}

#[test]
fn test_mixed_chunk_types() {
    // Create a sequence: signature -> simple -> signature
    let signature1 = create_signature_chunk();
    let records = [b"Record 1", b"Record 2"];
    let simple_chunk = create_simple_chunk(&records);
    let signature2 = create_signature_chunk();

    // Combine chunks
    let mut combined =
        BytesMut::with_capacity(signature1.len() + simple_chunk.len() + signature2.len());
    combined.extend_from_slice(&signature1);
    combined.extend_from_slice(&simple_chunk);
    combined.extend_from_slice(&signature2);
    let combined_chunks = combined.freeze();

    // Create a parser with combined chunks
    let mut parser = ChunksParser::new(combined_chunks);

    // First signature
    match parser.next().unwrap() {
        ChunkPiece::Signature(header) => {
            assert_eq!(header.chunk_type, ChunkType::Signature);
        }
        _ => panic!("Expected first Signature with header"),
    }

    // Simple chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart"),
    }

    // Read records
    for expected_record in &records {
        match parser.next().unwrap() {
            ChunkPiece::Record(record) => {
                assert_eq!(record, Bytes::copy_from_slice(*expected_record));
            }
            _ => panic!("Expected Record"),
        }
    }

    // End of simple chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd"),
    }

    // Second signature
    match parser.next().unwrap() {
        ChunkPiece::Signature(header) => {
            assert_eq!(header.chunk_type, ChunkType::Signature);
        }
        _ => panic!("Expected second Signature with header"),
    }

    // End of all chunks
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}

#[test]
fn test_recovery_during_simple_chunk_parsing() {
    // We'll create a sequence of chunks: signature -> valid simple chunk -> corrupted simple chunk -> signature
    // Then we'll verify we can recover from errors during the corrupted simple chunk parsing

    // Create the valid chunks
    let signature1 = create_signature_chunk();
    let valid_records = [b"Valid Record 1", b"Valid Record 2"];
    let valid_simple_chunk = create_simple_chunk(&valid_records);

    // Create corrupted data that will pass initial validation but fail during record parsing
    // We'll construct a simple chunk with valid header but invalid record sizes
    let mut corrupted_data = BytesMut::with_capacity(40);

    // Add compression type (None = 0)
    corrupted_data.put_u8(0);

    // Add a varint for sizes section length (10 bytes)
    corrupted_data.put_u8(10);

    // Add some corrupted record sizes that will cause errors during parsing
    // First varint is valid but points to data beyond what we have
    corrupted_data.put_u8(0xFF); // Start of a multi-byte varint
    corrupted_data.put_u8(0xFF); // Continue varint
    corrupted_data.put_u8(0xFF); // Continue varint
    corrupted_data.put_u8(0x01); // End varint (very large value > 2^24)

    // Add invalid varint sequence
    corrupted_data.put_u8(0xFF);
    corrupted_data.put_u8(0xFF);
    corrupted_data.put_u8(0xFF);
    corrupted_data.put_u8(0xFF);
    corrupted_data.put_u8(0xFF);
    corrupted_data.put_u8(0xFF);

    // Add some record data (will never be reached due to invalid sizes)
    corrupted_data.extend_from_slice(b"This data will never be read due to corruption");

    // Calculate the real hash of the data to avoid hash verification failure
    // We want to test record parsing failure, not hash verification failure
    use crate::hash::highway_hash;
    let data_hash = highway_hash(&corrupted_data);

    // Create a header claiming 2 records with correct hash
    let header = ChunkHeader::new(
        corrupted_data.len() as u64, // Data size
        data_hash,                   // Use the actual hash to pass hash verification
        ChunkType::SimpleRecords,
        2,   // We claim 2 records
        100, // Claim some decoded size
    );

    let corrupted_data = corrupted_data.freeze();

    // Write the header
    let header_bytes = write_chunk_header(&header).unwrap();

    // Combine header and corrupted data
    let mut corrupted_chunk = BytesMut::with_capacity(header_bytes.len() + corrupted_data.len());
    corrupted_chunk.extend_from_slice(&header_bytes);
    corrupted_chunk.extend_from_slice(&corrupted_data);
    let corrupted_chunk = corrupted_chunk.freeze();

    // Add a final signature chunk
    let signature2 = create_signature_chunk();

    // Combine all chunks
    let mut combined = BytesMut::with_capacity(
        signature1.len() + valid_simple_chunk.len() + corrupted_chunk.len() + signature2.len(),
    );
    combined.extend_from_slice(&signature1);
    combined.extend_from_slice(&valid_simple_chunk);
    combined.extend_from_slice(&corrupted_chunk);
    combined.extend_from_slice(&signature2);
    let combined_chunks = combined.freeze();

    // Create a parser with all chunks
    let mut parser = ChunksParser::new(combined_chunks);

    // First signature
    match parser.next().unwrap() {
        ChunkPiece::Signature(header) => {
            assert_eq!(header.chunk_type, ChunkType::Signature);
        }
        _ => panic!("Expected first Signature with header"),
    }

    // Valid simple chunk start
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart for valid chunk"),
    }

    // Read records from valid chunk
    for expected_record in &valid_records {
        match parser.next().unwrap() {
            ChunkPiece::Record(record) => {
                assert_eq!(record, Bytes::copy_from_slice(*expected_record));
            }
            _ => panic!("Expected Record from valid chunk"),
        }
    }

    // End of valid chunk
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkEnd => {}
        _ => panic!("Expected SimpleChunkEnd for valid chunk"),
    }

    // Now start parsing corrupted chunk - should return SimpleChunkStart
    match parser.next().unwrap() {
        ChunkPiece::SimpleChunkStart => {}
        _ => panic!("Expected SimpleChunkStart for corrupted chunk"),
    }

    // Trying to read records from corrupted chunk should fail
    let result = parser.next();
    assert!(
        result.is_err(),
        "Expected error parsing corrupted chunk data"
    );

    // Refresh the parser to recover
    parser.skip_chunk();

    // Should be able to continue with next (signature) chunk
    match parser.next().unwrap() {
        ChunkPiece::Signature(header) => {
            assert_eq!(header.chunk_type, ChunkType::Signature);
        }
        _ => panic!("Expected signature chunk with header after recovery"),
    }

    // End of all chunks
    match parser.next().unwrap() {
        ChunkPiece::ChunksEnd => {}
        _ => panic!("Expected ChunksEnd"),
    }
}
