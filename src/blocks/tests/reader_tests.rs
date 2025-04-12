//! Tests for BlockReader functionality

use super::super::reader::{BlockReader, BlockReaderConfig};
use super::super::writer::{BlockWriter, BlockWriterConfig};
use super::super::utils::BLOCK_HEADER_SIZE;
use bytes::Bytes;
use crate::error::RiegeliError;
use std::io::Cursor;

/// Helper function to create a test file with specific data
fn create_test_file(data: &[u8], block_size: u64) -> Vec<u8> {
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    writer.write_chunk(Bytes::from(data.to_vec())).unwrap();
    writer.flush().unwrap();
    
    writer.into_inner().into_inner()
}

#[test]
fn test_read_simple_chunks() {
    // Create a simple test file with a small chunk
    let test_data = b"This is a test chunk";
    let block_size = 100u64; // Use a small block size for testing
    
    let file_data = create_test_file(test_data, block_size);
    
    // Read the chunk back
    let cursor = Cursor::new(file_data);
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig {
            block_size,
        }
    ).unwrap();
    
    // Read the chunks
    let chunks = reader.read_chunks().unwrap();
    
    // Verify the chunks matches the original data
    assert_eq!(&chunks[..], test_data);
}

#[test]
fn test_read_chunks_crossing_block_boundary() {
    // Create a test file with a chunk that crosses a block boundary
    let block_size = 60u64; // Small block size for testing
    let usable_size = block_size - BLOCK_HEADER_SIZE;
    
    // Create a chunk slightly larger than the usable block size to cross a boundary
    let mut test_data = Vec::new();
    for i in 0..(usable_size as usize + 10) {
        test_data.push((i % 256) as u8);
    }
    
    let file_data = create_test_file(&test_data, block_size);
    
    // Read the chunk back
    let cursor = Cursor::new(file_data);
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig {
            block_size,
        }
    ).unwrap();
    
    // Read the chunks
    let chunks = reader.read_chunks().unwrap();
    
    // Verify the chunks matches the original data
    assert_eq!(&chunks[..], &test_data[..]);
}

#[test]
fn test_read_multiple_chunks_sequential() {
    // Create a file with multiple chunks
    let block_size = 64u64;
    
    // Create a buffer
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Write multiple chunks
    let chunk1 = b"First chunk data";
    let chunk2 = b"Second chunk with different data";
    let chunk3 = b"Third chunk";
    
    writer.write_chunk(Bytes::from(chunk1.to_vec())).unwrap();
    writer.write_chunk(Bytes::from(chunk2.to_vec())).unwrap();
    writer.write_chunk(Bytes::from(chunk3.to_vec())).unwrap();
    writer.flush().unwrap();
    
    let file_data = writer.into_inner().into_inner();
    
    // Read the chunks back
    let cursor = Cursor::new(file_data);
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig {
            block_size,
        }
    ).unwrap();
    
    // Read each chunks section and verify
    let read_chunks1 = reader.read_chunks().unwrap();
    assert_eq!(&read_chunks1[..], chunk1);
    
    let read_chunks2 = reader.read_chunks().unwrap();
    assert_eq!(&read_chunks2[..], chunk2);
    
    let read_chunks3 = reader.read_chunks().unwrap();
    assert_eq!(&read_chunks3[..], chunk3);
}

#[test]
fn test_large_chunks_crossing_multiple_boundaries() {
    // Create a file with a large chunk that crosses multiple block boundaries
    let block_size = 64u64;
    let chunk_size = (block_size * 5) as usize; // 5 blocks worth of data
    
    // Create pattern data
    let mut pattern_data = Vec::with_capacity(chunk_size);
    for i in 0..chunk_size {
        pattern_data.push((i % 256) as u8);
    }
    
    let file_data = create_test_file(&pattern_data, block_size);
    
    // Read the chunk back
    let cursor = Cursor::new(file_data);
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig {
            block_size,
        }
    ).unwrap();
    
    // Read the chunks
    let chunks = reader.read_chunks().unwrap();
    
    // Verify the chunks matches the original data
    assert_eq!(&chunks[..], &pattern_data[..]);
}

#[test]
fn test_skip_chunks() {
    // Create a file with multiple chunks
    let block_size = 64u64;
    
    // Create a buffer
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Write multiple chunks
    let chunk1 = b"First chunk data";
    let chunk2 = b"Second chunk with different data";
    let chunk3 = b"Third chunk";
    
    writer.write_chunk(Bytes::from(chunk1.to_vec())).unwrap();
    writer.write_chunk(Bytes::from(chunk2.to_vec())).unwrap();
    writer.write_chunk(Bytes::from(chunk3.to_vec())).unwrap();
    writer.flush().unwrap();
    
    let file_data = writer.into_inner().into_inner();
    
    // Read the chunks back
    let cursor = Cursor::new(file_data);
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig {
            block_size,
        }
    ).unwrap();
    
    // Read first chunks section
    let read_chunks1 = reader.read_chunks().unwrap();
    assert_eq!(&read_chunks1[..], chunk1);
    
    // Skip the second chunks section
    reader.skip_chunks().unwrap();
    
    // Read the third chunks section
    let read_chunks3 = reader.read_chunks().unwrap();
    assert_eq!(&read_chunks3[..], chunk3);
}

#[test]
fn test_multiple_chunks_in_single_block() {
    // This test demonstrates that when multiple chunks fit within a single block,
    // the reader will return them as a single Bytes object since it can't
    // determine chunk boundaries without additional chunk format information.
    
    // Create a buffer with a large enough block size to fit multiple chunks
    let block_size = 256u64; // Large block size
    let buffer = Cursor::new(Vec::new());
    
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Create several very small chunks that will all fit in a single block
    // after the initial header
    let small_chunk1 = b"Small chunk 1";
    let small_chunk2 = b"Small chunk 2";
    let small_chunk3 = b"Small chunk 3";
    
    // Calculate total size to ensure all chunks fit in one block
    let total_size = small_chunk1.len() + small_chunk2.len() + small_chunk3.len();
    let usable_size = block_size - BLOCK_HEADER_SIZE;
    assert!(total_size as u64 <= usable_size, 
        "Test assumption failed: chunks should fit in a single block");
    
    // Write all three chunks individually
    writer.write_chunk(Bytes::from(small_chunk1.to_vec())).unwrap();
    writer.write_chunk(Bytes::from(small_chunk2.to_vec())).unwrap();
    writer.write_chunk(Bytes::from(small_chunk3.to_vec())).unwrap();
    writer.flush().unwrap();
    
    // Get the file data
    let file_data = writer.into_inner().into_inner();
    
    
    // Read the chunks back
    let cursor = Cursor::new(file_data);
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig {
            block_size,
        }
    ).unwrap();
    
    // When starting at a block boundary, the reader uses block header information
    // to read exactly the first chunk
    let first_chunks = reader.read_chunks().unwrap();
    assert_eq!(&first_chunks[..], small_chunk1, 
               "First read should return only the first chunk");
    
    // For the second read, since we're now in the middle of a block, the behavior is
    // different. Since we can't rely on block header information for chunk boundaries,
    // the reader will read both remaining chunks.
    let remaining_chunks = reader.read_chunks().unwrap();
    
    // Create the expected data - remaining two chunks
    let mut expected_remaining = Vec::new();
    expected_remaining.extend_from_slice(small_chunk2);
    expected_remaining.extend_from_slice(small_chunk3);
    
    // Verify we got both remaining chunks
    assert_eq!(&remaining_chunks[..], &expected_remaining[..],
               "Second read should return the remaining chunks concatenated");
    
    // Verify that a third call to read_chunks() either returns empty data or an EOF error
    match reader.read_chunks() {
        Ok(more_data) => {
            assert!(more_data.is_empty(), 
                    "Third read_chunks call should return empty data");
        },
        Err(e) => {
            // An EOF error is acceptable at the end of the file
            match e {
                RiegeliError::UnexpectedEof => {
                    // This is expected, we've reached the end of the file
                    println!("Got expected EOF on third read");
                },
                _ => {
                    panic!("Unexpected error on third read_chunks call: {:?}", e);
                }
            }
        }
    }
}