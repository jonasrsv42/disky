//! Tests for basic block writing functionality

use super::super::writer::{BlockWriter, BlockWriterConfig};
use super::super::utils::BLOCK_HEADER_SIZE;
use bytes::Bytes;
use std::io::Cursor;

#[test]
fn test_write_chunk_smaller_than_block() {
    // Create a fresh buffer
    let buffer = Cursor::new(Vec::new());
    
    // Use a block size larger than our test data but sufficient to avoid cascading headers
    let block_size = 60u64; // > 2 * BLOCK_HEADER_SIZE (24)
    
    let mut writer = BlockWriter::with_config(
        buffer, 
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Create a small chunk of data that won't cross a block boundary
    let chunk_data = Bytes::from(b"abcdefghijklm".to_vec());
    
    // Verify test assumptions
    assert!(chunk_data.len() as u64 + BLOCK_HEADER_SIZE < block_size, 
            "Test chunk should not cross block boundary");
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer and check its contents
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    println!("Chunk data size: {}", chunk_data.len());
    println!("Header size: {}", BLOCK_HEADER_SIZE);
    println!("Actual size: {}", vec.len());
    
    // There should be a header at position 0 followed by the chunk data
    let expected_size = BLOCK_HEADER_SIZE as usize + chunk_data.len();
    assert_eq!(vec.len(), expected_size as usize);
    
    // Check chunk data is correctly written after the header
    assert_eq!(
        &vec[BLOCK_HEADER_SIZE as usize..],
        &chunk_data[..],
        "Chunk data should be written exactly as provided after the header"
    );
}

#[test]
fn test_write_chunk_at_block_boundary() {
    // Create a buffer and fill it to exactly one block
    // Use a block size large enough for our test and to avoid cascading headers
    let block_size = 1024u64; // >> 2 * BLOCK_HEADER_SIZE (24)
    let buffer = Cursor::new(vec![0; block_size as usize]);
    
    // Create a writer for appending at the block boundary
    let mut writer = BlockWriter::for_append_with_config(
        buffer,
        block_size,      // Current position at the block boundary
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Create a chunk
    let chunk_data = Bytes::from(vec![1; 100]);
    
    // Write the chunk - should add a block header
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer and check its contents
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // The buffer should contain exactly block_size (initial) +
    // block_header_size (added at boundary) + chunk_data.len() bytes
    let expected_size = block_size as usize + BLOCK_HEADER_SIZE as usize + chunk_data.len();
    assert_eq!(vec.len(), expected_size);
    
    // Check initial block data (zeros)
    assert_eq!(&vec[0..block_size as usize], &vec![0; block_size as usize]);
    
    // The block header should be at the block boundary
    // We can't easily verify the specific hash values
    
    // Check the chunk data is correctly appended
    assert_eq!(&vec[block_size as usize + BLOCK_HEADER_SIZE as usize..], &chunk_data[..]);
}

#[test]
fn test_writing_at_position_zero() {
    // Test writing at the beginning of a file (position 0)
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::new(buffer).unwrap();
    
    // Create a chunk
    let chunk_data = Bytes::from(b"This is chunk data written at position 0".to_vec());
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // The data should be written directly (position 0 is a block boundary but 
    // we expect a header to be written first)
    let expected_size = BLOCK_HEADER_SIZE as usize + chunk_data.len();
    assert_eq!(vec.len(), expected_size, 
        "Should write a header at position 0 followed by chunk data");
    
    // The block header should be at the beginning
    // Then the chunk data
    assert_eq!(&vec[BLOCK_HEADER_SIZE as usize..], &chunk_data[..],
        "Chunk data should follow the header");
}

#[test]
fn test_write_empty_chunk() {
    // Test writing an empty chunk
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::new(buffer).unwrap();
    
    // Write an empty chunk
    let empty_chunk = Bytes::new();
    writer.write_chunk(empty_chunk).unwrap();
    
    // Get the buffer and verify it's still empty (no headers needed for empty chunks)
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    assert_eq!(vec.len(), 0, "Empty chunk should not write any data");
}

#[test]
fn test_write_empty_chunk_at_block_boundary() {
    // Create a buffer and fill it to exactly one block
    let block_size = 100u64; // > 2 * BLOCK_HEADER_SIZE (24)
    let buffer = Cursor::new(vec![0; block_size as usize]);
    
    // Create a writer for appending at the block boundary
    let mut writer = BlockWriter::for_append_with_config(
        buffer,
        block_size, // At block boundary
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Write an empty chunk
    let empty_chunk = Bytes::new();
    writer.write_chunk(empty_chunk).unwrap();
    
    // Get the buffer and check
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // With our optimization, even at block boundaries we don't write headers for empty chunks
    let expected_size = block_size as usize; // Just the initial data, no header
    assert_eq!(vec.len(), expected_size, 
        "Empty chunk should not add any data, even at block boundaries");
}
