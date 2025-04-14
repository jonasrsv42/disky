//! Test that demonstrates a bug in the BlockReader implementation.
//! 
//! The bug occurs when:
//! 1. Reading starts within a block (not at a block boundary)
//! 2. A chunk ends exactly at a block boundary
//! 3. The next chunk starts at that block boundary
//! 
//! In this case, the reader incorrectly reads both chunks before emitting the first one.

use super::super::reader::{BlockReader, BlockReaderConfig};
use super::super::writer::{BlockWriter, BlockWriterConfig};
use bytes::Bytes;
use std::io::Cursor;

#[test]
fn test_bug_chunk_ends_at_block_boundary() {
    // This test demonstrates the bug where if we start reading within a block and the chunk ends 
    // right at the end of the block, and the next chunk starts at the beginning of a block,
    // then it will read both chunks before emitting the first one.
    
    // Use a small block size to make the test case easier to construct and debug
    let block_size = 100u64;
    
    // Create a buffer
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig { block_size },
    ).unwrap();
    
    // We'll construct a specific scenario:
    // 1. First, write some initial data that doesn't start at a block boundary
    let initial_offset = 30u64; // Start 30 bytes into the file
    writer.get_mut().set_position(initial_offset);
    writer.update_position().unwrap();
    
    // 2. Create a first chunk that will end exactly at the next block boundary
    // Need to fill: 100 - 30 = 70 bytes (to reach the block boundary)
    let first_chunk_size = (block_size - initial_offset) as usize;
    let first_chunk = vec![0xA1u8; first_chunk_size];
    
    // 3. Create a second chunk that will start at the block boundary
    let second_chunk = vec![0xB2u8; 50]; // 50 bytes for the second chunk
    
    println!("Test setup:");
    println!("  Block size: {}", block_size);
    println!("  Initial offset: {}", initial_offset);
    println!("  First chunk size: {}", first_chunk_size);
    println!("  Second chunk size: {}", second_chunk.len());
    
    // Write the two chunks
    writer.write_chunk(Bytes::from(first_chunk.clone())).unwrap();
    writer.write_chunk(Bytes::from(second_chunk.clone())).unwrap();
    writer.flush().unwrap();
    
    // Get the written data
    let file_data = writer.into_inner().into_inner();
    
    println!("Total file size: {}", file_data.len());
    
    // Now create a reader positioned at the initial offset
    let mut cursor = Cursor::new(file_data);
    cursor.set_position(initial_offset);
    
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig { block_size },
    ).unwrap();
    
    // This read should return only the first chunk, despite the second chunk
    // starting at a block boundary
    let read_chunk = reader.read_chunks().unwrap();
    
    // Verify we only received the first chunk
    assert_eq!(read_chunk.len(), first_chunk_size, 
               "First read should return only the first chunk data");
    
    // Verify the content matches the first chunk
    for (i, byte) in read_chunk.iter().enumerate() {
        assert_eq!(*byte, first_chunk[i], 
                 "Mismatch at position {}: expected {:02X}, got {:02X}", 
                 i, first_chunk[i], *byte);
    }
    
    // Now read the second chunk
    let read_chunk2 = reader.read_chunks().unwrap();
    
    // Verify we received the second chunk
    assert_eq!(read_chunk2.len(), second_chunk.len(), 
              "Second read should return only the second chunk data");
    
    // Verify the content matches the second chunk
    for (i, byte) in read_chunk2.iter().enumerate() {
        assert_eq!(*byte, second_chunk[i], 
                 "Mismatch at position {}: expected {:02X}, got {:02X}", 
                 i, second_chunk[i], *byte);
    }
}

#[test]
fn test_read_starting_within_block_crossing_boundary() {
    // Test reading starting in the middle of a block,
    // where the data crosses a block boundary
    let block_size = 100u64;
    let buffer = Cursor::new(Vec::new());
    
    // Create a writer
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig { block_size },
    ).unwrap();
    
    // Start writing at position 30
    writer.get_mut().set_position(30);
    writer.update_position().unwrap();
    
    // Write a chunk that will cross the block boundary
    // First block: 30 to 100 = 70 bytes
    // Second block: 100 to 150 = 50 bytes
    let pattern_data = (0..120u8).collect::<Vec<_>>();
    writer.write_chunk(Bytes::from(pattern_data.clone())).unwrap();
    writer.flush().unwrap();
    
    // Get the file data
    let file_data = writer.into_inner().into_inner();
    
    // Create a reader starting at position 30
    let mut cursor = Cursor::new(file_data);
    cursor.set_position(30);
    
    let mut reader = BlockReader::with_config(
        cursor,
        BlockReaderConfig { block_size },
    ).unwrap();
    
    // Read the chunk
    let read_data = reader.read_chunks().unwrap();
    
    // Verify the data matches the original pattern
    assert_eq!(read_data.len(), pattern_data.len(),
              "Read data length should match original pattern length");
    
    for (i, byte) in read_data.iter().enumerate() {
        assert_eq!(*byte, pattern_data[i],
                  "Byte at position {} should match original pattern", i);
    }
}