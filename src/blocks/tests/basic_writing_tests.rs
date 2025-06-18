//! Tests for basic block writing functionality

use super::super::writer::{BlockWriter, BlockWriterConfig};
use super::super::utils::BLOCK_HEADER_SIZE;
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
    let chunk_data = b"abcdefghijklm";
    
    // Verify test assumptions
    assert!(chunk_data.len() as u64 + BLOCK_HEADER_SIZE < block_size, 
            "Test chunk should not cross block boundary");
    
    // Write the chunk
    writer.write_chunk(chunk_data).unwrap();
    
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
        chunk_data,
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
    let chunk_data = vec![1; 100];
    
    // Write the chunk - should add a block header
    writer.write_chunk(&chunk_data).unwrap();
    
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
    let chunk_data = b"This is chunk data written at position 0";
    
    // Write the chunk
    writer.write_chunk(chunk_data).unwrap();
    
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
    assert_eq!(&vec[BLOCK_HEADER_SIZE as usize..], chunk_data,
        "Chunk data should follow the header");
}

#[test]
fn test_write_empty_chunk() {
    // Test writing an empty chunk
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::new(buffer).unwrap();
    
    // Write an empty chunk
    let empty_chunk = &[];
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
    let empty_chunk = &[];
    writer.write_chunk(empty_chunk).unwrap();
    
    // Get the buffer and check
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // With our optimization, even at block boundaries we don't write headers for empty chunks
    let expected_size = block_size as usize; // Just the initial data, no header
    assert_eq!(vec.len(), expected_size, 
        "Empty chunk should not add any data, even at block boundaries");
}

/// Tests that the position is correctly tracked after writing a chunk.
/// 
/// This test verifies that:
/// 1. The position starts at 0
/// 2. After writing a chunk, position = BLOCK_HEADER_SIZE + chunk_size
/// 3. After writing another chunk (without crossing a block boundary),
///    position = previous_position + chunk_size (no additional header needed)
#[test]
fn test_position_tracking_after_write() {
    // Create a fresh buffer
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::new(buffer).unwrap();
    
    // Initial position should be 0
    #[cfg(test)]
    assert_eq!(writer.position(), 0, "Initial position should be 0");
    
    // Create a chunk
    let chunk_data = b"This is test data for position tracking";
    let chunk_size = chunk_data.len() as u64;
    
    // Write the chunk
    writer.write_chunk(chunk_data).unwrap();
    
    // Expected position is BLOCK_HEADER_SIZE + chunk_size
    let expected_position = BLOCK_HEADER_SIZE + chunk_size;
    
    #[cfg(test)]
    assert_eq!(writer.position(), expected_position, 
        "Position should be updated to include header and chunk data");
    
    // Write another chunk
    let second_chunk = b"More test data";
    let second_chunk_size = second_chunk.len() as u64;
    
    // Get the current position before the second write
    #[cfg(test)]
    let before_second_write = writer.position();
    
    writer.write_chunk(second_chunk).unwrap();
    
    // Get the actual position after the second write to analyze behavior
    #[cfg(test)]
    let actual_position = writer.position();
    
    // Analyze if a block header was added or not (based on if we're at a block boundary)
    #[cfg(test)]
    println!("Before second write: {}", before_second_write);
    #[cfg(test)]
    println!("After second write: {}", actual_position);
    #[cfg(test)]
    println!("Difference: {}", actual_position - before_second_write);
    #[cfg(test)]
    println!("Second chunk size: {}", second_chunk_size);
    
    // In this test, the second write doesn't cross a block boundary,
    // so no block header is added
    let expected_position_2 = expected_position + second_chunk_size;
    
    #[cfg(test)]
    assert_eq!(writer.position(), expected_position_2,
        "Position should be updated correctly after multiple writes");
}

/// Tests that position is tracked correctly when writing at block boundaries.
/// 
/// This test verifies that:
/// 1. After writing data that brings us exactly to a block boundary, 
///    position = block_size
/// 2. When writing additional data after a block boundary, a block header is added
///    and position = block_size + BLOCK_HEADER_SIZE + additional_data_size
/// 3. The tracked position matches the actual buffer size
#[test]
fn test_position_tracking_at_block_boundaries() {
    // Use a small block size to make testing easier
    let block_size = 100u64;
    let buffer = Cursor::new(Vec::new());
    
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig { block_size },
    ).unwrap();
    
    // Write data that will bring us exactly to a block boundary
    let initial_data = vec![1; (block_size - BLOCK_HEADER_SIZE) as usize];
    writer.write_chunk(&initial_data).unwrap();
    
    // We should now be at a block boundary
    #[cfg(test)]
    assert_eq!(writer.position(), block_size, 
        "Position should be at block boundary");
    
    // Write more data - this should trigger a block header at the boundary
    let more_data = vec![2; 50];
    let more_data_len = more_data.len() as u64;
    writer.write_chunk(&more_data).unwrap();
    
    // Expected position: block_size + BLOCK_HEADER_SIZE + more_data_len
    let expected_position = block_size + BLOCK_HEADER_SIZE + more_data_len;
    
    #[cfg(test)]
    assert_eq!(writer.position(), expected_position,
        "Position should include block header added at boundary");
    
    // Verify the buffer has the expected size
    let vec = writer.into_inner().into_inner();
    assert_eq!(vec.len(), expected_position as usize,
        "Buffer size should match tracked position");
}

/// Tests that position tracking works correctly when appending to existing data.
/// 
/// This test verifies that:
/// 1. When creating a writer with for_append_with_config, position starts at the specified
///    append position
/// 2. After writing a chunk (without crossing a block boundary), 
///    position = initial_position + chunk_size
/// 3. The tracked position matches the actual buffer size
#[test]
fn test_position_tracking_when_appending() {
    // Create a buffer with some initial data
    let initial_position = 42u64; // Some arbitrary position
    let buffer = Cursor::new(vec![0; initial_position as usize]);
    
    // Create a writer for appending
    let mut writer = BlockWriter::for_append_with_config(
        buffer,
        initial_position,
        BlockWriterConfig::default(),
    ).unwrap();
    
    // Check initial position
    #[cfg(test)]
    assert_eq!(writer.position(), initial_position,
        "Initial position should match append position");
    
    // Write some data
    let chunk_data = b"Appended data";
    let chunk_size = chunk_data.len() as u64;
    
    // Get position before write for analysis
    #[cfg(test)]
    let before_write = writer.position();
    
    writer.write_chunk(chunk_data).unwrap();
    
    // Get actual position after write for analysis
    #[cfg(test)]
    let actual_position = writer.position();
    
    // Analyze what's happening
    #[cfg(test)]
    println!("Initial position: {}", initial_position);
    #[cfg(test)]
    println!("Before write: {}", before_write);
    #[cfg(test)]
    println!("After write: {}", actual_position);
    #[cfg(test)]
    println!("Difference: {}", actual_position - before_write);
    #[cfg(test)]
    println!("Chunk size: {}", chunk_size);
    
    // Determine expected position based on block boundary logic
    // If initial_position is not at a block boundary, header might not be needed
    let expected_position = initial_position + chunk_size;
    
    #[cfg(test)]
    assert_eq!(writer.position(), expected_position,
        "Position should be updated when appending");
    
    // Verify buffer contents
    let vec = writer.into_inner().into_inner();
    assert_eq!(vec.len(), expected_position as usize,
        "Buffer size should match tracked position");
}
