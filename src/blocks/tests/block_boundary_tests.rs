//! Tests for handling block boundaries in BlockWriter

use super::super::writer::{BlockWriter, BlockWriterConfig, BLOCK_HEADER_SIZE};
use super::helpers::{get_buffer,};
use bytes::Bytes;
use std::io::{Cursor, Write};

#[test]
fn test_write_chunk_crossing_block_boundary() {
    // Use a small block size that's still larger than header size to avoid cascading headers
    let block_size = 60u64;  // > 2 * BLOCK_HEADER_SIZE (24)
    
    // Start with a fresh buffer
    let buffer = Cursor::new(Vec::new());
    
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Create chunk with distinct patterns that's large enough to cross a block boundary
    // We need at least (block_size - BLOCK_HEADER_SIZE) + 1 bytes to cross the boundary
    // Using a repeating pattern for easy visual identification
    let needed_size = (block_size - BLOCK_HEADER_SIZE + 20) as usize; // +20 for some extra bytes
    let mut data = Vec::with_capacity(needed_size);
    
    // Fill with a repeating pattern
    let pattern = b"ABCDEFG";
    while data.len() < needed_size {
        data.extend_from_slice(pattern);
    }
    
    // Truncate to exact size
    data.truncate(needed_size);
    
    let chunk_data = Bytes::from(data);
    
    // Calculate where the first block boundary will be
    // There will be a header at pos 0, and data starts at pos BLOCK_HEADER_SIZE
    let first_header_size = BLOCK_HEADER_SIZE as usize;
    let bytes_in_first_block = (block_size - BLOCK_HEADER_SIZE) as usize;
    
    // Verify test assumptions
    assert!(chunk_data.len() > bytes_in_first_block,
           "Test chunk must cross at least one block boundary");
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer and check its contents
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // Print detailed info for debugging
    println!("Block size: {}", block_size);
    println!("Bytes in first block: {}", bytes_in_first_block);
    println!("Block header size: {}", BLOCK_HEADER_SIZE);
    println!("Chunk data size: {}", chunk_data.len());
    println!("Actual size: {}", vec.len());
    
    // The output should include:
    // 1. Initial header at position 0
    // 2. First part of chunk data (bytes_in_first_block)
    // 3. Another block header at block boundary (block_size)
    // 4. Rest of chunk data
    
    // Let's focus on checking the content rather than exact size
    println!("Note: Headers are at positions 0 and {}", block_size);
    
    // Ensure we have enough data (more than chunk + headers)
    let minimum_expected_size = chunk_data.len() + 2 * BLOCK_HEADER_SIZE as usize;
    assert!(vec.len() >= minimum_expected_size,
            "Output should include chunk data + at least two block headers");
    
    // Check first part of chunk data (after initial header, up to block boundary)
    let first_part_start = first_header_size;
    let first_part_end = block_size as usize; // End at the block boundary
    
    println!("First part of chunk: bytes {} to {}", first_part_start, first_part_end);
    
    assert_eq!(
        &vec[first_part_start..first_part_end],
        &chunk_data[..bytes_in_first_block],
        "First part of chunk data after initial header should match"
    );
    
    // Skip over the second block header
    let second_part_start = first_part_end + BLOCK_HEADER_SIZE as usize;
    
    // Check second part of chunk data (after second header)
    let remaining_bytes = chunk_data.len() - bytes_in_first_block;
    let second_part_end = second_part_start + remaining_bytes;
    
    println!("Second part of chunk: bytes {} to {}", second_part_start, second_part_end);
    
    assert_eq!(
        &vec[second_part_start..second_part_end],
        &chunk_data[bytes_in_first_block..],
        "Second part of chunk data after block boundary should match"
    );
    
    // Reconstruct the original data by concatenating the parts
    let mut reconstructed = Vec::new();
    reconstructed.extend_from_slice(&vec[first_part_start..first_part_end]);
    reconstructed.extend_from_slice(&vec[second_part_start..second_part_end]);
    
    assert_eq!(reconstructed, chunk_data,
              "Reconstructed data should match original chunk data");
}

#[test]
fn test_exact_block_size_boundaries() {
    // Test writing chunks exactly at block size boundaries
    let block_size = 100u64;
    let buffer = Cursor::new(Vec::new());
    
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Calculate usable block size (what fits in a block after the header)
    let usable_block_size = (block_size - BLOCK_HEADER_SIZE) as usize;
    
    // Create a chunk of exactly the usable block size
    let data = vec![123u8; usable_block_size];
    let chunk_data = Bytes::from(data.clone());
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer
    let vec = get_buffer(writer);
    
    // Expected layout: header (24 bytes) + data (76 bytes) = 100 bytes total
    // Which is exactly one block
    assert_eq!(vec.len(), block_size as usize);
    
    // Verify the data after the header
    assert_eq!(
        &vec[BLOCK_HEADER_SIZE as usize..],
        &data[..],
        "Data should be written exactly after the header"
    );
    
    // Now try with a chunk that's 1 byte larger than usable block size
    // This should cause it to cross a block boundary, requiring a second header
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Create data exactly 1 byte larger than usable block size
    let overflow_data = vec![42u8; usable_block_size + 1];
    let chunk_data = Bytes::from(overflow_data.clone());
    
    // Write the chunk
    writer.write_chunk(chunk_data).unwrap();
    
    // Get the buffer
    let vec = get_buffer(writer);
    
    // Expected:
    // - Header at 0 (24 bytes)
    // - First block data (76 bytes)
    // - Header at block boundary (24 bytes) 
    // - Last 1 byte of data
    let expected_size = block_size as usize + BLOCK_HEADER_SIZE as usize + 1;
    assert_eq!(vec.len(), expected_size);
    
    // Verify first part of data
    assert_eq!(
        &vec[BLOCK_HEADER_SIZE as usize..block_size as usize],
        &overflow_data[..usable_block_size],
        "First block data should match"
    );
    
    // Verify the last byte after the second header
    assert_eq!(
        vec[block_size as usize + BLOCK_HEADER_SIZE as usize],
        overflow_data[usable_block_size],
        "Last byte after second header should match"
    );
}

#[test]
fn test_write_chunk_crossing_multiple_block_boundaries() {
    // Test writing a chunk large enough to cross multiple block boundaries
    // Using a very predictable block and header size for easier debugging
    let block_size = 100u64; // Small, easy to reason about number
    let buffer = Cursor::new(Vec::new());
    
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Create a pattern-based chunk for easy verification
    // Each byte will be its position % 256, creating a recognizable pattern
    let needed_size = 250; // Cross two boundaries
    let mut pattern_data = Vec::with_capacity(needed_size);
    for i in 0..needed_size {
        pattern_data.push((i % 256) as u8);
    }
    let chunk_data = Bytes::from(pattern_data);
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    println!("Output buffer size: {}", vec.len());
    
    // Expected positions of block headers
    let header_positions = [
        0,                        // Initial header (position 0)
        block_size as usize,      // At first block boundary (position 100)
        (block_size * 2) as usize, // At second block boundary (position 200)
        (block_size * 3) as usize, // At third block boundary (position 300)
    ];
    
    // Verify the size makes sense
    let expected_data_size = chunk_data.len();
    let expected_header_count = if vec.len() == expected_data_size + 3 * BLOCK_HEADER_SIZE as usize {
        3 // 3 headers is the minimum for crossing 2 boundaries 
    } else if vec.len() == expected_data_size + 4 * BLOCK_HEADER_SIZE as usize {
        4 // 4 headers might be used in some implementations
    } else {
        panic!("Unexpected output size: {}, expected either {} or {}",
              vec.len(),
              expected_data_size + 3 * BLOCK_HEADER_SIZE as usize,
              expected_data_size + 4 * BLOCK_HEADER_SIZE as usize);
    };
    
    println!("Detected {} block headers", expected_header_count);
    
    // Now let's reconstruct the original data by skipping over the headers
    let mut reconstructed = Vec::with_capacity(chunk_data.len());
    
    // Track our position in the output buffer and input chunk
    let mut output_pos = 0;
    let mut remaining_chunk = chunk_data.len();
    let mut chunk_pos = 0;
    
    // Process each block and extract the data
    for i in 0..expected_header_count {
        // Skip the header
        let header_pos = header_positions[i] as usize;
        
        // Check if we've reached the end of our output
        if header_pos >= vec.len() {
            break;
        }
        
        // Validate we're at the expected header position
        assert_eq!(output_pos, header_pos, 
                  "Expected header at position {}, but we're at {}", header_pos, output_pos);
        
        // Skip over the header
        output_pos += BLOCK_HEADER_SIZE as usize;
        
        // Determine how much data to read before the next header (or end)
        let next_header_pos = if i < expected_header_count - 1 && (header_positions[i + 1] as usize) < vec.len() {
            header_positions[i + 1] as usize
        } else {
            vec.len() // No more headers, read to the end
        };
        
        let data_size = next_header_pos - output_pos;
        
        // Can't read more than what remains in the chunk
        let bytes_to_read = std::cmp::min(data_size, remaining_chunk);
        
        if bytes_to_read > 0 {
            // Add this section to our reconstructed data
            reconstructed.extend_from_slice(&vec[output_pos..output_pos + bytes_to_read]);
            
            // Verify this section matches the expected chunk data
            for j in 0..bytes_to_read {
                assert_eq!(vec[output_pos + j], chunk_data[chunk_pos + j], 
                          "Data mismatch at position {}", chunk_pos + j);
            }
            
            // Update tracking variables
            output_pos += bytes_to_read;
            remaining_chunk -= bytes_to_read;
            chunk_pos += bytes_to_read;
        }
    }
    
    // Final verification: we've reconstructed the entire chunk
    assert_eq!(reconstructed.len(), chunk_data.len(), 
              "Reconstructed data should have the same length as original chunk");
    assert_eq!(reconstructed, chunk_data.to_vec(),
              "Reconstructed data should match original chunk data");
    
    println!("Successfully verified chunk data across multiple block boundaries");
}

#[test]
fn test_header_values() {
    // Test to verify that the header values are correctly calculated
    let block_size = 100u64;
    let mut buffer = Cursor::new(Vec::new());
    
    // Write some recognizable initial data but not at a block boundary
    let initial_data = b"INIT_DATA";
    buffer.write_all(initial_data).unwrap();
    let initial_pos = initial_data.len() as u64;
    buffer.set_position(initial_pos);
    
    let mut writer = BlockWriter::for_append_with_config(
        buffer,
        initial_pos,
        BlockWriterConfig {
            block_size,
        }
    ).unwrap();
    
    // Create a chunk that will cross exactly one block boundary
    // First part: block_size - initial_pos bytes (to reach the boundary)
    // Second part: 50 additional bytes (after the boundary)
    let chunk_size = (block_size - initial_pos + 50) as usize;
    let chunk_data = Bytes::from(vec![b'X'; chunk_size]);
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // Extract the header values from the block header
    let header_pos = block_size as usize;
    
    // Skip header_hash (8 bytes)
    let previous_chunk_pos = header_pos + 8;
    let next_chunk_pos = previous_chunk_pos + 8;
    
    // Read the values as little-endian u64
    let previous_chunk = u64::from_le_bytes([
        vec[previous_chunk_pos],
        vec[previous_chunk_pos + 1],
        vec[previous_chunk_pos + 2],
        vec[previous_chunk_pos + 3],
        vec[previous_chunk_pos + 4],
        vec[previous_chunk_pos + 5],
        vec[previous_chunk_pos + 6],
        vec[previous_chunk_pos + 7],
    ]);
    
    let next_chunk = u64::from_le_bytes([
        vec[next_chunk_pos],
        vec[next_chunk_pos + 1],
        vec[next_chunk_pos + 2],
        vec[next_chunk_pos + 3],
        vec[next_chunk_pos + 4],
        vec[next_chunk_pos + 5],
        vec[next_chunk_pos + 6],
        vec[next_chunk_pos + 7],
    ]);
    
    // Verify the values
    // previous_chunk: distance from beginning of chunk to beginning of block
    let expected_previous_chunk = block_size - initial_pos;
    assert_eq!(previous_chunk, expected_previous_chunk,
        "previous_chunk value in the header should be distance from chunk start to block start");
    
    // next_chunk: distance from beginning of block to end of chunk
    // The specification is referring to the distance from block boundary to the end of the chunk data
    // In this case, we're writing 50 bytes after the block boundary
    // The header itself is not counted in this distance
    let expected_next_chunk = 50; 
    
    // If the test fails, print debugging info
    if next_chunk != expected_next_chunk {
        println!("Debug info for header values:");
        println!("  block_size: {}", block_size);
        println!("  initial_pos: {}", initial_pos);
        println!("  chunk_size: {}", chunk_size);
        println!("  header_pos: {}", header_pos);
        println!("  previous_chunk: {} (expected: {})", previous_chunk, expected_previous_chunk);
        println!("  next_chunk: {} (expected: {})", next_chunk, expected_next_chunk);
    }
    
    assert_eq!(next_chunk, expected_next_chunk,
        "next_chunk value in the header should be distance from block start to chunk end");
}
