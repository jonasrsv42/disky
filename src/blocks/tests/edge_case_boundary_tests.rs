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

//! Tests that validate edge cases in block boundary calculations.
//! This ensures our implementation properly follows the Riegeli specification
//! even in unusual or extreme scenarios.

use std::io::Cursor;

use crate::blocks::writer::{BlockWriter, BlockWriterConfig};
use crate::blocks::utils::BLOCK_HEADER_SIZE;

/// Test chunk end calculation for a variety of edge cases.
#[test]
fn test_chunk_end_edge_cases() {
    // Create a writer with a specific block size
    let block_size = 100; // Non-power-of-2 to better expose edge cases
    
    let cursor = Cursor::new(Vec::new());
    let config = BlockWriterConfig::with_block_size(block_size).unwrap();
    let writer = BlockWriter::with_config(cursor, config).unwrap();
    
    // Case 1: Empty chunk (0 bytes)
    let end1 = writer.compute_chunk_end(0, 0);
    assert_eq!(end1, 0, "Empty chunk should have position 0");
    
    // Case 2: Chunk that exactly fits in remaining block space
    let usable_block_size = block_size - BLOCK_HEADER_SIZE;
    let end2 = writer.compute_chunk_end(0, usable_block_size);
    assert_eq!(end2, block_size, "Chunk should end at exactly the block boundary");
    
    // Case 3: Chunk that extends exactly 1 byte beyond block boundary
    let end3 = writer.compute_chunk_end(0, usable_block_size + 1);
    // This requires a new block header, so should end at block_size + 1 + BLOCK_HEADER_SIZE
    assert_eq!(end3, block_size + 1 + BLOCK_HEADER_SIZE, 
        "Chunk that extends 1 byte over block boundary should include an additional header");
    
    // Case 4: Chunk that starts exactly at block boundary
    let end4 = writer.compute_chunk_end(block_size, 10);
    // This should just be the position + size
    assert_eq!(end4, block_size + 10 + BLOCK_HEADER_SIZE, 
        "Chunk starting at block boundary should just add its size");
    
    // Case 5: Multi-block chunk with specific size to test formula
    let start5 = 10; // Not at a boundary
    let size5 = block_size * 3; // Spans 4 blocks
    let end5 = writer.compute_chunk_end(start5, size5);
    
    // Using the formula from the Riegeli spec:
    // NumOverheadBlocks = (size + (pos + usable_block_size - 1) % block_size) / usable_block_size;
    let num_overhead_blocks = 
        (size5 + (start5 + usable_block_size - 1) % block_size) / usable_block_size;
    
    // The chunk end should be:
    // start + size + num_overhead_blocks * BLOCK_HEADER_SIZE
    let expected_end5 = start5 + size5 + (num_overhead_blocks * BLOCK_HEADER_SIZE);
    
    assert_eq!(end5, expected_end5, 
        "Complex case should match the Riegeli formula");
}

/// Test writing a chunk that starts exactly 1 byte before a block boundary
#[test]
fn test_chunk_starting_near_block_boundary() {
    // Create a writer with a specific block size
    let block_size = 64;
    
    let cursor = Cursor::new(Vec::new());
    let config = BlockWriterConfig::with_block_size(block_size).unwrap();
    let mut writer = BlockWriter::with_config(cursor, config).unwrap();
    
    // Position the writer so that the next write is 1 byte before a block boundary
    // First block header is 24 bytes, leaving 40 bytes.
    // We'll write 39 bytes of filler, then start our test chunk.
    let filler = vec![b'x'; 39];
    writer.write_chunk(&filler).unwrap();
    
    // Now write a chunk that will cross the block boundary
    // Our position should be 63 (1 byte before block boundary)
    let position = writer.position();
    assert_eq!(position, 63, "Position should be 1 byte before block boundary");
    
    // Write a chunk that will cross the boundary
    let test_chunk = vec![b'y'; 10];
    writer.write_chunk(&test_chunk).unwrap();
    writer.flush().unwrap();
    
    // Get the written data
    let data = writer.into_inner().into_inner();
    
    // Verify the data length
    // 1st block: 24-byte header + 39 bytes filler = 63 bytes
    // 2nd block: 24-byte header + 10 bytes remaining (of the 10-byte chunk)
    // Total: 63 + 24 + 10 = 97 bytes
    assert_eq!(data.len(), 97, "Data length should be 96 bytes");
    
    // Verify that the data contains our test pattern
    // First 'x' filler bytes
    for i in 24..63 {
        assert_eq!(data[i], b'x', "Byte at position {} should be 'x'", i);
    }
    
    // The next byte should be 'y' (start of our test chunk)
    assert_eq!(data[63], b'y', "Byte at position 63 should be 'y'");
    
    // After the block header, the rest of the 'y' bytes should continue
    for i in 64 + 24..96 {
        assert_eq!(data[i], b'y', "Byte at position {} should be 'y'", i);
    }
}

/// Test writing chunks that end exactly at a block boundary
#[test]
fn test_chunk_ending_at_exact_block_boundary() {
    // Create a writer with a specific block size
    let block_size = 64;
    let usable_block_size = block_size - BLOCK_HEADER_SIZE;
    
    let cursor = Cursor::new(Vec::new());
    let config = BlockWriterConfig::with_block_size(block_size).unwrap();
    let mut writer = BlockWriter::with_config(cursor, config).unwrap();
    
    // Write a chunk that will end exactly at a block boundary
    // First block is: 24-byte header + 40 bytes usable
    // We want this chunk to end at exactly 64 bytes (block boundary)
    let chunk_size = usable_block_size; // 40 bytes
    let chunk = vec![b'x'; chunk_size as usize];
    
    writer.write_chunk(&chunk).unwrap();
    writer.flush().unwrap();
    
    // Get the written data
    let data = writer.into_inner().into_inner();
    
    // Verify the data length should be exactly one block
    assert_eq!(data.len(), block_size as usize, 
        "Data length should be exactly one block ({})", block_size);
    
    // Verify next_chunk value in the block header
    // Extract the next_chunk field from the header
    let next_chunk_bytes = &data[16..24];
    let next_chunk = u64::from_le_bytes([
        next_chunk_bytes[0], next_chunk_bytes[1], next_chunk_bytes[2], next_chunk_bytes[3],
        next_chunk_bytes[4], next_chunk_bytes[5], next_chunk_bytes[6], next_chunk_bytes[7],
    ]);
    
    // next_chunk should equal the usable block size (40 bytes)
    // since that's the distance from the block beginning to chunk end
    assert_eq!(next_chunk, usable_block_size + BLOCK_HEADER_SIZE, 
        "next_chunk should be the usable block size ({})", usable_block_size);
    
    // Verify that Formula 3 holds for this case:
    // (next_chunk - 1) % block_size >= block_header_size
    assert!((next_chunk - 1) % block_size >= BLOCK_HEADER_SIZE,
        "Formula 3 violation: (next_chunk - 1) % block_size ({}) should be >= {} (block_header_size)",
        (next_chunk - 1) % block_size, BLOCK_HEADER_SIZE);
}

/// Test a complex multi-block scenario with specific boundaries
#[test]
fn test_multi_block_formula_validation() {
    // Create a writer with a specific block size
    let block_size = 64;
    
    let cursor = Cursor::new(Vec::new());
    let config = BlockWriterConfig::with_block_size(block_size).unwrap();
    let mut writer = BlockWriter::with_config(cursor, config).unwrap();
    
    // Write a complex pattern of chunks designed to exercise all boundary conditions
    
    // First chunk: Exactly fills the first block (40 bytes of data + 24-byte header)
    let chunk1 = vec![b'a'; 40];
    writer.write_chunk(&chunk1).unwrap();
    
    // Second chunk: Spans multiple blocks - 100 bytes which crosses 2 block boundaries
    let chunk2 = vec![b'b'; 100];
    writer.write_chunk(&chunk2).unwrap();
    
    // Third chunk: Small chunk (10 bytes) that fits in remaining space
    let chunk3 = vec![b'c'; 10];
    writer.write_chunk(&chunk3).unwrap();
    
    // Flush and get data
    writer.flush().unwrap();
    let data = writer.into_inner().into_inner();
    
    // Check for block boundaries and validate Formula 3 for each header
    for position in (0..data.len()).step_by(block_size as usize) {
        // Skip if we don't have at least a full header
        if position + BLOCK_HEADER_SIZE as usize > data.len() {
            continue;
        }
        
        // Extract next_chunk from the header
        let next_chunk_bytes = &data[position + 16..position + 24];
        let next_chunk = u64::from_le_bytes([
            next_chunk_bytes[0], next_chunk_bytes[1], next_chunk_bytes[2], next_chunk_bytes[3],
            next_chunk_bytes[4], next_chunk_bytes[5], next_chunk_bytes[6], next_chunk_bytes[7],
        ]);
        
        // Verify that next_chunk is non-zero (Formula 2)
        assert!(next_chunk > 0, 
            "Formula 2 violation: next_chunk at position {} should be > 0", position);
        
        // Verify that Formula 3 holds, except for very small chunks
        if next_chunk > 1 {
            assert!((next_chunk - 1) % block_size >= BLOCK_HEADER_SIZE,
                "Formula 3 violation at position {}: (next_chunk - 1) % block_size ({}) should be >= {}",
                position, (next_chunk - 1) % block_size, BLOCK_HEADER_SIZE);
        }
    }
}
