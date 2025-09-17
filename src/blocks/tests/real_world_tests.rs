//! Tests for real-world use cases of BlockWriter

use super::super::utils::BLOCK_HEADER_SIZE;
use super::super::writer::{BlockWriter, BlockWriterConfig};
use super::helpers::get_buffer;
use std::io::Cursor;

#[test]
fn test_write_chunks_sequentially() {
    // Start with a fresh buffer
    let buffer = Cursor::new(Vec::new());

    // Use a block size large enough to avoid additional block boundaries for this test
    let block_size = 200u64; // > 2 * BLOCK_HEADER_SIZE (24) and larger than our total data

    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // Write first chunk with a clear pattern
    let chunk1 = b"FIRST_CHUNK_DATA";
    writer.write_chunk(chunk1).unwrap();

    // Write second chunk with a different pattern
    let chunk2 = b"SECOND_CHUNK_WITH_DIFFERENT_DATA";
    writer.write_chunk(chunk2).unwrap();

    // Get the buffer and check its contents
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();

    println!("Chunk1 size: {}", chunk1.len());
    println!("Chunk2 size: {}", chunk2.len());
    println!("Block header size: {}", BLOCK_HEADER_SIZE);
    println!("Actual size: {}", vec.len());

    // The buffer should contain an initial header + both chunks sequentially
    // (since we're starting at position 0)
    let expected_size = BLOCK_HEADER_SIZE as usize + chunk1.len() + chunk2.len();
    assert_eq!(
        vec.len(),
        expected_size,
        "Output should contain header + both chunks"
    );

    // Check that the chunks are in the right order after the header
    let header_size = BLOCK_HEADER_SIZE as usize;
    let chunk1_start = header_size;
    let chunk1_end = chunk1_start + chunk1.len();
    let chunk2_start = chunk1_end;
    let chunk2_end = chunk2_start + chunk2.len();

    assert_eq!(
        &vec[chunk1_start..chunk1_end],
        chunk1,
        "First chunk should be written correctly after header"
    );
    assert_eq!(
        &vec[chunk2_start..chunk2_end],
        chunk2,
        "Second chunk should be written correctly after first chunk"
    );
}

#[test]
fn test_many_small_sequential_chunks() {
    // Test writing many small chunks back-to-back
    let buffer = Cursor::new(Vec::new());
    let block_size = 100u64;

    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // Create several small chunks
    let chunks = vec![
        &b"Chunk1"[..],
        &b"Chunk2"[..],
        &b"Chunk3"[..],
        &b"Chunk4"[..],
        &b"Chunk5"[..],
    ];

    // Write all chunks
    for chunk in chunks.iter() {
        writer.write_chunk(chunk).unwrap();
    }

    // Get the buffer
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();

    // We expect a header at position 0, then all chunks back-to-back
    let expected_size =
        chunks.iter().fold(0, |acc, chunk| acc + chunk.len()) + BLOCK_HEADER_SIZE as usize;

    assert_eq!(
        vec.len(),
        expected_size,
        "Output should include all chunks plus one header at the beginning"
    );

    // Verify each chunk
    let mut pos = BLOCK_HEADER_SIZE as usize;
    for chunk in chunks.iter() {
        let end_pos = pos + chunk.len();
        assert_eq!(
            &vec[pos..end_pos],
            &chunk[..],
            "Chunk data should be written correctly"
        );
        pos = end_pos;
    }
}

#[test]
fn test_very_large_chunk_many_boundaries() {
    // Test writing a large chunk that spans many block boundaries
    // Use a small block size to ensure we cross many boundaries
    let block_size = 64u64; // Small block size to trigger many boundaries
    let buffer = Cursor::new(Vec::new());

    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // Create a large chunk with a pattern that makes it easy to verify
    // Size chosen to cross multiple block boundaries
    let chunk_size = (block_size * 10) as usize; // 10 blocks worth of data
    let mut pattern_data = Vec::with_capacity(chunk_size);

    // Fill with a pattern where each byte is its index % 256
    for i in 0..chunk_size {
        pattern_data.push((i % 256) as u8);
    }
    // Write the chunk
    writer.write_chunk(&pattern_data).unwrap();

    // Get the buffer
    let vec = get_buffer(writer);

    // For large chunks, we need to precisely calculate how many headers will be inserted
    // This depends on both the original data size and where headers are inserted

    // When we write data with headers, each header takes up space in a block, reducing the
    // amount of actual data we can fit in each block. This creates a cascading effect.

    // Step 1: Calculate how many bytes of actual data fit in each block
    let data_per_block = block_size - BLOCK_HEADER_SIZE;

    // Step 2: Calculate how many blocks we'll need for all our data
    // The first block starts with a header, and then each subsequent block also has a header
    let mut total_headers = 1; // Start with initial header at position 0
    let mut remaining_data = chunk_size;
    let mut total_size = 0;

    // Calculate header and data distribution
    while remaining_data > 0 {
        let data_in_this_block = std::cmp::min(remaining_data, data_per_block as usize);
        total_size += BLOCK_HEADER_SIZE as usize + data_in_this_block;
        remaining_data -= data_in_this_block;

        // If we have more data, we'll need another header
        if remaining_data > 0 {
            total_headers += 1;
        }
    }

    println!("Block size: {}", block_size);
    println!("Data per block: {}", data_per_block);
    println!("Chunk size: {}", chunk_size);
    println!("Calculated header count: {}", total_headers);
    println!("Calculated total size: {}", total_size);
    println!("Actual size: {}", vec.len());

    // Verify the total size matches our calculated size
    assert_eq!(
        vec.len(),
        total_size,
        "Output size should match our calculated size including all necessary headers"
    );

    // Now reconstruct and verify the original data
    let mut reconstructed = Vec::with_capacity(chunk_size);
    let mut output_pos = 0;

    // For each block that contains data
    while output_pos < vec.len() {
        // Skip the header
        output_pos += BLOCK_HEADER_SIZE as usize;

        // Calculate how much data to read before the next header or end
        let next_header_pos = output_pos + data_per_block as usize;
        let data_end = std::cmp::min(next_header_pos, vec.len());

        // Add this block's data to our reconstruction
        if output_pos < data_end {
            reconstructed.extend_from_slice(&vec[output_pos..data_end]);
        }

        // Move to the next block
        output_pos = data_end;
    }

    // Verify reconstructed data size
    assert_eq!(
        reconstructed.len(),
        chunk_size,
        "Reconstructed data should be the same size as the original"
    );

    // Verify reconstructed data matches the original byte-by-byte
    for i in 0..chunk_size {
        assert_eq!(reconstructed[i], pattern_data[i], "Mismatch at byte {}", i);
    }

    // Full comparison
    assert_eq!(
        reconstructed, pattern_data,
        "Reconstructed data should exactly match the original"
    );
}

#[test]
fn test_write_interpretable_string_data() {
    // Start with a fresh buffer
    let buffer = Cursor::new(Vec::new());

    // Use a small block size for testing, but large enough to avoid cascading headers
    let block_size = 60u64; // > 2 * BLOCK_HEADER_SIZE (24)

    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // Create byte data with a clear, repeating pattern for easy visual debugging
    // Repeat "ABC" to create a pattern that's easy to verify visually
    let pattern = b"ABC";
    let mut data = Vec::new();
    for _ in 0..20 {
        // 60 bytes total, will cross block boundaries
        data.extend_from_slice(pattern);
    }
    let chunk_data = data;

    // Calculate where the first block boundary will be
    // Since we start at position 0, there will be a header, then data starts at BLOCK_HEADER_SIZE
    let first_header_size = BLOCK_HEADER_SIZE as usize;
    let bytes_in_first_block = (block_size - BLOCK_HEADER_SIZE) as usize;

    // Verify test assumptions
    assert!(
        chunk_data.len() > bytes_in_first_block,
        "Test chunk must cross at least one block boundary"
    );

    // Write the chunk
    writer.write_chunk(&chunk_data).unwrap();

    // Get the buffer and check its contents
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();

    println!("Block size: {}", block_size);
    println!("Bytes in first block: {}", bytes_in_first_block);
    println!("Header size: {}", BLOCK_HEADER_SIZE);
    println!("Chunk data size: {}", chunk_data.len());
    println!("Actual size: {}", vec.len());

    // The output should include:
    // 1. Initial block header at position 0
    // 2. First part of chunk data (up to block boundary)
    // 3. Another block header at block boundary
    // 4. Second part of chunk data (after block boundary)

    // Ensure we have enough data
    let minimum_expected_size = chunk_data.len() + 2 * BLOCK_HEADER_SIZE as usize;
    assert!(
        vec.len() >= minimum_expected_size,
        "Output should include chunk data + at least two block headers"
    );

    // Check first part of chunk data (after initial header, up to block boundary)
    let first_part_start = first_header_size;
    let first_part_end = block_size as usize; // End at the block boundary

    assert_eq!(
        &vec[first_part_start..first_part_end],
        &chunk_data[..bytes_in_first_block],
        "First part of data after initial header should match"
    );

    // Skip over the second block header
    let second_part_start = first_part_end + BLOCK_HEADER_SIZE as usize;

    // Check second part of chunk data (after second header)
    let remaining_bytes = chunk_data.len() - bytes_in_first_block;
    let second_part_end = second_part_start + remaining_bytes;

    assert_eq!(
        &vec[second_part_start..second_part_end],
        &chunk_data[bytes_in_first_block..],
        "Second part of data after second header should match"
    );

    // Verify that the pattern is preserved properly
    // Reconstruct the original data by combining parts from different blocks
    let mut reconstructed = Vec::new();
    reconstructed.extend_from_slice(&vec[first_part_start..first_part_end]);
    reconstructed.extend_from_slice(&vec[second_part_start..second_part_end]);

    assert_eq!(
        reconstructed,
        chunk_data.to_vec(),
        "Reconstructed data should match original chunk data"
    );
}

#[test]
fn test_appending_to_existing_file() {
    // Create a file with some existing content
    let initial_content = "This is some existing content in the file.";
    let mut buffer = Cursor::new(initial_content.as_bytes().to_vec());

    // Move to the end of the existing content
    let existing_size = initial_content.len() as u64;
    buffer.set_position(existing_size);

    // Create a BlockWriter for appending
    let mut writer = BlockWriter::for_append_with_config(
        buffer,
        existing_size, // Current position at end of existing content
        BlockWriterConfig::default(),
    )
    .unwrap();

    // Create new content to append
    let new_content = " This is new content to append.";

    // Append the new content
    writer.write_chunk(new_content.as_bytes()).unwrap();

    // Get the buffer and check its contents
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();

    // Convert to string for easier verification
    let result = std::str::from_utf8(&vec).unwrap();

    // The result should be the concatenation of both strings
    let expected = format!("{}{}", initial_content, new_content);
    assert_eq!(result, expected);
}
