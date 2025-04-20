//! Tests for BlockReader corruption recovery functionality

use super::super::reader::{BlockReader, BlockReaderConfig};
use super::super::utils::BLOCK_HEADER_SIZE;
use super::super::writer::{BlockWriter, BlockWriterConfig};
use super::helpers::extract_bytes;
use crate::error::DiskyError;
use bytes::Bytes;
use std::io::Cursor;

#[test]
fn test_recover_from_corrupted_header() {
    // Use a small block size for readability
    let block_size = 64u64;

    // Create test data with two chunks and a corrupted header between them
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // First chunk - small enough to fit in usable space of first block (block_size - BLOCK_HEADER_SIZE)
    // With block_size = 64 and BLOCK_HEADER_SIZE = 24, we have 40 bytes of usable space
    let chunk1 = b"Small first chunk";
    writer.write_chunk(Bytes::from(chunk1.to_vec())).unwrap();

    // Second chunk - this will be skipped due to corruption at the block header
    let chunk2 = b"Second chunk - will be skipped due to corruption, but must be large enough to fill block to recover next";
    writer.write_chunk(Bytes::from(chunk2.to_vec())).unwrap();

    // Third chunk (to ensure we have something to find after corrupt header)
    let chunk3 = b"Third chunk that should be readable after recovery";
    writer.write_chunk(Bytes::from(chunk3.to_vec())).unwrap();

    writer.flush().unwrap();
    let mut file_data = writer.into_inner().into_inner();

    // Find the position of the block boundary between chunks
    // It should be at an exact multiple of the block size
    let boundary_pos = block_size; // First boundary

    // Corrupt the block header at that position
    if file_data.len() > (boundary_pos + 4) as usize {
        // Corrupt a few bytes to make the hash invalid
        file_data[boundary_pos as usize] = file_data[boundary_pos as usize].wrapping_add(1);
        file_data[boundary_pos as usize + 1] = file_data[boundary_pos as usize + 1].wrapping_add(1);
    }

    // Create a reader
    let cursor = Cursor::new(&file_data);
    let mut reader = BlockReader::with_config(cursor, BlockReaderConfig { block_size }).unwrap();

    // Read the first chunk successfully - this should be completely within the first block
    // and not affected by the corrupted header at the block boundary
    let block_piece = reader.read_chunks().unwrap();
    let chunk1_read = extract_bytes(block_piece);
    assert_eq!(&chunk1_read[..], chunk1);

    // The next read should hit the corrupted header and fail
    let result = reader.read_chunks();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        DiskyError::BlockHeaderHashMismatch
    ));

    // Now try to recover
    reader.recover().unwrap();

    // After recovery, we should be able to read a chunk
    // This will either be chunk2 or chunk3, depending on whether our corruption
    // only affected the header or also damaged the chunk data
    let block_piece = reader.read_chunks().unwrap();
    let recovered_chunk = extract_bytes(block_piece);

    // Check that we recovered to a valid chunk
    // Since we corrupted the header at the start of the second chunk,
    // recovery should skip to the third chunk
    assert!(
        !recovered_chunk.is_empty(),
        "Should recover to a non-empty chunk"
    );
    assert_eq!(
        &recovered_chunk[..],
        chunk3,
        "Should recover to the third chunk"
    );
}

#[test]
fn test_recover_from_multiple_corrupted_headers() {
    // Use a small block size for readability
    let block_size = 64u64;

    // Create test data with multiple chunks and corrupted headers
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // First chunk - small enough to fit in first block
    let chunk1 = b"First chunk - before any corruption";
    writer.write_chunk(Bytes::from(chunk1.to_vec())).unwrap();

    // Second chunk - need to make this large enough to span multiple blocks
    // to ensure it forces the next chunk to start at a block boundary
    let chunk2 = b"Second chunk - will be skipped due to first corruption. This chunk is intentionally made very large to ensure it spans multiple blocks and forces proper alignment of subsequent chunks at block boundaries. We want to make sure the next chunk starts at a clean block boundary for testing.";
    writer.write_chunk(Bytes::from(chunk2.to_vec())).unwrap();

    // Third chunk - also make this large to ensure fourth chunk starts at a block boundary
    let chunk3 = b"Third chunk - will be skipped due to second corruption. This chunk is also made large to ensure it spans multiple blocks and forces the fourth chunk to start at a clean block boundary for testing. We need to have proper alignment for our test to work correctly.";
    writer.write_chunk(Bytes::from(chunk3.to_vec())).unwrap();

    // Fourth chunk that we expect to read after recovering from both corruptions
    let chunk4 = b"Fourth chunk - will be readable after recovery";
    writer.write_chunk(Bytes::from(chunk4.to_vec())).unwrap();

    writer.flush().unwrap();
    let mut file_data = writer.into_inner().into_inner();

    // Find where the block headers should be by checking file size
    // Due to the large chunks, we should have multiple blocks in the file
    assert!(
        file_data.len() > (2 * block_size) as usize,
        "File not large enough for test, expected > {} bytes, got {} bytes",
        2 * block_size,
        file_data.len()
    );

    // Find the first block boundary where we expect a header
    let first_boundary_pos = block_size;
    // Make sure it's a valid position in the file
    assert!(
        file_data.len() > (first_boundary_pos + 8) as usize,
        "File too small to corrupt first header"
    );

    // Corrupt the first header
    file_data[first_boundary_pos as usize] = file_data[first_boundary_pos as usize].wrapping_add(1);
    file_data[first_boundary_pos as usize + 1] =
        file_data[first_boundary_pos as usize + 1].wrapping_add(1);

    // Find a later block boundary to corrupt - we'll use multiple of block_size
    // since chunks are large and should cross multiple blocks
    let second_boundary_pos = 3 * block_size; // Use third block to ensure separation

    // Make sure it's a valid position in the file
    assert!(
        file_data.len() > (second_boundary_pos + 8) as usize,
        "File too small to corrupt second header, len: {}, need: {}",
        file_data.len(),
        second_boundary_pos + 8
    );

    // Corrupt the second header
    file_data[second_boundary_pos as usize] =
        file_data[second_boundary_pos as usize].wrapping_add(1);
    file_data[second_boundary_pos as usize + 1] =
        file_data[second_boundary_pos as usize + 1].wrapping_add(1);

    // Create a reader
    let cursor = Cursor::new(&file_data);
    let mut reader = BlockReader::with_config(cursor, BlockReaderConfig { block_size }).unwrap();

    // Read the first chunk successfully
    let block_piece = reader.read_chunks().unwrap();
    let chunk1_read = extract_bytes(block_piece);
    assert_eq!(&chunk1_read[..], chunk1);

    // The next read should hit the first corrupted header and fail
    let result = reader.read_chunks();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        DiskyError::BlockHeaderHashMismatch
    ));

    // Recover from the first corruption
    reader.recover().unwrap();

    // Try to read again, this may hit the second corrupted header
    let result = reader.read_chunks();
    if result.is_err() {
        // If we hit another corrupted header, recover again
        assert!(matches!(
            result.unwrap_err(),
            DiskyError::BlockHeaderHashMismatch
        ));
        reader.recover().unwrap();
    }

    // Now we should be able to read the fourth chunk
    let chunk4_read = extract_bytes(reader.read_chunks().unwrap());
    assert_eq!(&chunk4_read[..], chunk4);
}

#[test]
fn test_recover_from_previous_chunk_inconsistency() {
    // Use a small block size for readability
    let block_size = 64u64;

    // First we'll create a file with a chunk that got truncated (simulating partial write)
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // Write a large chunk that crosses into multiple blocks
    let large_chunk = vec![b'A'; (block_size * 2) as usize];
    writer.write_chunk(Bytes::from(large_chunk)).unwrap();
    writer.flush().unwrap();

    // Get the file data but truncate it to simulate an incomplete write
    // Cut it off in the middle of the chunk, after the first block
    let mut truncated_file = writer.into_inner().into_inner();
    truncated_file.truncate((block_size + 40) as usize); // Only keep a bit after the first block

    // Now create a new writer that appends to this truncated file
    let append_pos = truncated_file.len() as u64; // Position to append from
    let buffer2 = Cursor::new(truncated_file);
    let mut writer2 =
        BlockWriter::for_append_with_config(buffer2, append_pos, BlockWriterConfig { block_size })
            .unwrap();

    // Write a new chunk that starts a new logical unit
    // Make it large enough to ensure it crosses into a new block boundary
    let usable_block_size = block_size - BLOCK_HEADER_SIZE;
    // Create a chunk larger than the usable size of a block to ensure it crosses boundary
    let new_chunk = vec![b'B'; (usable_block_size + 20) as usize];
    writer2.write_chunk(Bytes::from(new_chunk.clone())).unwrap();
    writer2.flush().unwrap();

    // Get the completed file
    let file_data = writer2.into_inner().into_inner();

    // Now create a reader for this file
    let cursor = Cursor::new(&file_data);
    let mut reader = BlockReader::with_config(cursor, BlockReaderConfig { block_size }).unwrap();

    // When reading, we should hit an inconsistency at the block boundary where
    // the original chunk was truncated and the new chunk starts
    let result = reader.read_chunks();

    // This should fail, but the error could be either a hash mismatch or an inconsistency
    // depending on exactly where the truncation happened
    assert!(result.is_err());
    match result {
        Ok(_) => panic!("Expected corruption not OK."),
        Err(err) => match err {
            DiskyError::BlockHeaderInconsistency(e) => assert_eq!(
                e,
                "Block header previous_chunk value mismatch: expected 128, got 24"
            ),
            err => panic!("Got unexpected error {:?}", err),
        },
    }

    // Now try to recover
    reader.recover().unwrap();

    // After recovery, we should be able to read the new chunk
    let block_piece = reader.read_chunks().unwrap();
    let recovered_chunk = extract_bytes(block_piece);
    assert_eq!(&recovered_chunk[..], new_chunk);
}

#[test]
fn test_recover_at_end_of_file() {
    // Use a small block size for readability
    let block_size = 64u64;

    // Create a simple file with one chunk
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();
    writer
        .write_chunk(Bytes::from(b"Single chunk".to_vec()))
        .unwrap();
    writer.flush().unwrap();
    let mut file_data = writer.into_inner().into_inner();

    // Add padding to ensure we reach a block boundary
    let current_size = file_data.len() as u64;
    let next_boundary = ((current_size / block_size) + 1) * block_size;
    let padding_needed = next_boundary - current_size;

    file_data.extend(vec![0; padding_needed as usize]);

    // Now we're at a block boundary, add corrupted header
    let corrupted_header = vec![0xFF; BLOCK_HEADER_SIZE as usize]; // Completely invalid header
    file_data.extend_from_slice(&corrupted_header);

    // Create a reader
    let cursor = Cursor::new(&file_data);
    let mut reader = BlockReader::with_config(cursor, BlockReaderConfig { block_size }).unwrap();

    // Read the first chunk successfully
    let chunk = extract_bytes(reader.read_chunks().unwrap());
    assert_eq!(&chunk[..], b"Single chunk");

    // Try to read again, it should hit the corrupted header and error
    let result = reader.read_chunks();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        DiskyError::BlockHeaderHashMismatch
    ));

    // Try to recover, which should try to find the next valid chunk but hit EOF
    let recovery_result = reader.recover();
    assert!(recovery_result.is_err());
    assert!(matches!(
        recovery_result.unwrap_err(),
        DiskyError::UnexpectedEof
    ));
}
