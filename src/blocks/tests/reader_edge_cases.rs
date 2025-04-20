//! Tests for BlockReader edge cases

use super::super::reader::{BlockReader, BlockReaderConfig, BlocksPiece};
use super::super::utils::BLOCK_HEADER_SIZE;
use super::super::writer::{BlockWriter, BlockWriterConfig};
use super::helpers::extract_bytes;
use bytes::Bytes;
use std::io::Cursor;

#[test]
fn test_empty_and_tiny_chunks() {
    // Use a small block size for readability
    let block_size = 64u64;

    // Create a file with a mix of regular and tiny chunks
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // Write a regular chunk first
    let chunk1 = b"Regular chunk";
    writer.write_chunk(Bytes::from(chunk1.to_vec())).unwrap();

    // Write an empty chunk - this should be ignored by the writer
    let empty_chunk = b"";
    writer
        .write_chunk(Bytes::from(empty_chunk.to_vec()))
        .unwrap();

    // Write a tiny chunk (1 byte)
    let tiny_chunk = b"X";
    writer
        .write_chunk(Bytes::from(tiny_chunk.to_vec()))
        .unwrap();

    // Write another empty chunk - this should also be ignored
    writer
        .write_chunk(Bytes::from(empty_chunk.to_vec()))
        .unwrap();

    // Write final regular chunk
    let chunk3 = b"Final regular chunk";
    writer.write_chunk(Bytes::from(chunk3.to_vec())).unwrap();

    writer.flush().unwrap();
    let file_data = writer.into_inner().into_inner();

    // Now read back the chunks
    let cursor = Cursor::new(&file_data);
    let mut reader = BlockReader::with_config(cursor, BlockReaderConfig { block_size }).unwrap();

    // Read the first regular chunk
    let block_piece = reader.read_chunks().unwrap();
    let read_chunk1 = extract_bytes(block_piece);
    assert_eq!(&read_chunk1[..], chunk1);

    // Read the next chunk, which may contain both the tiny chunk and final chunk
    // because they might be in the same block and combined by the reader
    let block_piece = reader.read_chunks().unwrap();
    let read_next = extract_bytes(block_piece);

    // Create the expected combined chunk
    let mut combined = Vec::new();
    combined.extend_from_slice(tiny_chunk);
    combined.extend_from_slice(chunk3);

    // Check if the returned data contains the combined chunks
    assert_eq!(
        &read_next[..],
        &combined[..],
        "Expected combined tiny+final chunk"
    );

    // Verify we've reached EOF
    let result = reader.read_chunks();
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), BlocksPiece::EOF));
}

#[test]
fn test_file_boundary_edge_cases() {
    // Use a small block size for readability
    let block_size = 64u64;
    let usable_block_size = block_size - BLOCK_HEADER_SIZE;

    // Create a file with chunks ending exactly at block boundaries
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // First, write a complete block to ensure we start at a clean boundary
    let padding = vec![b'P'; usable_block_size as usize];
    writer.write_chunk(Bytes::from(padding.clone())).unwrap();

    // Verify we're at a block boundary
    let pos = writer.current_position();
    assert_eq!(
        pos % block_size,
        0,
        "Not at block boundary: position {} not divisible by block_size {}",
        pos,
        block_size
    );

    // Write a chunk that's exactly one usable block size
    let one_block_chunk = vec![b'Y'; usable_block_size as usize];
    writer
        .write_chunk(Bytes::from(one_block_chunk.clone()))
        .unwrap();

    // Verify we're at a block boundary again
    let pos = writer.current_position();
    assert_eq!(
        pos % block_size,
        0,
        "Not at block boundary: position {} not divisible by block_size {}",
        pos,
        block_size
    );

    // Write a tiny chunk
    let tiny_chunk = b"Z";
    writer
        .write_chunk(Bytes::from(tiny_chunk.to_vec()))
        .unwrap();

    writer.flush().unwrap();
    let file_data = writer.into_inner().into_inner();

    // Now read the chunks back
    let cursor = Cursor::new(&file_data);
    let mut reader = BlockReader::with_config(cursor, BlockReaderConfig { block_size }).unwrap();

    // Read the padding chunk
    let block_piece = reader.read_chunks().unwrap();
    let read_padding = extract_bytes(block_piece);
    assert_eq!(&read_padding[..], &padding[..]);

    // Read the one block chunk
    let block_piece = reader.read_chunks().unwrap();
    let read_one_block = extract_bytes(block_piece);
    assert_eq!(&read_one_block[..], &one_block_chunk[..]);

    // Read the tiny chunk
    let block_piece = reader.read_chunks().unwrap();
    let read_tiny = extract_bytes(block_piece);
    assert_eq!(&read_tiny[..], tiny_chunk);

    // Verify we've reached EOF
    let result = reader.read_chunks();
    assert!(result.is_ok());
    assert!(matches!(
        result.unwrap(),
        BlocksPiece::EOF
    ));
}

