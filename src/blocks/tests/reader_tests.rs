//! Tests for BlockReader functionality

use super::super::reader::{BlockReader, BlockReaderConfig};
use super::super::writer::{BlockWriter, BlockWriterConfig};
use super::super::utils::BLOCK_HEADER_SIZE;
use bytes::Bytes;
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
fn test_read_simple_chunk() {
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
    
    // Read the chunk
    let chunk = reader.read_chunk().unwrap();
    
    // Verify the chunk matches the original data
    assert_eq!(&chunk[..], test_data);
}

#[test]
fn test_read_chunk_crossing_block_boundary() {
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
    
    // Read the chunk
    let chunk = reader.read_chunk().unwrap();
    
    // Verify the chunk matches the original data
    assert_eq!(&chunk[..], &test_data[..]);
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
    
    // Read each chunk and verify
    let read_chunk1 = reader.read_chunk().unwrap();
    assert_eq!(&read_chunk1[..], chunk1);
    
    let read_chunk2 = reader.read_chunk().unwrap();
    assert_eq!(&read_chunk2[..], chunk2);
    
    let read_chunk3 = reader.read_chunk().unwrap();
    assert_eq!(&read_chunk3[..], chunk3);
}

#[test]
fn test_large_chunk_crossing_multiple_boundaries() {
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
    
    // Read the chunk
    let chunk = reader.read_chunk().unwrap();
    
    // Verify the chunk matches the original data
    assert_eq!(&chunk[..], &pattern_data[..]);
}

#[test]
fn test_skip_chunk() {
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
    
    // Read first chunk
    let read_chunk1 = reader.read_chunk().unwrap();
    assert_eq!(&read_chunk1[..], chunk1);
    
    // Skip the second chunk
    reader.skip_chunk().unwrap();
    
    // Read the third chunk
    let read_chunk3 = reader.read_chunk().unwrap();
    assert_eq!(&read_chunk3[..], chunk3);
}