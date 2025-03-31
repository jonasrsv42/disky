//! Tests for edge cases and error handling in BlockWriter

use super::super::writer::{BlockWriter, BlockWriterConfig, BLOCK_HEADER_SIZE};
use bytes::Bytes;
use std::io::{Cursor, Seek, Write};

// A custom writer that simulates I/O errors
struct FailingWriter {
    fail_after: usize, // Fail after writing this many bytes
    written: usize,    // Number of bytes written so far
    data: Vec<u8>,     // Underlying data
}

impl FailingWriter {
    fn new(fail_after: usize) -> Self {
        Self {
            fail_after,
            written: 0,
            data: Vec::new(),
        }
    }
}

impl Write for FailingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.written >= self.fail_after {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated I/O error"
            ));
        }
        
        let bytes_to_write = std::cmp::min(buf.len(), self.fail_after - self.written);
        self.data.extend_from_slice(&buf[..bytes_to_write]);
        self.written += bytes_to_write;
        
        if bytes_to_write < buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated I/O error"
            ));
        }
        
        Ok(bytes_to_write)
    }
    
    fn flush(&mut self) -> std::io::Result<()> {
        if self.written >= self.fail_after {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated I/O error"
            ));
        }
        Ok(())
    }
}

impl Seek for FailingWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            std::io::SeekFrom::Start(offset) => {
                self.written = offset as usize;
                Ok(offset)
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Only SeekFrom::Start is supported in this test"
            )),
        }
    }
    
    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.written as u64)
    }
}

#[test]
fn test_error_propagation() {
    // Test that I/O errors are properly propagated
    let buffer = FailingWriter::new(50); // Fail after 50 bytes
    
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size: 100,
        }
    ).unwrap();
    
    // Create a chunk larger than the failure threshold
    let data = vec![1u8; 100];
    let chunk_data = Bytes::from(data);
    
    // This should fail with an I/O error
    let result = writer.write_chunk(chunk_data);
    assert!(result.is_err(), "Expected an error but got success");
    
    match result {
        Err(crate::error::RiegeliError::Io(_)) => {
            // This is the expected error type
            // Now verify we can obtain the partial data
            let sink = writer.get_ref();
            assert_eq!(sink.written, 50, "Should have written 50 bytes before failing");
            assert_eq!(sink.data.len(), 50, "Should have 50 bytes of data");
        }
        _ => panic!("Expected I/O error but got {:?}", result),
    }
}

#[test]
fn test_writing_with_minimum_block_size() {
    // Test writing data with the minimum allowed block size
    let min_block_size = BLOCK_HEADER_SIZE * 2;
    let buffer = Cursor::new(Vec::new());
    
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size: min_block_size,
        }
    ).unwrap();
    
    // Create test data that will fill exactly one block's worth of data
    // At min_block_size, we should be able to write (min_block_size - BLOCK_HEADER_SIZE) bytes
    // before hitting the next block boundary
    let usable_block_size = (min_block_size - BLOCK_HEADER_SIZE) as usize;
    let data = vec![42u8; usable_block_size]; // Fill with a constant value
    
    let chunk_data = Bytes::from(data.clone());
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // Check the size
    let expected_size = BLOCK_HEADER_SIZE as usize + usable_block_size;
    assert_eq!(vec.len(), expected_size);
    
    // Verify the data after the header
    assert_eq!(
        &vec[BLOCK_HEADER_SIZE as usize..],
        &data[..],
        "Data should be written exactly after the header"
    );
    
    // Now test with data that crosses a block boundary with minimum block size
    // This is a critical test because with minimum block size, we're right at the edge
    // of what could cause cascading headers
    let buffer = Cursor::new(Vec::new());
    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size: min_block_size,
        }
    ).unwrap();
    
    // Create data slightly larger than one block's worth
    let crossing_data = vec![42u8; usable_block_size + 10]; // 10 bytes more than one block
    let chunk_data = Bytes::from(crossing_data.clone());
    
    // Write the chunk
    writer.write_chunk(chunk_data.clone()).unwrap();
    
    // Get the buffer
    let buffer = writer.into_inner();
    let vec = buffer.into_inner();
    
    // Should have: initial header + first block data + second header + remaining data
    let expected_size = 2 * BLOCK_HEADER_SIZE as usize + crossing_data.len();
    assert_eq!(vec.len(), expected_size);
    
    // Verify the first block of data
    assert_eq!(
        &vec[BLOCK_HEADER_SIZE as usize..min_block_size as usize],
        &crossing_data[..usable_block_size],
        "First block data should match"
    );
    
    // Verify the second block of data
    assert_eq!(
        &vec[(min_block_size + BLOCK_HEADER_SIZE) as usize..],
        &crossing_data[usable_block_size..],
        "Second block data should match"
    );
}
