//! Tests for edge cases and error handling in BlockWriter

use super::super::writer::{BlockWriter, BlockWriterConfig, BLOCK_HEADER_SIZE};
use bytes::Bytes;
use std::io::{Cursor, Seek, Write};

// A custom writer that simulates I/O errors
struct FailingWriter {
    fail_after: usize,                // Fail after writing this many bytes
    written: usize,                   // Number of bytes written so far
    data: Vec<u8>,                    // Underlying data
    fail_on_nth_write: Option<usize>, // Fail on the nth write operation
    write_count: usize,               // Count of write operations
    fail_on_flush: bool,              // Whether to fail on flush operations
}

impl FailingWriter {
    fn new(fail_after: usize) -> Self {
        Self {
            fail_after,
            written: 0,
            data: Vec::new(),
            fail_on_nth_write: None,
            write_count: 0,
            fail_on_flush: false,
        }
    }

    fn with_fail_on_flush() -> Self {
        Self {
            fail_after: usize::MAX, // Don't fail based on bytes
            written: 0,
            data: Vec::new(),
            fail_on_nth_write: None,
            write_count: 0,
            fail_on_flush: true,
        }
    }
}

impl Write for FailingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Increment write operation counter
        self.write_count += 1;

        // Check if we should fail on this specific write operation
        if let Some(n) = self.fail_on_nth_write {
            if self.write_count == n {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "Simulated interruption on specific write operation",
                ));
            }
        }

        // Check if we've written too many bytes
        if self.written >= self.fail_after {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated I/O error after byte threshold",
            ));
        }

        // Calculate how many bytes we can write before hitting the failure threshold
        let bytes_to_write = std::cmp::min(buf.len(), self.fail_after - self.written);
        self.data.extend_from_slice(&buf[..bytes_to_write]);
        self.written += bytes_to_write;

        // If we couldn't write all bytes, return an error
        if bytes_to_write < buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated partial write error",
            ));
        }

        Ok(bytes_to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.fail_on_flush {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated flush error",
            ));
        }

        if self.written >= self.fail_after {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated I/O error on flush",
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
                "Only SeekFrom::Start is supported in this test",
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

    let mut writer =
        BlockWriter::with_config(buffer, BlockWriterConfig { block_size: 100 }).unwrap();

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
            assert_eq!(
                sink.written, 50,
                "Should have written 50 bytes before failing"
            );
            assert_eq!(sink.data.len(), 50, "Should have 50 bytes of data");
        }
        _ => panic!("Expected I/O error but got {:?}", result),
    }
}

#[test]
fn test_failure_during_header_write() {
    // Test interruption specifically during block header writing
    // Block headers are 24 bytes, so we'll fail after 10 bytes
    // to interrupt in the middle of header writing
    let buffer = FailingWriter::new(10);

    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size: 64, // Small block size
        },
    )
    .unwrap();

    // Create a chunk large enough to trigger a block header write
    let data = vec![42u8; 100];
    let chunk_data = Bytes::from(data);

    // This should fail with an I/O error during header writing
    let result = writer.write_chunk(chunk_data);
    assert!(result.is_err(), "Expected an error but got success");

    match result {
        Err(crate::error::RiegeliError::Io(_)) => {
            // Verify the state of the writer
            let sink = writer.get_ref();
            assert_eq!(
                sink.written, 10,
                "Should have written 10 bytes before failing"
            );

            // In a real scenario, we'd have a partially written header which would need to be
            // detected and handled by any recovery mechanism
            assert_eq!(sink.data.len(), 10, "Should have a partial header");
        }
        _ => panic!("Expected I/O error but got {:?}", result),
    }
}

#[test]
fn test_flush_error_handling() {
    // Test handling of errors during flush operation
    let buffer = FailingWriter::with_fail_on_flush();

    let mut writer =
        BlockWriter::with_config(buffer, BlockWriterConfig { block_size: 100 }).unwrap();

    // Write a chunk successfully
    let data = vec![42u8; 30];
    let chunk_data = Bytes::from(data);
    writer.write_chunk(chunk_data).unwrap();

    // Now try to flush - should fail
    let result = writer.flush();
    assert!(result.is_err(), "Expected flush to fail");

    match result {
        Err(crate::error::RiegeliError::Io(_)) => {
            // Expected error type
            // Check that data was written correctly despite flush error
            let sink = writer.get_ref();
            assert!(
                sink.data.len() > 0,
                "Data should have been written despite flush error"
            );
        }
        _ => panic!("Expected I/O error but got {:?}", result),
    }
}

#[test]
fn test_practical_recovery_approach() {
    // This test demonstrates a practical approach to recovering from partial writes
    // without requiring internal state tracking in BlockWriter

    // In real-world scenarios, applications would need to:
    // 1. Keep track of logical chunk boundaries themselves
    // 2. Handle retries and resume operations at the application level

    // Step 1: Write a chunk, with a failure occurring partway through
    let block_size = 100u64;
    let failure_point = 80; // Fail after 80 bytes
    let buffer = FailingWriter::new(failure_point);

    let mut writer = BlockWriter::with_config(buffer, BlockWriterConfig { block_size }).unwrap();

    // Create a chunk that will cross block boundaries
    let chunk_data = Bytes::from(vec![0xAA; 150]);

    // This will fail partway through writing
    let result = writer.write_chunk(chunk_data.clone());
    assert!(result.is_err(), "Expected an error");

    // Capture the current state
    let failed_writer = writer.into_inner();
    let partial_data = failed_writer.data.clone();
    let written_pos = failed_writer.written;

    // Step 2: In a real application, we might need to scan the file to find the last valid
    // block header to determine where to resume.
    // For this test, we'll assume we know exactly where the write stopped.

    // Create a new buffer with the partial data for recovery
    let resume_buffer = Cursor::new(partial_data);

    // Step 3: Create a new writer starting from where we left off
    let mut recovery_writer = BlockWriter::for_append_with_config(
        resume_buffer,
        written_pos as u64, // Position to resume from
        BlockWriterConfig { block_size },
    )
    .unwrap();

    // In a practical application, we would need to keep track of what logical chunks
    // were successfully written and which were partial.
    // For this test, we'll simulate that we know the original chunk wasn't fully written
    // and we're going to retry it completely.

    // Write the full chunk again - the BlockWriter handles block headers automatically
    let result = recovery_writer.write_chunk(chunk_data);
    assert!(result.is_ok(), "Recovery write should succeed");

    // This approach requires the application to track logical chunks
    // and handle appropriate recovery strategies:
    // - Truncate to the last known good state and retry
    // - Skip already-written chunks and continue with new ones
    // - Attempt to resume partial chunks if chunk format allows
}

#[test]
fn test_incremental_writing() {
    // Test a strategy where we write data in smaller increments to
    // minimize data loss in case of failures

    const CHUNK_SIZE: usize = 150;
    const INCREMENT_SIZE: usize = 50;

    // Create a writer that will fail after writing a certain amount
    let buffer = FailingWriter::new(90); // Fail after 90 bytes

    let mut writer = BlockWriter::with_config(
        buffer,
        BlockWriterConfig {
            block_size: 100, // Block size of 100 bytes
        },
    )
    .unwrap();

    // Prepare a large chunk of data
    let complete_data = vec![0xBB; CHUNK_SIZE];

    // Instead of writing the whole chunk at once, write it in smaller increments
    let mut bytes_written = 0;

    // Keep track of whether we encountered an error
    let mut had_error = false;

    while bytes_written < CHUNK_SIZE {
        // Calculate the next increment to write
        let next_size = std::cmp::min(INCREMENT_SIZE, CHUNK_SIZE - bytes_written);
        let increment =
            Bytes::from(complete_data[bytes_written..bytes_written + next_size].to_vec());

        // Try to write this increment
        let result = writer.write_chunk(increment);

        if let Err(e) = result {
            // Handle the error - in a real app, we'd record where we got to
            match e {
                crate::error::RiegeliError::Io(_) => {
                    had_error = true;
                    break;
                }
                _ => panic!("Unexpected error: {:?}", e),
            }
        } else {
            // Increment was written successfully
            bytes_written += next_size;
        }
    }

    // Verify that we hit an error as expected
    assert!(had_error, "Should have encountered an error");

    // If this were a real application, we would:
    // 1. Record that we successfully wrote bytes_written bytes
    // 2. On restart/recovery, continue from that point

    // From the failure point, we can get the partial data and position
    let failed_writer = writer.into_inner();
    let partial_data = failed_writer.data.clone();
    let written_pos = failed_writer.written;

    // For demonstration purposes, let's recover and continue
    let mut resume_buffer = Cursor::new(partial_data);
    resume_buffer.set_position(written_pos as u64);

    // Create a new writer from the point of failure
    let mut recovery_writer = BlockWriter::for_append_with_config(
        resume_buffer,
        written_pos as u64,
        BlockWriterConfig { block_size: 100 },
    )
    .unwrap();

    // Continue writing the remaining data
    while bytes_written < CHUNK_SIZE {
        let next_size = std::cmp::min(INCREMENT_SIZE, CHUNK_SIZE - bytes_written);
        let increment =
            Bytes::from(complete_data[bytes_written..bytes_written + next_size].to_vec());

        recovery_writer.write_chunk(increment).unwrap();
        bytes_written += next_size;
    }

    // Verify all data was written
    assert_eq!(bytes_written, CHUNK_SIZE, "Should have written all data");

    // This incremental approach provides better resilience against errors
    // by limiting the amount of data that needs to be rewritten if a failure occurs
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
        },
    )
    .unwrap();

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
        },
    )
    .unwrap();

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
