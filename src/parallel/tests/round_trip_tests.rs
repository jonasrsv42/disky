use std::io::{Cursor, Seek, SeekFrom};
use bytes::Bytes;

use crate::parallel::writer::{ParallelWriter, ParallelWriterConfig};
use crate::writer::{RecordWriter, RecordWriterConfig};
use crate::reader::{RecordReader, DiskyPiece};
use crate::error::Result;

#[test]
fn test_parallel_writer_round_trip() -> Result<()> {
    // Test data
    let test_records = vec![
        b"record 1".to_vec(),
        b"this is record 2 with more data".to_vec(),
        b"record 3 with even more data content to verify".to_vec(),
        b"record 4 short".to_vec(),
        b"record 5 final record with lots of data to ensure proper boundaries".to_vec(),
    ];
    
    // Create a shared cursor for writing
    let cursor = Cursor::new(Vec::new());
    
    // Create and configure a single writer (for simplicity)
    let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default())?;
    let writers = vec![Box::new(writer)];
    
    // Create the parallel writer
    let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default())?;
    
    // Write records synchronously
    for record in &test_records {
        parallel_writer.write_record(record)?;
    }
    
    // Flush the writer to ensure all data is written
    parallel_writer.flush()?;
    
    // Get access to the writers, using process_all_resources to safely access them
    let mut written_data = Vec::new();
    
    // Use process_all_resources to safely get the data from the writer
    parallel_writer.get_resource_pool().process_all_resources(|resource| {
        // Access the writer's cursor
        let cursor = resource.writer.get_block_writer().get_ref();
        // Clone the data
        written_data = cursor.get_ref().clone();
        Ok(())
    })?;
    
    // Close the writer now that we have the data
    parallel_writer.close()?;
    
    // Create a reader to read back the data
    let mut read_cursor = Cursor::new(written_data);
    read_cursor.seek(SeekFrom::Start(0))?;
    let mut reader = RecordReader::new(read_cursor)?;
    
    // Read back all records and verify they match
    for expected_record in &test_records {
        match reader.next_record()? {
            DiskyPiece::Record(bytes) => {
                assert_eq!(bytes, Bytes::from(expected_record.clone()), 
                           "Read record doesn't match expected record");
            }
            DiskyPiece::EOF => {
                panic!("Reached EOF before reading all expected records");
            }
        }
    }
    
    // Verify we've reached the end
    match reader.next_record()? {
        DiskyPiece::EOF => {
            // This is expected
        }
        DiskyPiece::Record(_) => {
            panic!("Expected EOF but got another record");
        }
    }
    
    Ok(())
}