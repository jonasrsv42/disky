use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

use crate::parallel::sharding::{Autosharder, FileSharder, Sharder};
use crate::error::Result;

// Helper function to create a simple memory-based sharder
fn create_memory_sharder() -> impl Sharder<Cursor<Vec<u8>>> {
    Autosharder::new(|| {
        Ok(Cursor::new(Vec::new()))
    })
}

#[test]
fn test_memory_sharder_basic() {
    let sharder = create_memory_sharder();
    
    // Create a sink
    let mut sink = sharder.create_sink().unwrap();
    
    // Write some data
    sink.write_all(b"hello world").unwrap();
    sink.flush().unwrap();
    
    // Verify the data
    sink.seek(SeekFrom::Start(0)).unwrap();
    let mut buffer = Vec::new();
    sink.read_to_end(&mut buffer).unwrap();
    assert_eq!(buffer, b"hello world");
}

#[test]
fn test_memory_sharder_multiple_sinks() {
    let sharder = create_memory_sharder();
    
    // Create multiple sinks
    let mut sinks = Vec::new();
    for i in 0..5 {
        let mut sink = sharder.create_sink().unwrap();
        
        // Write unique data to each sink
        sink.write_all(format!("sink {}", i).as_bytes()).unwrap();
        sink.flush().unwrap();
        
        sinks.push(sink);
    }
    
    // Verify data in each sink
    for (i, mut sink) in sinks.into_iter().enumerate() {
        sink.seek(SeekFrom::Start(0)).unwrap();
        let mut buffer = Vec::new();
        sink.read_to_end(&mut buffer).unwrap();
        assert_eq!(buffer, format!("sink {}", i).as_bytes());
    }
}

#[test]
fn test_file_sharder_basic() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Create a file sharder
    let sharder = FileSharder::new(
        dir_path.clone(),
        "test"
    );
    
    // Create a sink
    let mut sink = sharder.create_sink().unwrap();
    
    // Write some data
    sink.write_all(b"hello world").unwrap();
    sink.flush().unwrap();
    
    // Verify the file exists
    let file_path = dir_path.join("test_0");
    assert!(file_path.exists());
    
    // Verify the data by reading the file again from disk
    // (we can't read and write the same file handle in this test)
    drop(sink); // Close the file handle first
    
    let mut file = std::fs::File::open(file_path).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    assert_eq!(buffer, b"hello world");
}

#[test]
fn test_file_sharder_sequential_numbering() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Create a file sharder
    let sharder = FileSharder::new(
        dir_path.clone(),
        "seq"
    );
    
    // Create multiple sinks
    let mut sinks = Vec::new();
    for i in 0..3 {
        let sink = sharder.create_sink().unwrap();
        sinks.push(sink);
        
        // Verify the file exists with the correct sequential number
        let file_path = dir_path.join(format!("seq_{}", i));
        assert!(file_path.exists());
    }
    
    // Drop all file handles explicitly before tempdir cleanup
    drop(sinks);
}

#[test]
fn test_file_sharder_with_nested_directory() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().join("nested").join("folders");
    
    // Create a file sharder with a nested directory path
    let sharder = FileSharder::new(
        dir_path.clone(),
        "file"
    );
    
    // Create a sink
    let sink = sharder.create_sink().unwrap();
    
    // Verify the directory and file were created
    assert!(dir_path.exists());
    assert!(dir_path.join("file_0").exists());
    
    // Explicitly drop the file handle before tempdir cleanup
    drop(sink);
}

#[test]
fn test_custom_factory_with_state() {
    // Create a stateful factory
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    
    // Create a sharder that uses the counter
    let sharder = Autosharder::new(move || {
        let count = counter_clone.fetch_add(1, Ordering::SeqCst);
        Ok(Cursor::new(vec![count as u8; count + 1]))
    });
    
    // Create multiple sinks
    let sinks = vec![
        sharder.create_sink().unwrap(),
        sharder.create_sink().unwrap(),
        sharder.create_sink().unwrap(),
    ];
    
    // Verify the sinks have different buffer sizes based on the counter
    let buffer_size_1 = sinks[0].get_ref().len();
    let buffer_size_2 = sinks[1].get_ref().len();
    let buffer_size_3 = sinks[2].get_ref().len();
    
    assert_eq!(buffer_size_1, 1);
    assert_eq!(buffer_size_2, 2);
    assert_eq!(buffer_size_3, 3);
    
    // Verify the counter value
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

#[test]
fn test_error_handling() {
    // Create a sharder that returns an error after a certain number of calls
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = counter.clone();
    
    let sharder = Autosharder::new(move || -> Result<Cursor<Vec<u8>>> {
        let mut count = counter_clone.lock().unwrap();
        *count += 1;
        
        if *count > 2 {
            // Return an error after 2 successful sinks
            Err(crate::error::DiskyError::Other("Simulated error".to_string()))
        } else {
            Ok(Cursor::new(Vec::new()))
        }
    });
    
    // First two sink creations should succeed
    let sink1 = sharder.create_sink();
    assert!(sink1.is_ok());
    
    let sink2 = sharder.create_sink();
    assert!(sink2.is_ok());
    
    // Third sink creation should fail
    let sink3 = sharder.create_sink();
    assert!(sink3.is_err());
    
    // Error message should match our simulated error
    if let Err(e) = sink3 {
        assert!(e.to_string().contains("Simulated error"));
    }
}

#[test]
fn test_file_sharder_with_custom_starting_index() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Create a file sharder with a custom starting index
    let sharder = FileSharder::with_start_index(
        dir_path.clone(),
        "custom",
        100
    );
    
    // Create a few sinks
    for i in 0..3 {
        let _sink = sharder.create_sink().unwrap();
        
        // Verify the files have the expected naming pattern
        let file_path = dir_path.join(format!("custom_{}", 100 + i));
        assert!(file_path.exists());
    }
}