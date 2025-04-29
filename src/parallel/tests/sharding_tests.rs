use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

use crate::parallel::sharding::{
    Autosharder, FileSharder, FileSharderConfig, RandomRepeatingFileShardLocator, Sharder, ShardLocator
};
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
    let sharder = FileSharder::with_prefix(
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
    let sharder = FileSharder::with_prefix(
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
    let sharder = FileSharder::with_prefix(
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

#[test]
fn test_file_sharder_with_append_mode() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Create some existing files first
    std::fs::create_dir_all(&dir_path).unwrap();
    for i in 0..3 {
        let file_path = dir_path.join(format!("append_{}", i));
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(format!("existing file {}", i).as_bytes()).unwrap();
    }
    
    // Create a file sharder with append mode enabled
    let config = FileSharderConfig::new("append").with_append(true);
    let sharder = FileSharder::with_config(dir_path.clone(), config);
    
    // Create a new sink - should skip the existing files
    let mut sink = sharder.create_sink().unwrap();
    sink.write_all(b"new file").unwrap();
    sink.flush().unwrap();
    
    // Verify that a new file was created with the next index
    let file_path = dir_path.join("append_3");
    assert!(file_path.exists());
    
    // Verify that the original files still have their original content
    for i in 0..3 {
        let file_path = dir_path.join(format!("append_{}", i));
        let mut file = std::fs::File::open(&file_path).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        assert_eq!(buffer, format!("existing file {}", i).as_bytes());
    }
    
    // Verify the new file has the correct content
    let mut file = std::fs::File::open(dir_path.join("append_3")).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    assert_eq!(buffer, b"new file");
}

#[test]
fn test_random_repeating_file_shard_locator() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Create some test files
    std::fs::create_dir_all(&dir_path).unwrap();
    for i in 0..5 {
        let file_path = dir_path.join(format!("random_{}", i));
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(format!("file content {}", i).as_bytes()).unwrap();
    }
    
    // Create a random repeating locator with a fixed seed for deterministic testing
    let locator = RandomRepeatingFileShardLocator::with_seed(
        dir_path.clone(),
        "random",
        42 // fixed seed for deterministic shuffling
    ).unwrap();
    
    // Read first round of shards and verify all files are accessed exactly once
    let mut content_set = std::collections::HashSet::new();
    for _ in 0..5 {
        let mut file = locator.next_shard().unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        let content = String::from_utf8_lossy(&buffer).to_string();
        
        // Verify this is the first time we've seen this content
        assert!(content_set.insert(content));
    }
    
    // Check that we have content from all 5 files
    assert_eq!(content_set.len(), 5);
    
    // Now read second round - should get all files again in a potentially different order
    let mut second_round_set = std::collections::HashSet::new();
    for _ in 0..5 {
        let mut file = locator.next_shard().unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        let content = String::from_utf8_lossy(&buffer).to_string();
        
        // Verify this is the first time we've seen this content in the second round
        // and that this content was also in the first round
        assert!(second_round_set.insert(content.clone()));
        assert!(content_set.contains(&content));
    }
    
    // Check that we have content from all 5 files in the second round
    assert_eq!(second_round_set.len(), 5);
}

#[test]
fn test_random_repeating_file_shard_locator_with_empty_directory() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Ensure the directory exists but is empty
    std::fs::create_dir_all(&dir_path).unwrap();
    
    // Attempting to create a locator with no matching files should fail
    let result = RandomRepeatingFileShardLocator::new(
        dir_path.clone(),
        "empty"
    );
    
    // Verify we get the expected error
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(e.to_string().contains("No shards found"));
    }
}

#[test]
fn test_random_repeating_file_shard_locator_two_threads() {
    use std::sync::Arc;
    use std::thread;
    
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    
    // Create some test files
    std::fs::create_dir_all(&dir_path).unwrap();
    for i in 0..10 {
        let file_path = dir_path.join(format!("mt_{}", i));
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(format!("thread test {}", i).as_bytes()).unwrap();
    }
    
    // Create a shared locator
    let locator = Arc::new(RandomRepeatingFileShardLocator::new(
        dir_path.clone(),
        "mt"
    ).unwrap());
    
    // Create two threads that each read 10 files (full set)
    let locator1 = Arc::clone(&locator);
    let thread1 = thread::spawn(move || {
        let mut contents = Vec::new();
        for _ in 0..10 {
            let mut file = locator1.next_shard().unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();
            contents.push(String::from_utf8_lossy(&buffer).to_string());
        }
        contents
    });

    // Read thread 1 first to ensure unique files.
    let contents1 = thread1.join().unwrap();
    
    let locator2 = Arc::clone(&locator);
    let thread2 = thread::spawn(move || {
        let mut contents = Vec::new();
        for _ in 0..10 {
            let mut file = locator2.next_shard().unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();
            contents.push(String::from_utf8_lossy(&buffer).to_string());
        }
        contents
    });
    
    let contents2 = thread2.join().unwrap();
    
    // Each thread should have read all 10 files
    let set1: std::collections::HashSet<_> = contents1.iter().collect();
    let set2: std::collections::HashSet<_> = contents2.iter().collect();
    
    assert_eq!(set1.len(), 10, "Thread 1 should read all 10 unique files");
    assert_eq!(set2.len(), 10, "Thread 2 should read all 10 unique files");
    
    // The combined set should have 10 unique file contents
    let all_contents: std::collections::HashSet<_> = contents1.iter().chain(contents2.iter()).collect();
    assert_eq!(all_contents.len(), 10, "There should be exactly 10 unique file contents");
}
