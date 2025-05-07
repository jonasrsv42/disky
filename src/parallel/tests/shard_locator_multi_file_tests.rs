use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::collections::HashSet;

use tempfile::tempdir;

use crate::error::Result;
use crate::parallel::sharding::{
    MultiPathShardLocator, RandomMultiPathShardLocator, ShardLocator
};

/// Helper function to create temporary shard files for testing
fn create_test_shards(count: usize) -> Result<(tempfile::TempDir, Vec<PathBuf>)> {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let mut file_paths = Vec::with_capacity(count);
    
    // Create the test files
    for i in 0..count {
        let file_path = temp_dir.path().join(format!("test_shard_{}", i));
        let mut file = File::create(&file_path)?;
        
        // Write some data to the file to make it unique
        write!(file, "This is test shard {}", i)?;
        
        file_paths.push(file_path);
    }
    
    Ok((temp_dir, file_paths))
}

/// Read the content of a file for verification
fn read_file_content(mut file: File) -> Result<String> {
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content)
}

#[test]
fn test_multi_path_shard_locator_basic() -> Result<()> {
    // Create test files
    let (temp_dir, file_paths) = create_test_shards(3)?;
    
    // Create the locator
    let locator = MultiPathShardLocator::new(file_paths)?;
    
    // Check the estimated shard count
    assert_eq!(locator.estimated_shard_count(), Some(3));
    
    // Check that we can read all the shards in the expected order
    let shard1 = locator.next_shard()?;
    let content1 = read_file_content(shard1)?;
    assert_eq!(content1, "This is test shard 0");
    
    let shard2 = locator.next_shard()?;
    let content2 = read_file_content(shard2)?;
    assert_eq!(content2, "This is test shard 1");
    
    let shard3 = locator.next_shard()?;
    let content3 = read_file_content(shard3)?;
    assert_eq!(content3, "This is test shard 2");
    
    // Make sure we get NoMoreShards after reading all shards
    match locator.next_shard() {
        Err(crate::error::DiskyError::NoMoreShards) => (),
        other => panic!("Expected NoMoreShards, got {:?}", other),
    }
    
    // Clean up
    drop(temp_dir);
    
    Ok(())
}

#[test]
fn test_multi_path_shard_locator_empty() {
    // Test that we get an error with empty paths
    let empty_paths: Vec<PathBuf> = Vec::new();
    match MultiPathShardLocator::new(empty_paths) {
        Err(crate::error::DiskyError::Other(msg)) if msg.contains("No shard paths provided") => (),
        other => panic!("Expected error for empty paths, got {:?}", other),
    }
}

#[test]
fn test_multi_path_shard_locator_nonexistent() {
    // Test that we get an error with nonexistent paths
    let nonexistent_path = PathBuf::from("/this/path/does/not/exist");
    match MultiPathShardLocator::new(vec![nonexistent_path]) {
        Err(crate::error::DiskyError::Other(msg)) if msg.contains("does not exist") => (),
        other => panic!("Expected error for nonexistent path, got {:?}", other),
    }
}

#[test]
fn test_random_multi_path_shard_locator() -> Result<()> {
    // Create test files
    let (temp_dir, file_paths) = create_test_shards(5)?;
    
    // Create the locator with a fixed seed for deterministic testing
    let locator = RandomMultiPathShardLocator::with_seed(file_paths.clone(), 42)?;
    
    // Check the estimated shard count
    assert_eq!(locator.estimated_shard_count(), Some(5));
    
    // Read all shards once and verify that we've seen each one exactly once
    let mut seen_contents = HashSet::new();
    
    for _ in 0..5 {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard)?;
        seen_contents.insert(content);
    }
    
    // Verify that we've seen all shards
    assert_eq!(seen_contents.len(), 5);
    
    // Now let's check that the locator repeats after exhausting all shards
    let first_shard = locator.next_shard()?;
    let first_content = read_file_content(first_shard)?;
    
    // Verify that first_content is in our seen_contents set
    assert!(seen_contents.contains(&first_content));
    
    // Clean up
    drop(temp_dir);
    
    Ok(())
}

#[test]
fn test_random_multi_path_shard_locator_repeatability() -> Result<()> {
    // Create test files
    let (temp_dir, file_paths) = create_test_shards(3)?;
    
    // Create two locators with the same seed
    let locator1 = RandomMultiPathShardLocator::with_seed(file_paths.clone(), 42)?;
    let locator2 = RandomMultiPathShardLocator::with_seed(file_paths.clone(), 42)?;
    
    // They should produce the same sequence
    for _ in 0..3 {
        let shard1 = locator1.next_shard()?;
        let shard2 = locator2.next_shard()?;
        
        let content1 = read_file_content(shard1)?;
        let content2 = read_file_content(shard2)?;
        
        assert_eq!(content1, content2);
    }
    
    // Clean up
    drop(temp_dir);
    
    Ok(())
}

#[test]
fn test_random_multi_path_shard_locator_different_seeds() -> Result<()> {
    // Create test files
    let (temp_dir, file_paths) = create_test_shards(5)?;
    
    // Create two locators with different seeds
    let locator1 = RandomMultiPathShardLocator::with_seed(file_paths.clone(), 42)?;
    let locator2 = RandomMultiPathShardLocator::with_seed(file_paths.clone(), 43)?;
    
    // Read all shards from both locators
    let mut sequence1 = Vec::new();
    let mut sequence2 = Vec::new();
    
    for _ in 0..5 {
        let shard1 = locator1.next_shard()?;
        let content1 = read_file_content(shard1)?;
        sequence1.push(content1);
        
        let shard2 = locator2.next_shard()?;
        let content2 = read_file_content(shard2)?;
        sequence2.push(content2);
    }
    
    // The sequences should not be the same (this could theoretically fail with a very small
    // probability if the RNG produces the same sequence for different seeds, but it's very unlikely)
    assert_ne!(sequence1, sequence2);
    
    // However, both sequences should contain all the shards
    let set1: HashSet<_> = sequence1.into_iter().collect();
    let set2: HashSet<_> = sequence2.into_iter().collect();
    
    assert_eq!(set1.len(), 5);
    assert_eq!(set2.len(), 5);
    
    // Clean up
    drop(temp_dir);
    
    Ok(())
}

#[test]
fn test_random_multi_path_shard_locator_reshuffle() -> Result<()> {
    // Create test files
    let (temp_dir, file_paths) = create_test_shards(3)?;
    
    // Create the locator with a fixed seed
    let locator = RandomMultiPathShardLocator::with_seed(file_paths.clone(), 42)?;
    
    // Read all shards in the first cycle
    let mut first_cycle = Vec::new();
    for _ in 0..3 {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard)?;
        first_cycle.push(content);
    }
    
    // Read all shards in the second cycle
    let mut second_cycle = Vec::new();
    for _ in 0..3 {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard)?;
        second_cycle.push(content);
    }
    
    // The second cycle should have all the same shards as the first cycle,
    // but they may be in a different order due to reshuffling
    let set1: HashSet<_> = first_cycle.into_iter().collect();
    let set2: HashSet<_> = second_cycle.into_iter().collect();
    
    assert_eq!(set1, set2);
    
    // Clean up
    drop(temp_dir);
    
    Ok(())
}