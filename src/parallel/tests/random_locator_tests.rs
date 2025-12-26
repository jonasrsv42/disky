use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use tempfile::tempdir;

use crate::error::Result;
use crate::parallel::sharding::{
    RandomMultiPathShardLocator, RandomRepeatingFileShardLocator, ShardLocator,
};

// Helper function to create test shards
fn create_test_shards(dir_path: &std::path::Path, count: usize) -> Result<()> {
    // Create shard files
    for i in 0..count {
        let file_path = dir_path.join(format!("shard_{}", i));
        let mut file = std::fs::File::create(file_path)?;
        std::io::Write::write_fmt(&mut file, format_args!("Shard content {}", i))?;
    }
    Ok(())
}

// Helper function to read file content
fn read_file_content(mut file: File) -> Result<String> {
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content)
}

#[test]
fn test_random_locator_reshuffle_cycles() -> Result<()> {
    // Create a temporary directory
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Create several shard files (enough to make it statistically unlikely
    // that we'd get the same order by chance)
    let shard_count = 10;
    create_test_shards(&dir_path, shard_count)?;

    // Create a random locator with a fixed seed for deterministic testing
    let locator = RandomRepeatingFileShardLocator::with_seed(dir_path, "shard", 42)?;

    // Read all shards in the first cycle and record their order
    let mut first_cycle_order = Vec::new();
    for _ in 0..shard_count {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard.source)?;
        first_cycle_order.push(content);
    }

    // Read all shards in the second cycle and record their order
    let mut second_cycle_order = Vec::new();
    for _ in 0..shard_count {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard.source)?;
        second_cycle_order.push(content);
    }

    // Read all shards in the third cycle and record their order
    let mut third_cycle_order = Vec::new();
    for _ in 0..shard_count {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard.source)?;
        third_cycle_order.push(content);
    }

    // Verify that each cycle contains all the expected shards
    let expected_shards: HashSet<_> = (0..shard_count)
        .map(|i| format!("Shard content {}", i))
        .collect();

    let first_cycle_set: HashSet<_> = first_cycle_order.iter().cloned().collect();
    let second_cycle_set: HashSet<_> = second_cycle_order.iter().cloned().collect();
    let third_cycle_set: HashSet<_> = third_cycle_order.iter().cloned().collect();

    assert_eq!(first_cycle_set, expected_shards);
    assert_eq!(second_cycle_set, expected_shards);
    assert_eq!(third_cycle_set, expected_shards);

    // Check that all three cycles have different orders
    assert!(
        first_cycle_order != second_cycle_order,
        "First and second cycles had identical shard orders, which suggests the RNG seed isn't changing between reshuffles"
    );

    assert!(
        second_cycle_order != third_cycle_order,
        "Second and third cycles had identical shard orders, which suggests the RNG seed isn't changing between reshuffles"
    );

    assert!(
        first_cycle_order != third_cycle_order,
        "First and third cycles had identical shard orders, which suggests the RNG seed isn't changing between reshuffles"
    );

    // For a more rigorous test, check that the order changes by comparing positions of each shard
    // across cycles
    let mut position_changes = HashMap::new();

    for (shard_content, _) in expected_shards.iter().map(|s| (s, ())) {
        let pos1 = first_cycle_order
            .iter()
            .position(|s| s == shard_content)
            .unwrap();
        let pos2 = second_cycle_order
            .iter()
            .position(|s| s == shard_content)
            .unwrap();
        let pos3 = third_cycle_order
            .iter()
            .position(|s| s == shard_content)
            .unwrap();

        // Count position changes
        if pos1 != pos2 {
            *position_changes.entry(shard_content).or_insert(0) += 1;
        }
        if pos2 != pos3 {
            *position_changes.entry(shard_content).or_insert(0) += 1;
        }
        if pos1 != pos3 {
            *position_changes.entry(shard_content).or_insert(0) += 1;
        }
    }

    // Check that at least half of the shards changed position at least once
    let shards_that_moved = position_changes
        .values()
        .filter(|&&count| count > 0)
        .count();
    assert!(
        shards_that_moved >= shard_count / 2,
        "Only {} out of {} shards changed position across cycles",
        shards_that_moved,
        shard_count
    );

    Ok(())
}

#[test]
fn test_random_multi_path_locator_reshuffle_cycles() -> Result<()> {
    // Create a temporary directory
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Create several shard files
    let shard_count = 10;
    create_test_shards(&dir_path, shard_count)?;

    // Create paths for the multi-path locator
    let file_paths: Vec<PathBuf> = (0..shard_count)
        .map(|i| dir_path.join(format!("shard_{}", i)))
        .collect();

    // Create a random multi-path locator with a fixed seed for deterministic testing
    let locator = RandomMultiPathShardLocator::with_seed(file_paths, 42)?;

    // Read all shards in the first cycle and record their order
    let mut first_cycle_order = Vec::new();
    for _ in 0..shard_count {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard.source)?;
        first_cycle_order.push(content);
    }

    // Read all shards in the second cycle and record their order
    let mut second_cycle_order = Vec::new();
    for _ in 0..shard_count {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard.source)?;
        second_cycle_order.push(content);
    }

    // Read all shards in the third cycle and record their order
    let mut third_cycle_order = Vec::new();
    for _ in 0..shard_count {
        let shard = locator.next_shard()?;
        let content = read_file_content(shard.source)?;
        third_cycle_order.push(content);
    }

    // Verify that each cycle contains all the expected shards
    let expected_shards: HashSet<_> = (0..shard_count)
        .map(|i| format!("Shard content {}", i))
        .collect();

    let first_cycle_set: HashSet<_> = first_cycle_order.iter().cloned().collect();
    let second_cycle_set: HashSet<_> = second_cycle_order.iter().cloned().collect();
    let third_cycle_set: HashSet<_> = third_cycle_order.iter().cloned().collect();

    assert_eq!(first_cycle_set, expected_shards);
    assert_eq!(second_cycle_set, expected_shards);
    assert_eq!(third_cycle_set, expected_shards);

    // Check that all three cycles have different orders
    assert!(
        first_cycle_order != second_cycle_order,
        "First and second cycles had identical shard orders, which suggests the RNG seed isn't changing between reshuffles"
    );

    assert!(
        second_cycle_order != third_cycle_order,
        "Second and third cycles had identical shard orders, which suggests the RNG seed isn't changing between reshuffles"
    );

    assert!(
        first_cycle_order != third_cycle_order,
        "First and third cycles had identical shard orders, which suggests the RNG seed isn't changing between reshuffles"
    );

    // For a more rigorous test, check that the order changes by comparing positions of each shard
    // across cycles
    let mut position_changes = HashMap::new();

    for (shard_content, _) in expected_shards.iter().map(|s| (s, ())) {
        let pos1 = first_cycle_order
            .iter()
            .position(|s| s == shard_content)
            .unwrap();
        let pos2 = second_cycle_order
            .iter()
            .position(|s| s == shard_content)
            .unwrap();
        let pos3 = third_cycle_order
            .iter()
            .position(|s| s == shard_content)
            .unwrap();

        // Count position changes
        if pos1 != pos2 {
            *position_changes.entry(shard_content).or_insert(0) += 1;
        }
        if pos2 != pos3 {
            *position_changes.entry(shard_content).or_insert(0) += 1;
        }
        if pos1 != pos3 {
            *position_changes.entry(shard_content).or_insert(0) += 1;
        }
    }

    // Check that at least half of the shards changed position at least once
    let shards_that_moved = position_changes
        .values()
        .filter(|&&count| count > 0)
        .count();
    assert!(
        shards_that_moved >= shard_count / 2,
        "Only {} out of {} shards changed position across cycles",
        shards_that_moved,
        shard_count
    );

    Ok(())
}
