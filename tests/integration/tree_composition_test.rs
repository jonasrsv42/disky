//! Integration tests for composing complex reader trees.
//!
//! These tests verify that different tree nodes can be composed together
//! to build complex reader pipelines combining:
//! - Shard readers (sequential file shards)
//! - Individual record readers
//! - Sampling readers (weighted interleaving)
//! - Reservoir shuffle (streaming randomization)
//! - Threaded nodes (offload subtrees to dedicated threads)
//! - Round-robin interleaving

use std::collections::HashSet;
use std::io::Cursor;

use tempfile::tempdir;

use disky::error::Result;
use disky::parallel::node::ThreadedNodeConfig;
use disky::reader::RecordReaderConfig;
use disky::shard::reader::SequentialShardReaderConfig;
use disky::shard::sink::FileShardsBuilder;
use disky::shard::source::{FileShards, SequentialShardSource};
use disky::shard::writer::SequentialShardWriterConfig;
use disky::tree::reader::RoundRobinNode;
use disky::tree::sampling::{ReservoirShuffleConfig, SamplingReaderConfig};
use disky::writer::RecordWriterConfig;

/// Helper: write test data to a buffer.
fn create_in_memory_data(prefix: &str, count: usize) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = RecordWriterConfig::new(Cursor::new(&mut buffer))
            .build()
            .unwrap();
        for i in 0..count {
            let record = format!("{}{}", prefix, i);
            writer.write_record(record.as_bytes()).unwrap();
        }
        writer.close().unwrap();
    }
    buffer
}

/// Helper: write test data to shard files.
fn create_shard_data(dir: &std::path::Path, prefix: &str, count: usize) -> Result<()> {
    let sink = FileShardsBuilder::new(dir.to_path_buf(), prefix).build()?;
    let mut writer = SequentialShardWriterConfig::new(sink)
        .max_shard_bytes(500) // Small shards to create multiple files
        .build();

    for i in 0..count {
        let record = format!("{}_{}", prefix, i);
        writer.write_record(record.as_bytes())?;
    }
    writer.close()?;
    Ok(())
}

/// Helper: collect all records from an iterator into a set of strings.
fn collect_as_strings(
    reader: impl Iterator<Item = Result<bytes::Bytes>>,
) -> Result<HashSet<String>> {
    let mut set = HashSet::new();
    for result in reader {
        let bytes = result?;
        set.insert(String::from_utf8_lossy(&bytes).to_string());
    }
    Ok(set)
}

/// Helper: collect all records preserving order.
fn collect_ordered(reader: impl Iterator<Item = Result<bytes::Bytes>>) -> Result<Vec<String>> {
    let mut vec = Vec::new();
    for result in reader {
        let bytes = result?;
        vec.push(String::from_utf8_lossy(&bytes).to_string());
    }
    Ok(vec)
}

// =============================================================================
// Test: Simple threaded node wrapping a single reader
// =============================================================================

#[test]
fn test_threaded_single_reader() -> Result<()> {
    let data = create_in_memory_data("T", 50);

    let reader = ThreadedNodeConfig::new(RecordReaderConfig::new(Cursor::new(data))).build()?;

    let records = collect_as_strings(reader)?;

    assert_eq!(records.len(), 50);
    for i in 0..50 {
        assert!(records.contains(&format!("T{}", i)));
    }
    Ok(())
}

// =============================================================================
// Test: Threaded node wrapping a shard reader
// =============================================================================

#[test]
fn test_threaded_shard_reader() -> Result<()> {
    let dir = tempdir().unwrap();
    create_shard_data(dir.path(), "shard", 100)?;

    let shards = FileShards::from_pattern(dir.path(), "shard")?;
    let shard_reader = SequentialShardReaderConfig::new(SequentialShardSource::new(shards));

    // Wrap in a threaded node
    let reader = ThreadedNodeConfig::new(shard_reader)
        .with_buffer_size(16)
        .build()?;

    let records = collect_as_strings(reader)?;

    assert_eq!(records.len(), 100);
    for i in 0..100 {
        assert!(records.contains(&format!("shard_{}", i)));
    }
    Ok(())
}

// =============================================================================
// Test: Sampling reader with threaded children
// =============================================================================

#[test]
fn test_sampling_with_threaded_children() -> Result<()> {
    let data_a = create_in_memory_data("A", 30);
    let data_b = create_in_memory_data("B", 20);

    // Each source runs on its own thread
    let threaded_a = ThreadedNodeConfig::new(RecordReaderConfig::new(Cursor::new(data_a)));
    let threaded_b = ThreadedNodeConfig::new(RecordReaderConfig::new(Cursor::new(data_b)));

    let reader = SamplingReaderConfig::new()
        .append(1.0, threaded_a)
        .append(1.0, threaded_b)
        .build()?;

    let records = collect_as_strings(reader)?;

    assert_eq!(records.len(), 50);
    for i in 0..30 {
        assert!(records.contains(&format!("A{}", i)));
    }
    for i in 0..20 {
        assert!(records.contains(&format!("B{}", i)));
    }
    Ok(())
}

// =============================================================================
// Test: Shuffle with threaded child
// =============================================================================

#[test]
fn test_shuffle_with_threaded_child() -> Result<()> {
    let data = create_in_memory_data("S", 100);

    let reader = ReservoirShuffleConfig::new(ThreadedNodeConfig::new(RecordReaderConfig::new(
        Cursor::new(data),
    )))
    .with_buffer_size(20)
    .with_seed(12345)
    .build()?;

    let records = collect_ordered(reader)?;

    // All records present
    let as_set: HashSet<_> = records.iter().cloned().collect();
    assert_eq!(as_set.len(), 100);
    for i in 0..100 {
        assert!(as_set.contains(&format!("S{}", i)));
    }

    // With shuffle, order should differ from sequential (with high probability)
    let sequential: Vec<String> = (0..100).map(|i| format!("S{}", i)).collect();
    assert_ne!(records, sequential, "Shuffle should change order");

    Ok(())
}

// =============================================================================
// Test: Complex tree - sampling from shards + memory, threaded, then shuffled
// =============================================================================

#[test]
fn test_complex_tree_sampling_shards_memory_threaded_shuffled() -> Result<()> {
    let dir = tempdir().unwrap();

    // Create shard data on disk
    create_shard_data(dir.path(), "disk", 50)?;

    // Create in-memory data
    let mem_data = create_in_memory_data("mem", 30);

    // Build tree:
    //
    //   ReservoirShuffle
    //       |
    //   SamplingReader
    //      /        \
    // Threaded    Threaded
    //    |           |
    // ShardReader  RecordReader
    //
    let shards = FileShards::from_pattern(dir.path(), "disk")?;
    let shard_node = ThreadedNodeConfig::new(SequentialShardReaderConfig::new(
        SequentialShardSource::new(shards),
    ))
    .with_buffer_size(8);

    let memory_node =
        ThreadedNodeConfig::new(RecordReaderConfig::new(Cursor::new(mem_data))).with_buffer_size(8);

    let sampling = SamplingReaderConfig::new()
        .append(1.0, shard_node)
        .append(1.0, memory_node);

    let reader = ReservoirShuffleConfig::new(sampling)
        .with_buffer_size(30)
        .with_seed(42)
        .build()?;

    let records = collect_as_strings(reader)?;

    // Verify all records present
    assert_eq!(records.len(), 80); // 50 + 30

    for i in 0..50 {
        assert!(
            records.contains(&format!("disk_{}", i)),
            "Missing disk_{}",
            i
        );
    }
    for i in 0..30 {
        assert!(records.contains(&format!("mem{}", i)), "Missing mem{}", i);
    }

    Ok(())
}

// =============================================================================
// Test: Round-robin with threaded children
// =============================================================================

#[test]
fn test_round_robin_with_threaded_children() -> Result<()> {
    let data_x = create_in_memory_data("X", 10);
    let data_y = create_in_memory_data("Y", 10);
    let data_z = create_in_memory_data("Z", 10);

    let tree = RoundRobinNode::new()
        .append(ThreadedNodeConfig::new(RecordReaderConfig::new(
            Cursor::new(data_x),
        )))
        .append(ThreadedNodeConfig::new(RecordReaderConfig::new(
            Cursor::new(data_y),
        )))
        .append(ThreadedNodeConfig::new(RecordReaderConfig::new(
            Cursor::new(data_z),
        )));

    let reader = tree.build()?;
    let records = collect_ordered(reader)?;

    assert_eq!(records.len(), 30);

    // Round-robin should interleave: X0, Y0, Z0, X1, Y1, Z1, ...
    for i in 0..10 {
        assert_eq!(records[i * 3], format!("X{}", i));
        assert_eq!(records[i * 3 + 1], format!("Y{}", i));
        assert_eq!(records[i * 3 + 2], format!("Z{}", i));
    }

    Ok(())
}

// =============================================================================
// Test: Deeply nested tree - thread wrapping shuffle wrapping sampling
// =============================================================================

#[test]
fn test_deeply_nested_tree() -> Result<()> {
    let data_a = create_in_memory_data("A", 20);
    let data_b = create_in_memory_data("B", 20);

    // Inner: sampling from two sources
    let sampling = SamplingReaderConfig::new()
        .append(1.0, RecordReaderConfig::new(Cursor::new(data_a)))
        .append(1.0, RecordReaderConfig::new(Cursor::new(data_b)));

    // Middle: shuffle the sampled stream
    let shuffled = ReservoirShuffleConfig::new(sampling)
        .with_buffer_size(15)
        .with_seed(999);

    // Outer: run the whole thing on a thread
    let reader = ThreadedNodeConfig::new(shuffled)
        .with_buffer_size(32)
        .build()?;

    let records = collect_as_strings(reader)?;

    assert_eq!(records.len(), 40);
    for i in 0..20 {
        assert!(records.contains(&format!("A{}", i)));
        assert!(records.contains(&format!("B{}", i)));
    }

    Ok(())
}

// =============================================================================
// Test: Multiple shard sources combined with round-robin and threading
// =============================================================================

#[test]
fn test_multiple_shard_sources_round_robin_threaded() -> Result<()> {
    let dir = tempdir().unwrap();

    // Create two separate shard datasets
    let dir_alpha = dir.path().join("alpha");
    let dir_beta = dir.path().join("beta");
    std::fs::create_dir_all(&dir_alpha)?;
    std::fs::create_dir_all(&dir_beta)?;

    create_shard_data(&dir_alpha, "data", 25)?;
    create_shard_data(&dir_beta, "data", 25)?;

    // Build tree that round-robins between two threaded shard readers
    let shards_alpha = FileShards::from_pattern(&dir_alpha, "data")?;
    let shards_beta = FileShards::from_pattern(&dir_beta, "data")?;

    let tree = RoundRobinNode::new()
        .append(ThreadedNodeConfig::new(SequentialShardReaderConfig::new(
            SequentialShardSource::new(shards_alpha),
        )))
        .append(ThreadedNodeConfig::new(SequentialShardReaderConfig::new(
            SequentialShardSource::new(shards_beta),
        )));

    let reader = tree.build()?;
    let records = collect_ordered(reader)?;

    assert_eq!(records.len(), 50);

    // Both sources should be present (they have same record names since same prefix)
    // Round-robin alternates between them
    let as_set: HashSet<_> = records.iter().cloned().collect();
    for i in 0..25 {
        assert!(as_set.contains(&format!("data_{}", i)));
    }

    Ok(())
}

// =============================================================================
// Test: Early termination doesn't deadlock (drop reader mid-stream)
// =============================================================================

#[test]
fn test_early_termination_no_deadlock() -> Result<()> {
    let data = create_in_memory_data("E", 10000);

    let mut reader = ThreadedNodeConfig::new(
        ReservoirShuffleConfig::new(RecordReaderConfig::new(Cursor::new(data)))
            .with_buffer_size(100),
    )
    .with_buffer_size(16)
    .build()?;

    // Read only a few records then drop
    for _ in 0..10 {
        let _ = reader.next();
    }

    // Dropping should not deadlock
    drop(reader);

    Ok(())
}

// =============================================================================
// Test: Empty sources in a tree
// =============================================================================

#[test]
fn test_empty_sources_in_tree() -> Result<()> {
    let data_empty = create_in_memory_data("E", 0);
    let data_full = create_in_memory_data("F", 10);

    let reader = SamplingReaderConfig::new()
        .append(
            1.0,
            ThreadedNodeConfig::new(RecordReaderConfig::new(Cursor::new(data_empty))),
        )
        .append(
            1.0,
            ThreadedNodeConfig::new(RecordReaderConfig::new(Cursor::new(data_full))),
        )
        .build()?;
    let records = collect_as_strings(reader)?;

    assert_eq!(records.len(), 10);
    for i in 0..10 {
        assert!(records.contains(&format!("F{}", i)));
    }

    Ok(())
}
