//! Integration tests for shard writer → reader round-trips using real files.

use tempfile::tempdir;

use disky::error::Result;
use disky::reader::RecordReaderConfig;
use disky::shard::reader::SequentialShardReader;
use disky::shard::sink::FileShardsBuilder;
use disky::shard::source::{FileShards, SequentialShardSource};
use disky::shard::writer::SequentialShardWriterConfig;

/// Collect all records from a SequentialShardReader into Vec<Vec<u8>>.
fn collect_records(
    reader: SequentialShardReader<
        std::fs::File,
        impl Iterator<Item = Result<disky::shard::source::Shard<std::fs::File>>>,
    >,
) -> Result<Vec<Vec<u8>>> {
    reader.map(|r| r.map(|b| b.to_vec())).collect()
}

#[test]
fn single_shard_roundtrip() -> Result<()> {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();

    // Write
    let sink = FileShardsBuilder::new(dir_path.clone(), "shard").build()?;
    let mut writer = SequentialShardWriterConfig::new(sink).build();

    let expected: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("record_{}", i).into_bytes())
        .collect();

    for record in &expected {
        writer.write_record(record)?;
    }
    writer.close()?;

    // Read
    let source = FileShards::from_pattern(dir_path, "shard")?;
    let reader = SequentialShardReader::new(
        SequentialShardSource::new(source),
        RecordReaderConfig::default(),
    );
    let actual = collect_records(reader)?;

    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn multi_shard_roundtrip() -> Result<()> {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();

    // Write with a small shard limit to force rotation.
    let sink = FileShardsBuilder::new(dir_path.clone(), "data").build()?;
    let mut writer = SequentialShardWriterConfig::new(sink)
        .max_shard_bytes(200)
        .build();

    let expected: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("entry_{:04}", i).into_bytes())
        .collect();

    for record in &expected {
        writer.write_record(record)?;
    }
    writer.close()?;

    // Verify multiple shard files were created.
    let shard_count = std::fs::read_dir(&dir_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("data_"))
        .count();
    assert!(
        shard_count > 1,
        "Expected multiple shards, got {}",
        shard_count
    );

    // Read back all records across shards.
    let source = FileShards::from_pattern(dir_path, "data")?;
    let reader = SequentialShardReader::new(
        SequentialShardSource::new(source),
        RecordReaderConfig::default(),
    );
    let actual = collect_records(reader)?;

    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn large_records_across_shards() -> Result<()> {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();

    let sink = FileShardsBuilder::new(dir_path.clone(), "big").build()?;
    let mut writer = SequentialShardWriterConfig::new(sink)
        .max_shard_bytes(50_000)
        .build();

    let expected: Vec<Vec<u8>> = vec![vec![b'A'; 30_000], vec![b'B'; 30_000], vec![b'C'; 30_000]];

    for record in &expected {
        writer.write_record(record)?;
    }
    writer.close()?;

    let source = FileShards::from_pattern(dir_path, "big")?;
    let reader = SequentialShardReader::new(
        SequentialShardSource::new(source),
        RecordReaderConfig::default(),
    );
    let actual = collect_records(reader)?;

    assert_eq!(actual.len(), expected.len());
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(a, e, "Mismatch at record {}", i);
    }
    Ok(())
}

#[test]
fn empty_records_roundtrip() -> Result<()> {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();

    let sink = FileShardsBuilder::new(dir_path.clone(), "empty").build()?;
    let mut writer = SequentialShardWriterConfig::new(sink).build();

    let expected: Vec<Vec<u8>> = vec![
        b"before".to_vec(),
        vec![],
        b"middle".to_vec(),
        vec![],
        vec![],
        b"after".to_vec(),
    ];

    for record in &expected {
        writer.write_record(record)?;
    }
    writer.close()?;

    let source = FileShards::from_pattern(dir_path, "empty")?;
    let reader = SequentialShardReader::new(
        SequentialShardSource::new(source),
        RecordReaderConfig::default(),
    );
    let actual = collect_records(reader)?;

    assert_eq!(actual, expected);
    Ok(())
}
