//! Tests for BlockWriter configuration options

use super::super::writer::{BlockWriter, BlockWriterConfig};
use std::io::Cursor;

#[test]
fn test_block_writer_creation() {
    let buffer = Cursor::new(Vec::new());
    let writer = BlockWriter::new(buffer).unwrap();
    
    // Verify default configuration
    assert_eq!(writer.config.block_size, 1 << 16); // 64 KiB
    assert_eq!(super::super::writer::BLOCK_HEADER_SIZE, 24); // Fixed header size
}

#[test]
fn test_custom_block_size() {
    let buffer = Cursor::new(Vec::new());
    let config = BlockWriterConfig::with_block_size(1 << 20).unwrap(); // 1 MiB
    let writer = BlockWriter::with_config(buffer, config).unwrap();
    
    assert_eq!(writer.config.block_size, 1 << 20);
    assert_eq!(writer.config.usable_block_size(), (1 << 20) - super::super::writer::BLOCK_HEADER_SIZE);
}

#[test]
fn test_minimum_valid_block_size() {
    // Test with the absolute minimum valid block size: 2 * BLOCK_HEADER_SIZE
    let min_block_size = super::super::writer::BLOCK_HEADER_SIZE * 2;
    
    // Should be able to create a config with the minimum size
    let config = BlockWriterConfig::with_block_size(min_block_size).unwrap();
    let buffer = Cursor::new(Vec::new());
    let writer = BlockWriter::with_config(buffer, config).unwrap();
    
    assert_eq!(writer.config.block_size, min_block_size);
    
    // Try with invalid block size (too small)
    let invalid_size = super::super::writer::BLOCK_HEADER_SIZE * 2 - 1; // One byte too small
    let result = BlockWriterConfig::with_block_size(invalid_size);
    assert!(result.is_err(), "Should reject block size smaller than 2 * BLOCK_HEADER_SIZE");
}
