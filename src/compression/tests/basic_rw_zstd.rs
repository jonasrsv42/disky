//! Basic roundtrip tests for Zstd compression.
//!
//! Tests the compression and decompression interfaces with Zstd to ensure
//! data integrity through the roundtrip process.

use std::io::Cursor;

use bytes::Bytes;

use crate::compression::core::{Compressor, Decompressor, CompressionType};
use crate::compression::zstd::{ZstdCompressor, ZstdDecompressor};
use crate::reader::{RecordReader, DiskyPiece};
use crate::writer::{RecordWriter, RecordWriterConfig};

#[test]
fn test_zstd_roundtrip_small_data() {
    let original_data = b"Hello, world! This is a test string for zstd compression.";
    
    // Create compressor and decompressor
    let mut compressor = ZstdCompressor::new().expect("Failed to create ZstdCompressor");
    let mut decompressor = ZstdDecompressor::new();
    
    // Compress the data
    let compressed = compressor.compress(original_data).expect("Compression failed");
    
    // For small data, compression may not reduce size due to overhead
    // Just verify that compression/decompression works correctly
    println!("Original size: {}, Compressed size: {}", original_data.len(), compressed.len());
    println!("Compressed data: {:?}", &compressed[..10.min(compressed.len())]);
    
    // Convert to Bytes for decompression (simulating file storage/retrieval)
    let compressed_bytes = Bytes::copy_from_slice(compressed);
    
    // Decompress the data
    let decompressed = decompressor.decompress(compressed_bytes, original_data.len()).expect("Decompression failed");
    
    // Verify roundtrip integrity
    assert_eq!(decompressed.as_ref(), original_data, "Roundtrip data mismatch");
}

#[test]
fn test_zstd_roundtrip_empty_data() {
    let original_data = b"";
    
    let mut compressor = ZstdCompressor::new().expect("Failed to create ZstdCompressor");
    let mut decompressor = ZstdDecompressor::new();
    
    let compressed = compressor.compress(original_data).expect("Compression failed");
    let compressed_bytes = Bytes::copy_from_slice(compressed);
    let decompressed = decompressor.decompress(compressed_bytes, original_data.len()).expect("Decompression failed");
    
    assert_eq!(decompressed.as_ref(), original_data, "Empty data roundtrip failed");
}

#[test]
fn test_zstd_roundtrip_large_data() {
    // Create a larger test dataset that will definitely benefit from compression
    let original_data = "ABCD".repeat(1000); // 4KB of repeated data
    let original_bytes = original_data.as_bytes();
    
    let mut compressor = ZstdCompressor::new().expect("Failed to create ZstdCompressor");
    let mut decompressor = ZstdDecompressor::new();
    
    let compressed = compressor.compress(original_bytes).expect("Compression failed");
    
    // This repetitive data should compress very well
    assert!(compressed.len() < original_bytes.len() / 10, 
        "Compressed size ({}) should be much smaller than original ({}) for repetitive data", 
        compressed.len(), original_bytes.len());
    
    let compressed_bytes = Bytes::copy_from_slice(compressed);
    let decompressed = decompressor.decompress(compressed_bytes, original_bytes.len()).expect("Decompression failed");
    
    assert_eq!(decompressed.as_ref(), original_bytes, "Large data roundtrip failed");
}

#[test]
fn test_zstd_roundtrip_binary_data() {
    // Test with binary data including null bytes
    let original_data: Vec<u8> = (0..=255).cycle().take(512).collect();
    
    let mut compressor = ZstdCompressor::new().expect("Failed to create ZstdCompressor");
    let mut decompressor = ZstdDecompressor::new();
    
    let compressed = compressor.compress(&original_data).expect("Compression failed");
    let compressed_bytes = Bytes::copy_from_slice(compressed);
    let decompressed = decompressor.decompress(compressed_bytes, original_data.len()).expect("Decompression failed");
    
    assert_eq!(decompressed.as_ref(), original_data.as_slice(), "Binary data roundtrip failed");
}

#[test]
fn test_zstd_multiple_compressions() {
    // Test that the same compressor can be used multiple times
    let data1 = b"First piece of data to compress";
    let data2 = b"Second piece of data to compress";
    let data3 = b"Third piece of data to compress";
    
    let mut compressor = ZstdCompressor::new().expect("Failed to create ZstdCompressor");
    let mut decompressor = ZstdDecompressor::new();
    
    // Compress and test each piece independently (due to lifetime constraints)
    
    {
        let compressed = compressor.compress(data1).expect("Compression failed");
        let compressed_bytes = Bytes::copy_from_slice(compressed);
        let decompressed = decompressor.decompress(compressed_bytes, data1.len())
            .expect("Decompression failed");
        assert_eq!(decompressed.as_ref(), data1, "Data roundtrip failed");
    }
    
    {
        let compressed = compressor.compress(data2).expect("Compression failed");
        let compressed_bytes = Bytes::copy_from_slice(compressed);
        let decompressed = decompressor.decompress(compressed_bytes, data2.len())
            .expect("Decompression failed");
        assert_eq!(decompressed.as_ref(), data2, "Data roundtrip failed");
    }
    
    {
        let compressed = compressor.compress(data3).expect("Compression failed");
        let compressed_bytes = Bytes::copy_from_slice(compressed);
        let decompressed = decompressor.decompress(compressed_bytes, data3.len())
            .expect("Decompression failed");
        assert_eq!(decompressed.as_ref(), data3, "Data roundtrip failed");
    }
}

#[test]
fn test_zstd_different_compression_levels() {
    let original_data = "Test data for compression level comparison. ".repeat(100);
    let original_bytes = original_data.as_bytes();
    
    // Test different compression levels
    for level in [1, 3, 6, 9, 15] {
        let mut compressor = ZstdCompressor::with_level(level)
            .expect(&format!("Failed to create ZstdCompressor with level {}", level));
        let mut decompressor = ZstdDecompressor::new();
        
        let compressed = compressor.compress(original_bytes)
            .expect(&format!("Compression failed with level {}", level));
        let compressed_bytes = Bytes::copy_from_slice(compressed);
        let decompressed = decompressor.decompress(compressed_bytes, original_bytes.len())
            .expect(&format!("Decompression failed with level {}", level));
        
        assert_eq!(decompressed.as_ref(), original_bytes, 
            "Roundtrip failed with compression level {}", level);
    }
}

#[test]
fn test_zstd_invalid_compression_level() {
    // Test that invalid compression levels are rejected
    assert!(ZstdCompressor::with_level(0).is_err(), "Level 0 should be invalid");
    assert!(ZstdCompressor::with_level(23).is_err(), "Level 23 should be invalid");
    assert!(ZstdCompressor::with_level(-1).is_err(), "Negative level should be invalid");
}

#[test]
fn test_zstd_e2e_writer_reader_roundtrip() {

    // Create test data
    let test_records = vec![
        b"First record with some test data".to_vec(),
        b"Second record with different content and more text to compress".to_vec(),
        b"Third record: ".repeat(50), // Repetitive data that compresses well
        b"Fourth record with binary data".to_vec(),
        (0u8..=255).cycle().take(500).collect::<Vec<u8>>(), // Binary data
    ];

    // Write records with Zstd compression to in-memory buffer
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let config = RecordWriterConfig::default().with_compression(CompressionType::Zstd);
        let mut writer = RecordWriter::with_config(cursor, config)
            .expect("Failed to create RecordWriter with Zstd compression");

        for record in &test_records {
            writer.write_record(record).expect("Failed to write record");
        }

        writer.close().expect("Failed to close writer");
    }

    // Read records back and verify
    {
        let cursor = Cursor::new(&buffer);
        let mut reader = RecordReader::new(cursor).expect("Failed to create RecordReader");

        let mut read_records = Vec::new();
        
        loop {
            match reader.next_record().expect("Failed to read record") {
                DiskyPiece::Record(record) => read_records.push(record.to_vec()),
                DiskyPiece::EOF => break,
            }
        }

        // Verify we got all records back correctly
        assert_eq!(read_records.len(), test_records.len(), "Record count mismatch");
        
        for (i, (original, read_back)) in test_records.iter().zip(read_records.iter()).enumerate() {
            assert_eq!(original, read_back, "Record {} content mismatch", i);
        }
    }

    // Verify compression actually happened by checking buffer size
    let uncompressed_size: usize = test_records.iter().map(|r| r.len()).sum();
    
    // The compressed buffer should be smaller than raw data
    // (accounting for headers and metadata, but the repetitive data should compress well)
    println!("Compressed size: {} bytes, Uncompressed data: {} bytes", buffer.len(), uncompressed_size);
    assert!(
        buffer.len() < uncompressed_size, 
        "Compressed buffer size ({} bytes) should be smaller than uncompressed data ({} bytes)",
        buffer.len(), uncompressed_size
    );
}

#[test] 
fn test_zstd_e2e_mixed_record_sizes() {

    // Create test data with very different record sizes
    let test_records = vec![
        b"tiny".to_vec(),
        b"".to_vec(), // empty record
        "medium sized record with some text content".repeat(10).as_bytes().to_vec(),
        "large record with lots of repeated content ".repeat(100).as_bytes().to_vec(),
        (0u8..=255).cycle().take(1000).collect::<Vec<u8>>(), // Large binary record
    ];

    // Write with Zstd compression to in-memory buffer
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let config = RecordWriterConfig::default().with_compression(CompressionType::Zstd);
        let mut writer = RecordWriter::with_config(cursor, config)
            .expect("Failed to create RecordWriter with Zstd compression");

        for record in &test_records {
            writer.write_record(record).expect("Failed to write record");
        }

        writer.close().expect("Failed to close writer");
    }

    // Read back and verify
    {
        let cursor = Cursor::new(&buffer);
        let mut reader = RecordReader::new(cursor).expect("Failed to create RecordReader");

        let mut read_records = Vec::new();
        
        loop {
            match reader.next_record().expect("Failed to read record") {
                DiskyPiece::Record(record) => read_records.push(record.to_vec()),
                DiskyPiece::EOF => break,
            }
        }

        assert_eq!(read_records.len(), test_records.len(), "Record count mismatch");
        
        for (i, (original, read_back)) in test_records.iter().zip(read_records.iter()).enumerate() {
            assert_eq!(original, read_back, "Record {} content mismatch (size: original={}, read={})", 
                i, original.len(), read_back.len());
        }
    }
}