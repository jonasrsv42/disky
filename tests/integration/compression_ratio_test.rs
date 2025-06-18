//! Integration test for compression ratio analysis on real data.
//!
//! This test reads the logmel.disky file (if it exists) and analyzes
//! compression ratios with different zstd levels to help optimize
//! compression settings for real-world data.

use std::fs;
use std::path::Path;
use tempfile::NamedTempFile;

use disky::reader::{DiskyPiece, RecordReader};
use disky::writer::{RecordWriter, RecordWriterConfig};

#[cfg(feature = "zstd")]
use disky::compression::CompressionType;

/// Test compression ratios on real logmel spectrogram data
/// 
/// This test is ignored by default due to its slow execution time.
/// Run explicitly with: cargo test test_logmel_compression_ratios --ignored --features zstd -- --nocapture
#[test]
#[ignore]
fn test_logmel_compression_ratios() {
    let logmel_path = "tests/integration/logmel.disky";
    
    // Check if the logmel.disky file exists
    if !Path::new(logmel_path).exists() {
        println!("logmel.disky not found, skipping compression ratio test");
        return;
    }

    // Read all records from the logmel file
    let file = fs::File::open(logmel_path).expect("Failed to open logmel.disky");
    let mut reader = RecordReader::new(file).expect("Failed to create RecordReader");
    
    let mut records = Vec::new();
    let mut total_record_size = 0;
    
    loop {
        match reader.next_record().expect("Failed to read record") {
            DiskyPiece::Record(bytes) => {
                total_record_size += bytes.len();
                records.push(bytes.to_vec());
            }
            DiskyPiece::EOF => break,
        }
    }
    
    println!("Loaded {} records from logmel.disky", records.len());
    println!("Total record data size: {} bytes ({:.2} MB)", 
        total_record_size, 
        total_record_size as f64 / 1024.0 / 1024.0
    );
    
    if records.is_empty() {
        println!("No records found in logmel.disky");
        return;
    }

    // Test uncompressed file size first
    let uncompressed_file_size = write_records_uncompressed(&records);
    println!("Uncompressed file size: {} bytes ({:.2} MB)", 
        uncompressed_file_size, 
        uncompressed_file_size as f64 / 1024.0 / 1024.0
    );

    // Test compression ratios with zstd (if feature is enabled)
    #[cfg(feature = "zstd")]
    test_zstd_file_compression_ratios(&records, uncompressed_file_size);
    
    #[cfg(not(feature = "zstd"))]
    println!("zstd feature not enabled, skipping compression ratio analysis");
}

/// Write records to an uncompressed file and return the file size
fn write_records_uncompressed(records: &[Vec<u8>]) -> u64 {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let mut writer = RecordWriter::new(temp_file.reopen().unwrap()).expect("Failed to create writer");
    
    // Write all records
    for record in records {
        writer.write_record(record).expect("Failed to write record");
    }
    
    writer.close().expect("Failed to close writer");
    
    // Get file size
    temp_file.as_file().metadata().expect("Failed to get file metadata").len()
}

#[cfg(feature = "zstd")]
fn test_zstd_file_compression_ratios(records: &[Vec<u8>], uncompressed_file_size: u64) {
    // Test different compression levels
    let compression_levels = vec![1, 3, 6, 9, 15, 20];
    
    println!("\nZstd File Compression Analysis:");
    println!("Level | File Size       | Ratio | Savings");
    println!("------|-----------------|-------|--------");
    
    for level in compression_levels {
        let compressed_file_size = write_records_with_zstd_level(records, level);
        
        if compressed_file_size > 0 {
            let ratio = uncompressed_file_size as f64 / compressed_file_size as f64;
            let savings = (1.0 - (compressed_file_size as f64 / uncompressed_file_size as f64)) * 100.0;
            
            println!("{:5} | {:13} | {:5.2}x | {:6.1}%", 
                level, 
                format_size(compressed_file_size),
                ratio,
                savings
            );
        } else {
            println!("{:5} | ERROR           |       |       ", level);
        }
    }
    
    println!("\nNote: Higher compression levels take longer but may achieve better ratios.");
    println!("Level 6 (current default) is usually a good balance for most workloads.");
    println!("File sizes include all disky format overhead (headers, metadata, etc.)");
}

#[cfg(feature = "zstd")]
fn write_records_with_zstd_level(records: &[Vec<u8>], level: i32) -> u64 {
    let temp_file = match NamedTempFile::new() {
        Ok(f) => f,
        Err(_) => return 0,
    };
    
    let config = RecordWriterConfig::default().with_compression(CompressionType::Zstd(level));
    let mut writer = match RecordWriter::with_config(temp_file.reopen().unwrap(), config) {
        Ok(w) => w,
        Err(_) => return 0,
    };
    
    // Write all records
    for record in records {
        if writer.write_record(record).is_err() {
            return 0;
        }
    }
    
    if writer.close().is_err() {
        return 0;
    }
    
    // Get file size
    temp_file.as_file().metadata().map(|m| m.len()).unwrap_or(0)
}

/// Format byte size in a human-readable way
fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / 1024.0 / 1024.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}