// Copyright 2024
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use disky::error::Result;
use disky::reader::RecordReader;
use disky::sampling::SamplingReader;
use disky::writer::RecordWriter;
use std::io::Cursor;

/// Writes test data to a buffer and returns it
fn create_test_data(prefix: &str, count: usize) -> Vec<u8> {
    let mut buffer = Vec::new();
    
    {
        // Create a writer with a cursor
        let mut writer = RecordWriter::new(Cursor::new(&mut buffer)).unwrap();
        
        // Write records
        for i in 0..count {
            let record = format!("{}{}", prefix, i);
            writer.write_record(record.as_bytes()).unwrap();
        }
        
        // Close the writer
        writer.close().unwrap();
    }
    
    buffer
}

#[test]
fn test_sampling_from_multiple_files() -> Result<()> {
    // Create two test data buffers
    let data_a = create_test_data("A", 10);
    let data_b = create_test_data("B", 15);

    // Create readers from the data
    let reader_a = RecordReader::new(Cursor::new(&data_a))?;
    let reader_b = RecordReader::new(Cursor::new(&data_b))?;

    // Create a sampling reader with different weights
    let sources = vec![(2.0, reader_a), (1.0, reader_b)];
    let reader = SamplingReader::new(sources)?;

    // Collect all records using Iterator trait
    let mut records = Vec::new();
    for result in reader {
        let record = result?;
        records.push(record);
    }

    // Should have collected all 25 records (10 from A + 15 from B)
    assert_eq!(records.len(), 25);

    // Convert to strings for easier verification
    let record_strings: Vec<String> = records
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();

    // Check that all records from both sources are present
    for i in 0..10 {
        let expected = format!("A{}", i);
        assert!(record_strings.contains(&expected));
    }

    for i in 0..15 {
        let expected = format!("B{}", i);
        assert!(record_strings.contains(&expected));
    }

    Ok(())
}
