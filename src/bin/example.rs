//// Copyright 2024
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////      http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//
////! Example usage of the Disky crate (Rust implementation of Riegeli).
//
//use disky::{writer::RecordWriterOptions, RecordReader, RecordWriter};
//use std::fs::File;
//
//fn create_sample_record(i: usize, size: usize) -> Vec<u8> {
//    let piece = format!("{} ", i).into_bytes();
//    let mut result = Vec::with_capacity(size);
//
//    while result.len() < size {
//        result.extend_from_slice(&piece);
//    }
//
//    result.truncate(size);
//    result
//}
//
//fn write_records(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
//    println!("Writing to {}", filename);
//
//    let file = File::create(filename)?;
//    let mut writer = RecordWriter::with_options(
//        file,
//        RecordWriterOptions {
//            chunk_size: 16,
//            ..Default::default()
//        },
//    )?;
//
//    for i in 0..10000 {
//        let record = create_sample_record(i, 100);
//        writer.write_record(&record)?;
//    }
//
//    writer.close()?;
//    println!("Successfully wrote 100 records");
//
//    Ok(())
//}
//
//fn read_records(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
//    println!("Reading from {}", filename);
//
//    let file = File::open(filename)?;
//    let mut reader = RecordReader::new(file)?;
//
//    let mut count = 0;
//    loop {
//        match reader.read_record() {
//            Ok(record) => {
//                count += 1;
//                if count <= 5 {
//                    // Only print the first 5 records
//                    let preview = String::from_utf8_lossy(&record[..20]);
//                    println!("Record {}: {}...", count, preview);
//                }
//            }
//            Err(e) => {
//                if let disky::error::RiegeliError::UnexpectedEof = e {
//                    break;
//                } else {
//                    return Err(Box::new(e));
//                }
//            }
//        }
//    }
//
//    println!("Successfully read {} records", count);
//
//    Ok(())
//}
//
//fn main() -> Result<(), Box<dyn std::error::Error>> {
//    let filename = "example.riegeli";
//
//    write_records(filename)?;
//    read_records(filename)?;
//
//    Ok(())
//}
