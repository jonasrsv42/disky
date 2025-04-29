use std::fs::File;
use disky::reader::{RecordReader, DiskyPiece};
use disky::writer::RecordWriter;

fn main() -> disky::error::Result<()> {
    // Basic writing example
    basic_write_example()?;
    
    // Basic reading example
    basic_read_example()?;
    
    // Append mode example
    append_example()?;
    
    // Using iterator interface
    iterator_example()?;
    
    println!("All examples completed successfully!");
    Ok(())
}

// Basic example of writing records to a file
fn basic_write_example() -> disky::error::Result<()> {
    println!("Running basic write example...");
    
    // Create a new file
    let file = File::create("basic_example.riegeli")?;
    
    // Create a writer with default settings
    let mut writer = RecordWriter::new(file)?;
    
    // Write some records
    writer.write_record(b"Record 1")?;
    writer.write_record(b"Record 2")?;
    writer.write_record(b"Record 3")?;
    
    // Close the writer (ensures all data is flushed)
    writer.close()?;
    
    println!("Wrote 3 records to basic_example.riegeli");
    Ok(())
}

// Basic example of reading records from a file
fn basic_read_example() -> disky::error::Result<()> {
    println!("Running basic read example...");
    
    // Open the file for reading
    let file = File::open("basic_example.riegeli")?;
    
    // Create a reader with default settings
    let mut reader = RecordReader::new(file)?;
    
    // Read and process all records
    let mut count = 0;
    loop {
        match reader.next_record()? {
            DiskyPiece::Record(bytes) => {
                count += 1;
                println!("Read record {}: {} bytes", count, bytes.len());
                println!("Content: {}", String::from_utf8_lossy(&bytes));
            }
            DiskyPiece::EOF => break,
        }
    }
    
    println!("Read {} records from basic_example.riegeli", count);
    Ok(())
}

// Example of appending to an existing file
fn append_example() -> disky::error::Result<()> {
    println!("Running append example...");
    
    // Open existing file for appending
    let file = File::options().read(true).write(true).open("basic_example.riegeli")?;
    
    // Get the current file size (where to start appending)
    let file_size = file.metadata()?.len();
    
    // Create a writer in append mode
    let mut writer = RecordWriter::for_append(file, file_size)?;
    
    // Append a few records
    writer.write_record(b"Appended Record 1")?;
    writer.write_record(b"Appended Record 2")?;
    
    // Close the writer
    writer.close()?;
    
    // Now read all records from the file to verify append worked
    let file = File::open("basic_example.riegeli")?;
    let mut reader = RecordReader::new(file)?;
    
    let mut count = 0;
    loop {
        match reader.next_record()? {
            DiskyPiece::Record(_) => count += 1,
            DiskyPiece::EOF => break,
        }
    }
    
    println!("After appending, the file now contains {} records", count);
    Ok(())
}

// Example of using the iterator interface
fn iterator_example() -> disky::error::Result<()> {
    println!("Running iterator example...");
    
    // Open the file for reading
    let file = File::open("basic_example.riegeli")?;
    
    // Create a reader
    let reader = RecordReader::new(file)?;
    
    // Use iterator interface to process records
    println!("Records read via iterator:");
    for (i, record_result) in reader.enumerate() {
        let record = record_result?;
        println!("#{}: {}", i+1, String::from_utf8_lossy(&record));
    }
    
    Ok(())
}
