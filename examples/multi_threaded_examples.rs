#[cfg(feature = "parallel")]
mod parallel_example {
    use bytes::Bytes;
    use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
    use disky::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
    use disky::parallel::reader::{ShardingConfig as ReaderShardingConfig, ParallelReaderConfig, DiskyParallelPiece};
    use disky::parallel::writer::{ShardingConfig as WriterShardingConfig, ParallelWriterConfig};
    use disky::parallel::sharding::{FileSharder, FileShardLocator};
    use tempfile::tempdir;

    pub fn run_examples() -> disky::error::Result<()> {
        println!("Running multi-threaded examples...");
        
        // Create a temporary directory that will be automatically cleaned up
        let temp_dir = tempdir()?;
        let dir_path = temp_dir.path().to_path_buf();
        println!("Created temporary directory: {}", dir_path.display());
        
        // Basic multi-threaded writing example with multiple shards
        basic_mt_write_example(&dir_path)?;
        
        // Basic multi-threaded reading example
        basic_mt_read_example(&dir_path)?;
        
        // Using iterator interface with multi-threaded reader
        iterator_mt_example(&dir_path)?;
        
        println!("All multi-threaded examples completed successfully!");
        Ok(())
    }

    // Basic example of writing records using multi-threaded writer with multiple shards
    fn basic_mt_write_example(dir_path: &std::path::Path) -> disky::error::Result<()> {
        println!("Running basic multi-threaded write example...");
        
        // Create a FileSharder that will create multiple shard files
        let file_sharder = FileSharder::with_prefix(dir_path.to_path_buf(), "mt_shard");
        
        // Configure the sharding (3 shards in this case)
        let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 3);
        
        // Create the writer configuration with default settings and 4 worker threads
        let config = MultiThreadedWriterConfig {
            writer_config: ParallelWriterConfig::default(),
            worker_threads: 4,
        };
        
        // Create the multi-threaded writer
        let writer = MultiThreadedWriter::new(sharding_config, config)?;
        
        // Write some records
        for i in 0..30 {
            let data = format!("Multi-threaded Record #{}", i);
            let bytes = Bytes::from(data.into_bytes());
            
            // Write asynchronously
            let promise = writer.write_record(bytes)?;
            
            // Wait for the write to complete - using let _ to avoid the warning
            let _ = promise.wait()?;
        }
        
        // Flush and close the writer
        writer.flush()?;
        writer.close()?;
        
        println!("Wrote 30 records to sharded files");
        Ok(())
    }

    // Basic example of reading records using multi-threaded reader
    fn basic_mt_read_example(dir_path: &std::path::Path) -> disky::error::Result<()> {
        println!("Running basic multi-threaded read example...");
        
        // Create a FileShardLocator to read from our sharded files
        let shard_locator = FileShardLocator::new(dir_path.to_path_buf(), "mt_shard")?;
        
        // Configure the sharding
        let sharding_config = ReaderShardingConfig::new(Box::new(shard_locator), 3);
        
        // Create the reader configuration with 4 worker threads
        let config = MultiThreadedReaderConfig {
            reader_config: ParallelReaderConfig::default(),
            worker_threads: 4,
            queue_size_bytes: 8 * 1024 * 1024, // 8MB queue
        };
        
        // Create the multi-threaded reader
        let reader = MultiThreadedReader::new(sharding_config, config)?;
        
        // Read all records
        let mut count = 0;
        loop {
            match reader.read()? {
                DiskyParallelPiece::Record(bytes) => {
                    count += 1;
                    println!("Read MT record {}: {}", count, String::from_utf8_lossy(&bytes));
                }
                DiskyParallelPiece::EOF => break,
                DiskyParallelPiece::ShardFinished => {
                    println!("Shard finished");
                    continue;
                }
            }
        }
        
        println!("Read {} records from sharded files using multi-threaded reader", count);
        Ok(())
    }

    // Example of using the iterator interface with multi-threaded reader
    fn iterator_mt_example(dir_path: &std::path::Path) -> disky::error::Result<()> {
        println!("Running multi-threaded iterator example...");
        
        // Create a FileShardLocator for the sharded files
        let shard_locator = FileShardLocator::new(dir_path.to_path_buf(), "mt_shard")?;
        
        // Configure the sharding
        let sharding_config = ReaderShardingConfig::new(Box::new(shard_locator), 3);
        
        // Create the reader with default configuration
        let reader = MultiThreadedReader::new(sharding_config, MultiThreadedReaderConfig::default())?;
        
        // Use iterator interface to process records
        println!("Records read via multi-threaded iterator:");
        let mut count = 0;
        for record_result in reader {
            let record = record_result?;
            count += 1;
            println!("#{}: {}", count, String::from_utf8_lossy(&record));
        }
        
        println!("Read {} records using iterator interface", count);
        Ok(())
    }
}

fn main() -> disky::error::Result<()> {
    #[cfg(feature = "parallel")]
    {
        return parallel_example::run_examples();
    }
    
    #[cfg(not(feature = "parallel"))]
    {
        println!("This example requires the 'parallel' feature to be enabled.");
        println!("Please run with: cargo run --example multi_threaded_examples --features parallel");
        Ok(())
    }
}
