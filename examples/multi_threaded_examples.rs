#[cfg(feature = "parallel")]
mod parallel_example {
    use bytes::Bytes;
    use disky::parallel::multi_threaded_reader::{MultiThreadedReaderConfig, ReadingOrder};
    use disky::parallel::multi_threaded_writer::MultiThreadedWriterConfig;
    use disky::parallel::reader::{DiskyParallelPiece, ShardingConfig as ReaderShardingConfig};
    use disky::parallel::writer::ShardingConfig as WriterShardingConfig;
    use disky::shard::sink::FileShardsBuilder;
    use disky::shard::source::{FileShards, SequentialShardSource};
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

        // Create a shard factory that will create multiple shard files
        let file_sharder = FileShardsBuilder::new(dir_path, "mt_shard").build()?;

        // Configure the sharding (3 shards in this case)
        let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 3);

        // Create the multi-threaded writer with 4 worker threads
        let writer = MultiThreadedWriterConfig::new(sharding_config)
            .with_worker_threads(4)
            .build()?;

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

        // Create a FileShards to read from our sharded files
        let file_shards = FileShards::from_pattern(dir_path.to_path_buf(), "mt_shard")?;
        let source = SequentialShardSource::new(file_shards);

        // Configure the sharding
        let sharding_config = ReaderShardingConfig::new(Box::new(source), 3);

        // Create the multi-threaded reader with 4 worker threads
        let reader = MultiThreadedReaderConfig::new(sharding_config)
            .with_worker_threads(4)
            .with_queue_size_bytes(8 * 1024 * 1024) // 8MB queue
            .with_reading_order(ReadingOrder::Drain)
            .build()?;

        // Read all records
        let mut count = 0;
        loop {
            match reader.read()? {
                DiskyParallelPiece::Record(bytes) => {
                    count += 1;
                    println!(
                        "Read MT record {}: {}",
                        count,
                        String::from_utf8_lossy(&bytes)
                    );
                }
                DiskyParallelPiece::EOF => break,
                DiskyParallelPiece::ShardFinished => {
                    println!("Shard finished");
                    continue;
                }
            }
        }

        println!(
            "Read {} records from sharded files using multi-threaded reader",
            count
        );
        Ok(())
    }

    // Example of using the iterator interface with multi-threaded reader
    fn iterator_mt_example(dir_path: &std::path::Path) -> disky::error::Result<()> {
        println!("Running multi-threaded iterator example...");

        // Create a FileShards for the sharded files
        let file_shards = FileShards::from_pattern(dir_path.to_path_buf(), "mt_shard")?;
        let source = SequentialShardSource::new(file_shards);

        // Configure the sharding
        let sharding_config = ReaderShardingConfig::new(Box::new(source), 3);

        // Create the reader with default configuration
        let reader = MultiThreadedReaderConfig::new(sharding_config).build()?;

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
        println!(
            "Please run with: cargo run --example multi_threaded_examples --features parallel"
        );
        Ok(())
    }
}
