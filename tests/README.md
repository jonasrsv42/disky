# Disky Integration Tests and Benchmarks

This directory contains integration tests for the Disky binary file format implementation. Integration tests verify the functionality of the library from an external perspective, treating it as a black box.

## Running the Integration Tests

To run the integration tests:

```bash
cargo test --test '*'
```

This will run all integration tests in this directory.

## Running Benchmarks

Performance benchmarks are located in the `benches` directory. To run the benchmarks:

```bash
cargo bench
```

The benchmarks measure various aspects of performance including:

1. Writing records of different sizes
2. Reading records of different sizes
3. Stream processing benchmarks

## Test Categories

- **Round Trip Tests**: Verify that records written with `RecordWriter` can be correctly read back with `RecordReader`.
- **Large Records Tests**: Test handling of records that span multiple blocks.
- **Many Small Records Tests**: Test handling of many small records to verify proper chunking.
- **Iterator Interface Tests**: Test the iterator interface for reading records.

## Benchmark Categories

- **Writing Benchmarks**: Measure performance of writing records of various sizes.
- **Reading Benchmarks**: Measure performance of reading records of various sizes.
- **Stream Processing Benchmarks**: Measure performance of processing records in a streaming fashion.