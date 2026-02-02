# Migration: Generic Shards to Dynamic Dispatch

## Goal

Remove generics from the shard module to simplify FFI (Python bindings via PyO3). Currently, the generic `Source` type parameter propagates through the entire codebase, requiring separate Python wrapper classes for each source type.

## Current Architecture

```
Shards<Source>              // trait with associated type
  └── Shard<Source>         // struct containing source: Source
      └── SequentialShardReader<Source, ShardIter>
          └── RecordReader<Source>
              └── ParallelReader<Source>
                  └── MultiThreadedReader<Source>
```

Every layer is generic over `Source: Read + Seek`.

## Target Architecture

```
Shards                      // trait, no generics
  └── Shard { reader: Reader, id: String }
      └── SequentialShardReader  // no generics
          └── ParallelReader     // no generics
              └── MultiThreadedReader  // no generics
```

Type erasure happens inside `Shards::open()`, which creates the `RecordReader` internally and returns a boxed iterator.

## Key Changes

### 1. `Shard` struct (shard/source/mod.rs)

```rust
// Before
pub struct Shard<Source> {
    pub source: Source,
    pub id: String,
}

// After
pub struct Shard {
    pub reader: Reader,  // Box<dyn Iterator<Item = Result<Bytes>> + Send + Sync>
    pub id: String,
}
```

### 2. `Shards` trait (shard/source/mod.rs)

```rust
// Before
pub trait Shards {
    type Source: Read + Seek;
    fn count(&self) -> usize;
    fn open(&self, index: usize) -> Result<Shard<Self::Source>>;
}

// After
pub trait Shards: Send + Sync {
    fn count(&self) -> usize;
    fn open(&self, index: usize) -> Result<Shard>;
}
```

### 3. `FileShards` (shard/source/file.rs)

```rust
// Before: returns raw File
fn open(&self, index: usize) -> Result<Shard<File>> {
    let file = File::open(&self.paths[index])?;
    Ok(Shard { source: file, id: path.to_string() })
}

// After: creates RecordReader internally
fn open(&self, index: usize) -> Result<Shard> {
    let file = File::open(&self.paths[index])?;
    let reader = RecordReaderConfig::new(file)
        .options(self.reader_options)
        .build()?;
    Ok(Shard {
        reader: Box::new(reader),
        id: path.to_string()
    })
}
```

### 4. Iteration strategies (shard/source/sequential.rs, random.rs)

Remove generic parameters. These wrap `Box<dyn Shards>` and yield `Result<Shard>`.

### 5. Shard readers (shard/reader/sequential.rs, round_robin.rs)

Simplify - no longer create `RecordReader`, just chain `Reader`s from `Shard`.

### 6. Parallel module (parallel/reader.rs, multi_threaded_reader.rs)

Remove all generic parameters. `ShardingConfig`, `ParallelReader`, `MultiThreadedReader` become concrete types.

## Migration Order

1. [x] **shard/source/mod.rs** - Change `Shard` struct and `Shards` trait
2. [x] **shard/source/file.rs** - Update `FileShards` to create `RecordReader` in `open()`
3. [x] **shard/source/memory.rs** - Update `MemoryShards` similarly
4. [x] **shard/source/sequential.rs** - Remove generics from `SequentialShardSource`
5. [x] **shard/source/random.rs** - Remove generics from `RandomRepeatingShardSource`
6. [x] **shard/reader/sequential.rs** - Simplify `SequentialShardReader`
7. [x] **shard/reader/round_robin.rs** - Simplify `RoundRobinShardReader`
8. [x] **parallel/reader.rs** - Remove generics from `ShardingConfig`, `ParallelReader`
9. [x] **parallel/multi_threaded_reader.rs** - Remove generics from `MultiThreadedReader`
10. [x] **Update tests** - Fix all test files (405 passed, including parallel tests)
11. [ ] **Update pisky bindings** - Simplify Python wrapper classes

## RecordReaderOptions Handling

Options move from reader level to shards level:

```rust
// Before
SequentialShardReaderConfig::new(source).reader_options(opts)

// After
FileShards::new(paths).reader_options(opts)
```

## Pitfalls

1. **Shard ID preservation** - Keep `id: String` in `Shard` for error context
2. **Error wrapping** - Errors from `RecordReader` should include shard context
3. **Thread safety** - `Reader` requires `Send + Sync`, ensure all sources comply
4. **Test mocks** - `MemoryShards` must create valid `RecordReader` from test data

## Benefits

1. **Simpler FFI** - Single Python class instead of one per source type
2. **No vtable overhead on reads** - `RecordReader<File>` reads directly, only `Iterator::next()` is virtual
3. **Cleaner API** - `Shards::open()` returns what you want (iterator of records)
4. **Reduced code** - No generic parameter propagation through 6+ types
