# Tree Reader Architecture

This module provides a composable tree-based reader architecture where any node
implements `Iterator<Item = Result<Bytes>>`, enabling flexible composition and
threading strategies.

## Node Types

All nodes implement `Iterator<Item = Result<Bytes>>`:

```
Iterator<Item = Result<Bytes>>
├── RecordReader<Source>      # Leaf - reads from a single file/source
├── ShardReader<Source, S>    # Owns a Shards impl, manages RecordReaders internally
├── SamplingReader<I>         # Weighted random sampling from children
├── TreeReader<I>             # Round-robin or scheduled reads from children
└── ThreadedReader<I>         # Thread boundary - owns subtree, exposes queue
```

## Composability

Because all nodes share the same trait, they compose arbitrarily:

```
                    SamplingReader
                   /              \
           ShardReader          TreeReader
               |                /        \
           [shards]     RecordReader   ShardReader
                                           |
                                       [shards]
```

### Hierarchical Sampling

`SamplingReader` can nest, enabling hierarchical sampling strategies:

```
SamplingReader (70% group A, 30% group B)
├── SamplingReader (group A: 50% dataset1, 50% dataset2)
│   ├── ShardReader (dataset1)
│   └── ShardReader (dataset2)
└── SamplingReader (group B: 80% dataset3, 20% dataset4)
    ├── ShardReader (dataset3)
    └── ShardReader (dataset4)
```

This allows sampling across groups of datasets, then within those groups,
with independent weights at each level.

## Threading Model

`ThreadedReader<I>` acts as a thread boundary:
- Takes ownership of any `Iterator<Item = Result<Bytes>>` (a subtree)
- Spawns a thread that drives the subtree and pushes records to a queue
- Exposes `Iterator<Item = Result<Bytes>>` by reading from that queue

This allows injecting parallelism at any level:

```
              SamplingReader          <- main thread reads from queues
               /           \
       ThreadedReader    ThreadedReader
            |                |
       ShardReader       TreeReader    <- each subtree owned by its thread
            |             /    \
        [shards]    ShardReader ShardReader
```

### Flexible Thread Assignment

- Single thread across multiple datasets: one `ThreadedReader` wrapping a `TreeReader`
- One thread per dataset: each dataset wrapped in its own `ThreadedReader`
- N threads for M datasets: partition datasets into N groups, each in a `ThreadedReader`

## Design Principles

1. **No internal synchronization in leaf components** - `Shards`, `Sharder`, etc.
   use `&self` / `&mut self`. Synchronization is managed by owners (e.g., `ThreadedReader`).

2. **Ownership flows down the tree** - each node owns its children completely.

3. **Thread boundaries are explicit** - `ThreadedReader` is the only place where
   threading is introduced, making the concurrency model clear.

4. **Uniform interface** - everything is `Iterator<Item = Result<Bytes>>`, so new
   node types can be added without changing existing code.

## Implementation Status

- [x] `RecordReader` - exists in `src/reader.rs`
- [x] `SamplingReader` - exists in `src/sampling/sampling_reader.rs`
- [x] `Shards` trait and implementations - exists in `src/shard/source/`
- [ ] `ShardReader` - to be implemented in `src/shard/reader.rs`
- [ ] `TreeReader` - to be implemented
- [ ] `ThreadedReader` - to be implemented
