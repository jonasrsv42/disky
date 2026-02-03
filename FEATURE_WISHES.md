# Feature Wishes

## Checkpointing / Resume Support

Tree nodes already serialize via `serialize_as_bytes()`. Checkpointing needs opaque state per node.

Add to `Reader` trait in `src/tree/reader/mod.rs`:

```rust
pub trait Reader: Iterator<Item = Result<Bytes>> + Send + Sync {
    fn checkpoint(&self) -> Result<Vec<u8>>;
    fn restore(&mut self, state: &[u8]) -> Result<()>;
}
```

Each node serializes its own state. Examples:
- Sequential reader: shard index + record offset
- RandomRepeat: shard index + record offset + rng state
- Shuffle: entire buffer contents (can be large!)
- Sampling: child states + rng state

Composite nodes recursively checkpoint children.
Full checkpoint = serialized tree config + opaque state bytes.
