//! Closure-based shard sink.

use std::io::{Seek, Write};

use crate::error::Result;
use crate::shard::sink::{Shard, Shards};

/// A closure-based shard factory. Implements [`Shards`].
///
/// Wraps an `FnMut() -> Result<S>` to produce sinks on demand.
/// Each shard gets an auto-incremented id like `closure_shard_0`, `closure_shard_1`, etc.
///
/// Useful for tests that need stateful factories (e.g., counting creations
/// or simulating failures after N calls).
///
/// # Examples
///
/// ```
/// use std::io::Cursor;
/// use disky::shard::sink::{ClosureShards, Shards};
///
/// let mut shards = ClosureShards::new(|| Ok(Cursor::new(Vec::new())));
///
/// let s0 = shards.next().unwrap();
/// assert_eq!(s0.id, "closure_shard_0");
/// ```
pub struct ClosureShards<ClosureType, SinkType> {
    factory: ClosureType,
    counter: usize,
    _marker: std::marker::PhantomData<SinkType>,
}

impl<ClosureType, SinkType> ClosureShards<ClosureType, SinkType>
where
    SinkType: Write + Seek,
    ClosureType: FnMut() -> Result<SinkType>,
{
    /// Create a new `ClosureShards` from a factory closure.
    pub fn new(factory: ClosureType) -> Self {
        Self {
            factory,
            counter: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<ClosureType, SinkType> Shards for ClosureShards<ClosureType, SinkType>
where
    SinkType: Write + Seek,
    ClosureType: FnMut() -> Result<SinkType>,
{
    type Sink = SinkType;

    fn next(&mut self) -> Result<Shard<SinkType>> {
        let sink = (self.factory)()?;
        let id = format!("closure_shard_{}", self.counter);
        self.counter += 1;
        Ok(Shard { sink, id })
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn produces_sinks_from_closure() {
        let mut shards = ClosureShards::new(|| Ok(Cursor::new(Vec::new())));

        let mut s = shards.next().unwrap();
        s.sink.write_all(b"hello").unwrap();

        let data = s.sink.into_inner();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn increments_ids() {
        let mut shards = ClosureShards::new(|| Ok(Cursor::new(Vec::new())));

        let s0 = shards.next().unwrap();
        let s1 = shards.next().unwrap();
        let s2 = shards.next().unwrap();

        assert_eq!(s0.id, "closure_shard_0");
        assert_eq!(s1.id, "closure_shard_1");
        assert_eq!(s2.id, "closure_shard_2");
    }

    #[test]
    fn closure_can_capture_state() {
        let counter = Arc::new(Mutex::new(0usize));
        let counter_clone = counter.clone();

        let mut shards = ClosureShards::new(move || {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
            Ok(Cursor::new(Vec::new()))
        });

        let _ = shards.next().unwrap();
        let _ = shards.next().unwrap();
        let _ = shards.next().unwrap();

        assert_eq!(*counter.lock().unwrap(), 3);
    }

    #[test]
    fn closure_can_fail() {
        let call_count = Arc::new(Mutex::new(0usize));
        let call_count_clone = call_count.clone();

        let mut shards = ClosureShards::new(move || {
            let mut count = call_count_clone.lock().unwrap();
            *count += 1;
            if *count > 2 {
                Err(crate::error::DiskyError::Other("fail".to_string()))
            } else {
                Ok(Cursor::new(Vec::new()))
            }
        });

        assert!(shards.next().is_ok());
        assert!(shards.next().is_ok());
        assert!(shards.next().is_err());
    }
}
