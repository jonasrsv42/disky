# TODO Items for Disky

## Riegeli Specification Compliance

The Riegeli specification includes formulas for valid chunk boundaries that we need to address in our implementation:

1. `previous_chunk % kBlockSize < kUsableBlockSize` - Our implementation correctly follows this formula
2. `next_chunk > 0` - Our implementation correctly follows this formula
3. `(next_chunk - 1) % kBlockSize >= kBlockHeaderSize` - Our implementation doesn't currently satisfy this formula in all cases

### Issues to Address

1. **Block Header next_chunk Calculation**: Our implementation calculates the `next_chunk` field differently than the Riegeli specification. This affects Formula 3 compliance. In our tests, we found:
   - When `next_chunk` should be exactly `usable_block_size + BLOCK_HEADER_SIZE`, our implementation sometimes uses different values.
   - We need to review the `write_block_header` function in `BlockWriter` to ensure correct `next_chunk` calculation.

2. **Block Header previous_chunk Calculation**: In multi-block chunks, our implementation sometimes sets `previous_chunk` to 0 when it should be calculated differently.

### Testing Improvements

We have added tests in `chunk_boundary_validation.rs` that validate our implementation against the Riegeli specification formulas, but have commented out the failing assertions. We should:

1. Fix the implementation to comply with all formulas
2. Uncomment the assertions in the tests
3. Add more comprehensive tests for edge cases

### Block Boundary Behavior

Our implementation correctly handles:
- Block headers at block boundaries
- Setting `previous_chunk=0` when appending at a block boundary
- Chunk continuity across block boundaries

But we need to improve:
- Formula 3 compliance for `next_chunk` calculations 
- Consistent calculations across all code paths