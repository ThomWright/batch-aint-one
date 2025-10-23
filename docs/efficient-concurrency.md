# Efficient concurrency

## Context

Imagine a system where our concurrency is database connections. These can be expensive to create (CPU), and to maintain (memory). Ideally, we use fewer connections and higher batch sizes for the most efficient way to process multiple items concurrently.

We can think of concurrency existing on two axes:

1. **Number of concurrent connections used** (up to `max_concurrency`)
2. **Batch size per connection** (up to `max_batch_size`)

The `Immediate` strategy prioritises maximizing the number of concurrent connections used before maximizing batch sizes.

## Problem

When using the `Immediate` strategy, when the rate of items arriving is <= the rate at which we can process them one at a time on C connections (where C = `max_concurrency`), our batch size will tend to be only 1.

We will maximise our connection pool concurrency before we maximise our batch sizes.

Example with 3 connections (pre-initialised):

1. Item 1 arrives, immediately start processing on C1
2. Item 2 arrives, immediately start processing on C2
3. Item 3 arrives, immediately start processing on C3
4. Item 1 finishes processing
5. Item 4 arrives, immediately start processing on C1
6. etc.

This can lead to inefficient processing, especially when a sudden influx of items requires acquiring lots of new connections, which can be slow.

## Solution: `Balanced` Policy

Implements a Nagle-like algorithm for batch processing.

### API

```rust
BatchingPolicy::Balanced { min_size_hint: usize }
```

### Behaviour

**When no batches are currently processing:**

- First item processes immediately
- Maintains low first-item latency when system is idle

**When batches are already processing:**

- If current batch size < `min_size_hint`:
  - Wait for either:
    - Batch size to reach `min_size_hint`, OR
    - Any batch to complete processing
- If current batch size >= `min_size_hint`:
  - Start acquiring resources and process immediately
  - Uses available concurrency once minimum efficiency threshold is met

This creates natural back-pressure during busy periods while maintaining low latency when idle.

### Key characteristics

- **Prioritizes concurrency using batching** to up to `min_size_hint`
- **Shifts to use available concurrency** above `min_size_hint`
- **Balances latency and resource efficiency** dynamically based on load
- **Simple configuration**: single `min_size_hint` parameter

### Edge cases

#### `min_size_hint = 1`

Degenerates to `Immediate` policy - processes as soon as any capacity is available.

#### `min_size_hint = max_batch_size`

Strongly prefers full batches, but will still process when:

1. System is idle (first item processes immediately)
2. Another batch finishes (uses available capacity with partial batch)
3. The batch fills to `max_batch_size` (new resources are acquired and then the batch starts processing concurrently)

This maximizes batch sizes before using more available concurrency. In effect, behaves opposite to `Immediate`, which maximizes concurrency before batch sizes.

#### `min_size_hint > max_batch_size`

**Invalid configuration** - validation error at runtime.

### Performance characteristics

**Best case latency (first item when idle):**

- Same as `Immediate`: processes immediately

**Worst case latency (item arriving to existing batch < min_size_hint):**

- Waits for: min(time_to_reach_hint, time_for_any_batch_to_finish)
- Bounded by processing time of current batches

**Throughput:**

- Better than `Immediate` at moderate load (fewer connections, larger batches)
- Equal to `Immediate` at high load (all concurrency utilized)
- Slightly worse than `Immediate` when load is bursty around min_size_hint threshold

### Implementation notes

**Resource acquisition**: Delayed until batch conditions are met.

**Interaction with other limits**: Works within existing `max_batch_size` and `max_key_concurrency` constraints.

**Processing trigger events**:

- Batch reaches `min_size_hint`
- Any batch completes (via `on_finish`)
- Never time-based (no timeouts)
