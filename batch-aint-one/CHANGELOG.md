# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 0.16.0

### Added

- `Limits::max_items_awaiting_worker`, to override how many items can wait for the background worker to accept them before `add()` waits for room. Unlike the other limits it applies to the whole batcher, not per key. The default is unchanged.
- `LimitsBuilder` and `BatcherBuilder` are now exported, so the API docs include their setters, e.g. each limit's default.
- `MetricsRecorder::gauge_recording` and `GaugeRecording`, to choose when gauges are recorded: after every change (the default), on an interval, or only when `WorkerHandle::record_gauges()` is called.
- `WorkerHandle::record_gauges()`, to record the current gauges on demand.
- `MetricsRecorder::items_cancelled`, recording items dropped without being processed because their callers stopped waiting.
- `Limits::max_key_queued_items`, limiting how many items can be queued per key. It defaults to `max_batch_size * max_key_concurrency * 2`, which rejects at the same point as the previous default.
- `Processor` and `MetricsRecorder` now document which methods may panic: panics in `Processor::acquire_resources()` and `Processor::process()` are caught, but the processor's other trait implementations, those of its associated types, and `MetricsRecorder` methods must not panic.
- `Processor` now documents that the batcher doesn't time out `acquire_resources()` or `process()`, so implementations should bound them.

### Changed

- Items whose callers stop waiting, by dropping the future returned by `Batcher::add()`, before their batch starts processing are no longer processed, and no longer count against the queue limit. Batches are made up of items whose callers are still waiting, so they stay full when callers give up. `Batcher::add()` documents the details.
- Every policy now starts a batch by acquiring resources, and forms the batch from the oldest queued items once they've been acquired. Previously `Size` and `Duration` formed the batch first, and so did `Immediate` and `Balanced` after another batch finished. Items which arrive while resources are being acquired now join the batch, so batches can be larger without waiting longer.
- When resource acquisitions for a key complete out of order, the first to complete now processes the oldest items, instead of the items waiting for that particular acquisition.
- Under `Size` and `Duration`, items now count against the queue limit until their batch starts processing, including while its resources are being acquired, as they already did under `Immediate` and `Balanced`. With slow resource acquisition, items can be rejected sooner.
- `BatchStats`' processing duration no longer includes resource acquisition under `Size` and `Duration`, matching `Immediate` and `Balanced`. Acquisition is reported by `MetricsRecorder::resource_acquisition_completed`.
- A failed resource acquisition fails the oldest queued items, up to `max_batch_size`, instead of the items that joined the batch it was for.
- `MetricsRecorder::queue_depth_changed` now reports how many batches the queued items would fill, counting a partial one. Keys with nothing queued now count as 0, instead of 1.
- Dropping the last `Batcher` no longer aborts the worker. Resource acquisition and processing already in progress run to completion, and `WorkerHandle::wait_for_shutdown()` now waits for them, instead of returning while they were still running. Batches which haven't started are still dropped, and so are resources acquired after the drop, without processing.
- While shutting down, the worker processes batches without waiting for the batching policy: a partial batch doesn't wait to fill up, and a `Duration` batch doesn't wait for its deadline. The concurrency limits still apply. Previously, a `Size` policy's final partial batch could stop shutdown from ever completing.
- Gauges are recorded once more when the worker shuts down, so the final values aren't left stale.
- With `GaugeRecording::OnChange`, gauges are now also recorded after timeouts and resource acquisition, which can start processing a batch.
- Batchers without metrics no longer compute gauges.

### Deprecated

- `OnFull::Reject`, in favour of `OnFull::Wait`, which behaves the same. Items are only rejected by the queue limit, whichever `OnFull` is used.
- `LimitsBuilder::max_batch_queue_size`, in favour of `max_key_queued_items`. It still limits the queue to `max_batch_queue_size * max_batch_size` items, unless `max_key_queued_items` is set.
- `MetricsRecorder::queue_depth_changed`, in favour of `queue_items_changed`. Batches only exist once they're processing, so it's still called, with how many batches the queued items would fill.

### Fixed

- `Duration` with `OnFull::Reject` no longer acquires resources for a full batch early when another batch finishes. It waits until the oldest item has waited for the full duration, as documented.
- Items added just as a shutting-down worker exits are now processed. Previously they could be dropped, and their callers got `BatchError::Rx`.

### Internal

- The `Duration` policy's deadlines are now handled in the worker's event loop, instead of spawning a timer task per batch. This removes a race where a late timer message could be matched to a later batch with the same key. It was harmless, but only because of how the `Duration` policy sets deadlines.

## 0.15.1

### Added

- `MetricsRecorder::queue_items_changed`, reporting the total number of items queued for processing, complementing the existing batch-count `queue_depth_changed`.

## 0.15.0

### Added

- Per-item `queue_durations` field on `BatchStats`, measuring time from worker receipt to processing start.
- `MetricsRecorderFactory` trait: the batcher creates its own recorder using its name, replacing the old `.metrics_recorder(Arc<dyn MetricsRecorder>)` builder method.

### Changed

- Builder method renamed from `.metrics_recorder()` to `.metrics()`, now accepts `Box<dyn MetricsRecorderFactory>`.

## 0.14.0

### Added

- Optional `MetricsRecorder` trait for observability, passed via `Batcher::builder().metrics_recorder(...)`. Implement this to bridge batcher metrics (item rates, batch sizes, latencies, rejections, queue depth, concurrency) to your metrics system.
- `BatchStats` struct with public constructor, passed to `MetricsRecorder::batch_completed`.

## 0.13.1

### Internal

- The worker event loop is now the single writer of batch state. Resource-acquisition tasks
  report their result by message instead of mutating shared state, removing the
  `Arc<Mutex<…>>` previously shared with those tasks and making the race fixed in 0.13.0
  structurally impossible. No public API or behaviour change.
- Resource-acquisition failures are now handled directly on the worker rather than via a spawned
  task and a round-trip message, keeping the in-flight concurrency counters symmetric.

## 0.13.0

### Changed

- `Limits` now panics on construction if any limit is zero, instead of panicking obscurely or
  stalling later
- `Limits::default()` now matches the builder defaults: `max_batch_queue_size` defaults to
  `max_key_concurrency * 2` (was `max_key_concurrency`)

### Fixed

- Fixed a race condition where a batch whose resource acquisition failed could be processed
  anyway, leaking concurrency capacity and eventually stalling the key
- Batch queues for idle keys are now removed, fixing unbounded memory growth when batching over
  many distinct keys
- The `batch.first_item_wait_time_secs` span attribute now has sub-second precision (was
  truncated to whole seconds, so almost always `0`)

### Documentation

- The batching policy comparison is now on `BatchingPolicy` itself, so it is visible on docs.rs
- Documented `shut_down()` semantics: new items are still accepted while shutting down, and
  shutdown may never complete under sustained traffic
- Documented `Limits` builder defaults and clarified when items are rejected
- Documented the `Processor::process` output invariant: exactly one output per input, in order
- Various smaller documentation fixes

## 0.12.0

### Changed

- [Breaking] Added `BatchError::ProcessorInvariantViolation` variant to indicate processor invariant violations

## 0.11.2

### Fixed

- Fixed double error messaging

## 0.11.1

### Fixed

- Add error sources to `BatchError` variants to implement `std::error::Error` correctly

## 0.11.0

### Added

- `BatchingPolicy` now implements `Display` for human-readable output
- `Limits` now implements `Display` for human-readable output
- `BatchingPolicy` now implements `Clone`

### Changed

- Improved logging: changed some debug logs to info level
- Improved documentation for `Limits`
- Internal: replaced atomic-based concurrency tracking with simpler counter variables, reducing potential for race conditions
- Internal refactoring: policy implementations moved to separate modules for better code organization

### Fixed

- Fixed bugs in policy logic
- Improved accuracy of limit checking

## 0.10.0

### Changed

- [Breaking] Updated to use `bon` for `Limits` builder
- [Breaking] `RejectionReason` enum variants refactored for clarity

### Added

- New `BatchingPolicy::Balanced` policy for efficient resource usage
- New `max_batch_queue_size` limit to control queued batches per key

## 0.9.0

### Changed

- [Breaking] Replaced `Batcher::new` with `Batcher::builder`
- [Breaking] Prefixed `Limits` builder functions with `with_`
- [Breaking] Require a name for a `Batcher` for tracing/logs
- [Breaking] Replaced generics with associated types on `Processor` (and pretty much everything else)
- [Breaking] Added `acquire_resources()` to the `Processor` trait

### Added

- Ability to wait for shut down using `worker.wait_for_shutdown().await`
- Ability to shut down gracefully using `worker.shut_down().await`

### Fixed

- Improved `Processor` panic handling
