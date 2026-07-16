# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
