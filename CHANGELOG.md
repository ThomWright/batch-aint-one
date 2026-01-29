# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
