# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.9.0

### Changed

- [Breaking] Replaced `Batcher::new` with `Batcher::builder`
- [Breaking] Prefixed `Limits` builder functions with `with_`
- [Breaking] Require a name for a `Batcher` for tracing/logs
- [Breaking] Replaced generics with associated types on `Processor`
- [Breaking] Added `acquire_resources()` to the `Processor` trait

### Added

- Ability to wait for shut down using `worker.wait_for_shutdown().await`
- Ability to shut down gracefully using `worker.shut_down().await`

### Fixed

- Improved `Processor` panic handling
