# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 0.2.1

### Added

- `batch_queue_items` and `batch_queue_items_max_per_key` gauges, tracking `MetricsRecorder::queue_items_changed`.

## 0.2.0

### Added

- `batch_queue_duration_seconds` histogram metric for per-item queue duration.
- `BatchMetrics` now implements `MetricsRecorderFactory`, so it can be passed directly to `Batcher::builder().metrics(Box::new(metrics))` without manually creating a recorder.

## 0.1.0

### Added

- Initial release: Prometheus `MetricsRecorder` implementation backed by the `prometheus` crate. Registers counters, histograms, and gauges for all `MetricsRecorder` callbacks, with a `batcher` label to distinguish multiple `Batcher` instances.
