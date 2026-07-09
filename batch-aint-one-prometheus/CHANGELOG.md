# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 0.1.0

### Added

- Initial release: Prometheus `MetricsRecorder` implementation backed by the `prometheus` crate. Registers counters, histograms, and gauges for all `MetricsRecorder` callbacks, with a `batcher` label to distinguish multiple `Batcher` instances.
