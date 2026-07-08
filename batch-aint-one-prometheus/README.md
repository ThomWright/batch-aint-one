# batch-aint-one-prometheus

Prometheus metrics for [batch-aint-one](https://crates.io/crates/batch-aint-one).

## Usage

```rust
use batch_aint_one_prometheus::PrometheusMetrics;
use prometheus::Registry;
use std::sync::Arc;

// Register metrics on your Prometheus registry.
let registry = Registry::new();
let metrics = PrometheusMetrics::new(&registry).unwrap();

// Create a recorder for each Batcher instance and pass it to the builder.
let recorder = Arc::new(metrics.recorder("my-batcher"));

// let batcher = Batcher::builder()
//     .name("my-batcher")
//     .processor(my_processor)
//     .limits(my_limits)
//     .batching_policy(BatchingPolicy::Immediate)
//     .metrics_recorder(recorder)
//     .build();
```

All metrics are labelled with a `batcher` label, so multiple `Batcher` instances can share the same `PrometheusMetrics`.

Use `PrometheusMetrics::with_prefix` to customise the metric name prefix (default: `batch`).

## Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `batch_items_received_total` | Counter | batcher | Items received by the batcher |
| `batch_channel_duration_seconds` | Histogram | batcher | Time items spent in the channel before the worker picked them up |
| `batch_items_rejected_total` | Counter | batcher | Items rejected due to a full batch queue |
| `batch_batches_completed_total` | Counter | batcher, success | Batches that finished processing |
| `batch_batch_size` | Histogram | batcher | Number of items per batch |
| `batch_batch_processing_duration_seconds` | Histogram | batcher, success | Time taken to process a batch |
| `batch_item_latency_seconds` | Histogram | batcher | End-to-end latency per item, from submission to result delivery |
| `batch_resource_acquisition_duration_seconds` | Histogram | batcher, success | Time taken to acquire resources for a batch |
| `batch_active_keys` | Gauge | batcher | Number of keys with active batch queues |
| `batch_processing_concurrency` | Gauge | batcher | Total number of batches currently processing |
| `batch_processing_concurrency_max_per_key` | Gauge | batcher | Highest batch processing concurrency across any single key |
| `batch_queue_depth` | Gauge | batcher | Total number of batches queued for processing |
| `batch_queue_depth_max_per_key` | Gauge | batcher | Deepest batch queue across any single key |
