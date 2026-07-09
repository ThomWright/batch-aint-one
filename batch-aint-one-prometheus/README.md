# batch-aint-one-prometheus

Prometheus metrics for [batch-aint-one](https://crates.io/crates/batch-aint-one).

## Usage

```rust
use batch_aint_one::{Batcher, BatchingPolicy, Limits, Processor};
use batch_aint_one_prometheus::BatchMetrics;
use prometheus::Registry;

#[derive(Debug, Clone)]
struct MyProcessor;
impl Processor for MyProcessor {
    type Key = String;
    type Input = String;
    type Output = String;
    type Error = String;
    type Resources = ();
    async fn acquire_resources(&self, _key: String) -> Result<(), String> { Ok(()) }
    async fn process(&self, _key: String, inputs: impl Iterator<Item = String> + Send, _resources: ()) -> Result<Vec<String>, String> {
        Ok(inputs.collect())
    }
}

// Register metrics on your Prometheus registry.
let registry = Registry::new();
let metrics = BatchMetrics::new(&registry).unwrap();

// Pass the metrics to each Batcher. The batcher label is set from the batcher's name.
tokio_test::block_on(async {
let batcher = Batcher::builder()
    .name("my-batcher")
    .processor(MyProcessor)
    .limits(Limits::builder().max_batch_size(10).build())
    .batching_policy(BatchingPolicy::Immediate)
    .metrics(Box::new(metrics))
    .build();
});
```

All metrics are labelled with a `batcher` label, so multiple `Batcher` instances can share the same `BatchMetrics`. `BatchMetrics` implements [`MetricsRecorderFactory`](batch_aint_one::MetricsRecorderFactory), so the batcher creates its own recorder using its name.

Use `BatchMetrics::with_prefix` to customise the metric name prefix (default: `batch`).

## Metrics

| Metric                                        | Type      | Labels           | Description                                                      |
|-----------------------------------------------|-----------|------------------|------------------------------------------------------------------|
| `batch_items_received_total`                  | Counter   | batcher          | Items received by the batcher                                    |
| `batch_channel_duration_seconds`              | Histogram | batcher          | Time items spent in the channel before the worker picked them up |
| `batch_items_rejected_total`                  | Counter   | batcher          | Items rejected due to a full batch queue                         |
| `batch_batches_completed_total`               | Counter   | batcher, success | Batches that finished processing                                 |
| `batch_batch_size`                            | Histogram | batcher          | Number of items per batch                                        |
| `batch_batch_processing_duration_seconds`     | Histogram | batcher, success | Time taken to process a batch                                    |
| `batch_item_latency_seconds`                  | Histogram | batcher          | End-to-end latency per item, from submission to result delivery  |
| `batch_queue_duration_seconds`                | Histogram | batcher          | Time items spent in the batch queue before processing started    |
| `batch_resource_acquisition_duration_seconds` | Histogram | batcher, success | Time taken to acquire resources for a batch                      |
| `batch_active_keys`                           | Gauge     | batcher          | Number of keys with active batch queues                          |
| `batch_processing_concurrency`                | Gauge     | batcher          | Total number of batches currently processing                     |
| `batch_processing_concurrency_max_per_key`    | Gauge     | batcher          | Highest batch processing concurrency across any single key       |
| `batch_queue_depth`                           | Gauge     | batcher          | Total number of batches queued for processing                    |
| `batch_queue_depth_max_per_key`               | Gauge     | batcher          | Deepest batch queue across any single key                        |
