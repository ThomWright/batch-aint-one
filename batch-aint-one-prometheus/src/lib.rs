//! Prometheus metrics for [`batch-aint-one`](batch_aint_one).
//!
//! See the [README](https://github.com/ThomWright/batch-aint-one) for usage examples.

#[cfg(doctest)]
use doc_comment::doctest;
#[cfg(doctest)]
doctest!("../README.md");

use std::fmt::Debug;
use std::time::Duration;

use batch_aint_one::{BatchStats, MetricsRecorder, MetricsRecorderFactory};
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

const DEFAULT_PREFIX: &str = "batch";

const CHANNEL_DURATION_BUCKETS: &[f64] = &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1];
const BATCH_SIZE_BUCKETS: &[f64] = &[1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0];

/// Shared metric definitions registered on a Prometheus [`Registry`].
///
/// Create once, then call [`recorder`](Self::recorder) for each `Batcher` instance.
/// All metrics use a `batcher` label to distinguish between instances.
///
/// Metric types are `Clone`-cheap (`Arc`-backed internally), so cloning this struct is
/// inexpensive.
#[derive(Debug, Clone)]
pub struct BatchMetrics {
    items_received_total: IntCounterVec,
    channel_duration_seconds: HistogramVec,
    items_rejected_total: IntCounterVec,
    batches_completed_total: IntCounterVec,
    batch_size: HistogramVec,
    batch_processing_duration_seconds: HistogramVec,
    item_latency_seconds: HistogramVec,
    queue_duration_seconds: HistogramVec,
    resource_acquisition_duration_seconds: HistogramVec,
    active_keys: IntGaugeVec,
    processing_concurrency: IntGaugeVec,
    processing_concurrency_max_per_key: IntGaugeVec,
    queue_depth: IntGaugeVec,
    queue_depth_max_per_key: IntGaugeVec,
}

impl BatchMetrics {
    /// Register metrics on `registry` with the default prefix (`batch`).
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        Self::with_prefix(registry, DEFAULT_PREFIX)
    }

    /// Register metrics on `registry` with a custom prefix.
    ///
    /// Metric names follow the pattern `{prefix}_{metric_name}`.
    pub fn with_prefix(registry: &Registry, prefix: &str) -> prometheus::Result<Self> {
        let batcher_labels = &["batcher"];
        let batcher_success_labels = &["batcher", "success"];

        let items_received_total = IntCounterVec::new(
            Opts::new("items_received_total", "Items received by the batcher").namespace(prefix),
            batcher_labels,
        )?;
        let channel_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "channel_duration_seconds",
                "Time items spent in the channel before the worker picked them up",
            )
            .namespace(prefix)
            .buckets(CHANNEL_DURATION_BUCKETS.to_vec()),
            batcher_labels,
        )?;
        let items_rejected_total = IntCounterVec::new(
            Opts::new(
                "items_rejected_total",
                "Items rejected due to a full batch queue",
            )
            .namespace(prefix),
            batcher_labels,
        )?;
        let batches_completed_total = IntCounterVec::new(
            Opts::new(
                "batches_completed_total",
                "Batches that finished processing",
            )
            .namespace(prefix),
            batcher_success_labels,
        )?;
        let batch_size = HistogramVec::new(
            HistogramOpts::new("batch_size", "Number of items per batch")
                .namespace(prefix)
                .buckets(BATCH_SIZE_BUCKETS.to_vec()),
            batcher_labels,
        )?;
        let batch_processing_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "batch_processing_duration_seconds",
                "Time taken to process a batch",
            )
            .namespace(prefix),
            batcher_success_labels,
        )?;
        let item_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "item_latency_seconds",
                "End-to-end latency per item, from submission to result delivery",
            )
            .namespace(prefix),
            batcher_labels,
        )?;
        let queue_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "queue_duration_seconds",
                "Time items spent in the batch queue before processing started",
            )
            .namespace(prefix),
            batcher_labels,
        )?;
        let resource_acquisition_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "resource_acquisition_duration_seconds",
                "Time taken to acquire resources for a batch",
            )
            .namespace(prefix),
            batcher_success_labels,
        )?;
        let active_keys = IntGaugeVec::new(
            Opts::new("active_keys", "Number of keys with active batch queues").namespace(prefix),
            batcher_labels,
        )?;
        let processing_concurrency = IntGaugeVec::new(
            Opts::new(
                "processing_concurrency",
                "Total number of batches currently processing",
            )
            .namespace(prefix),
            batcher_labels,
        )?;
        let processing_concurrency_max_per_key = IntGaugeVec::new(
            Opts::new(
                "processing_concurrency_max_per_key",
                "Highest batch processing concurrency across any single key",
            )
            .namespace(prefix),
            batcher_labels,
        )?;
        let queue_depth = IntGaugeVec::new(
            Opts::new(
                "queue_depth",
                "Total number of batches queued for processing",
            )
            .namespace(prefix),
            batcher_labels,
        )?;
        let queue_depth_max_per_key = IntGaugeVec::new(
            Opts::new(
                "queue_depth_max_per_key",
                "Deepest batch queue across any single key",
            )
            .namespace(prefix),
            batcher_labels,
        )?;

        registry.register(Box::new(items_received_total.clone()))?;
        registry.register(Box::new(channel_duration_seconds.clone()))?;
        registry.register(Box::new(items_rejected_total.clone()))?;
        registry.register(Box::new(batches_completed_total.clone()))?;
        registry.register(Box::new(batch_size.clone()))?;
        registry.register(Box::new(batch_processing_duration_seconds.clone()))?;
        registry.register(Box::new(item_latency_seconds.clone()))?;
        registry.register(Box::new(queue_duration_seconds.clone()))?;
        registry.register(Box::new(resource_acquisition_duration_seconds.clone()))?;
        registry.register(Box::new(active_keys.clone()))?;
        registry.register(Box::new(processing_concurrency.clone()))?;
        registry.register(Box::new(processing_concurrency_max_per_key.clone()))?;
        registry.register(Box::new(queue_depth.clone()))?;
        registry.register(Box::new(queue_depth_max_per_key.clone()))?;

        Ok(Self {
            items_received_total,
            channel_duration_seconds,
            items_rejected_total,
            batches_completed_total,
            batch_size,
            batch_processing_duration_seconds,
            item_latency_seconds,
            queue_duration_seconds,
            resource_acquisition_duration_seconds,
            active_keys,
            processing_concurrency,
            processing_concurrency_max_per_key,
            queue_depth,
            queue_depth_max_per_key,
        })
    }

    /// Create a [`BatchMetricsRecorder`] for a specific batcher instance.
    ///
    /// The `batcher_name` is used as the value for the `batcher` label on all metrics.
    pub fn recorder(&self, batcher_name: &str) -> BatchMetricsRecorder {
        BatchMetricsRecorder {
            metrics: self.clone(),
            batcher_name: batcher_name.to_string(),
        }
    }
}

impl MetricsRecorderFactory for BatchMetrics {
    fn create_recorder(&self, batcher_name: &str) -> Box<dyn MetricsRecorder> {
        Box::new(self.recorder(batcher_name))
    }
}

/// Records metrics for a single batcher instance.
///
/// Created via [`BatchMetrics::recorder`]. Implements [`MetricsRecorder`] so it can be
/// passed to `Batcher::builder().metrics(...)`.
#[derive(Debug, Clone)]
pub struct BatchMetricsRecorder {
    metrics: BatchMetrics,
    batcher_name: String,
}

impl MetricsRecorder for BatchMetricsRecorder {
    fn item_received(&self, channel_duration: Duration) {
        let name = &self.batcher_name;
        self.metrics
            .items_received_total
            .with_label_values(&[name])
            .inc();
        self.metrics
            .channel_duration_seconds
            .with_label_values(&[name])
            .observe(channel_duration.as_secs_f64());
    }

    fn item_rejected(&self) {
        self.metrics
            .items_rejected_total
            .with_label_values(&[&self.batcher_name])
            .inc();
    }

    fn batch_completed(&self, metrics: &BatchStats) {
        let name = self.batcher_name.as_str();
        let success = if metrics.success { "true" } else { "false" };

        self.metrics
            .batches_completed_total
            .with_label_values(&[name, success])
            .inc();
        self.metrics
            .batch_size
            .with_label_values(&[name])
            .observe(metrics.size as f64);
        self.metrics
            .batch_processing_duration_seconds
            .with_label_values(&[name, success])
            .observe(metrics.processing_duration.as_secs_f64());

        for latency in &metrics.item_latencies {
            self.metrics
                .item_latency_seconds
                .with_label_values(&[name])
                .observe(latency.as_secs_f64());
        }
        for queue_duration in &metrics.queue_durations {
            self.metrics
                .queue_duration_seconds
                .with_label_values(&[name])
                .observe(queue_duration.as_secs_f64());
        }
    }

    fn resource_acquisition_completed(&self, duration: Duration, success: bool) {
        let success = if success { "true" } else { "false" };
        self.metrics
            .resource_acquisition_duration_seconds
            .with_label_values(&[self.batcher_name.as_str(), success])
            .observe(duration.as_secs_f64());
    }

    fn active_keys_changed(&self, count: usize) {
        self.metrics
            .active_keys
            .with_label_values(&[&self.batcher_name])
            .set(count as i64);
    }

    fn processing_concurrency_changed(&self, total: usize, max_per_key: usize) {
        let name = &self.batcher_name;
        self.metrics
            .processing_concurrency
            .with_label_values(&[name])
            .set(total as i64);
        self.metrics
            .processing_concurrency_max_per_key
            .with_label_values(&[name])
            .set(max_per_key as i64);
    }

    fn queue_depth_changed(&self, total: usize, max_per_key: usize) {
        let name = &self.batcher_name;
        self.metrics
            .queue_depth
            .with_label_values(&[name])
            .set(total as i64);
        self.metrics
            .queue_depth_max_per_key
            .with_label_values(&[name])
            .set(max_per_key as i64);
    }
}

#[cfg(test)]
mod tests {
    use prometheus::proto::MetricFamily;

    use super::*;

    fn get_metric<'a>(families: &'a [MetricFamily], name: &str) -> Option<&'a MetricFamily> {
        families.iter().find(|f| f.name() == name)
    }

    fn get_counter_value(families: &[MetricFamily], name: &str, labels: &[(&str, &str)]) -> f64 {
        let family =
            get_metric(families, name).unwrap_or_else(|| panic!("metric {name} not found"));
        for metric in family.get_metric() {
            if labels_match(metric, labels) {
                return metric.get_counter().value();
            }
        }
        panic!("no metric {name} with labels {labels:?}");
    }

    fn get_gauge_value(families: &[MetricFamily], name: &str, labels: &[(&str, &str)]) -> f64 {
        let family =
            get_metric(families, name).unwrap_or_else(|| panic!("metric {name} not found"));
        for metric in family.get_metric() {
            if labels_match(metric, labels) {
                return metric.get_gauge().value();
            }
        }
        panic!("no metric {name} with labels {labels:?}");
    }

    fn get_histogram_count(families: &[MetricFamily], name: &str, labels: &[(&str, &str)]) -> u64 {
        let family =
            get_metric(families, name).unwrap_or_else(|| panic!("metric {name} not found"));
        for metric in family.get_metric() {
            if labels_match(metric, labels) {
                return metric.get_histogram().get_sample_count();
            }
        }
        panic!("no metric {name} with labels {labels:?}");
    }

    fn get_histogram_sum(families: &[MetricFamily], name: &str, labels: &[(&str, &str)]) -> f64 {
        let family =
            get_metric(families, name).unwrap_or_else(|| panic!("metric {name} not found"));
        for metric in family.get_metric() {
            if labels_match(metric, labels) {
                return metric.get_histogram().get_sample_sum();
            }
        }
        panic!("no metric {name} with labels {labels:?}");
    }

    fn labels_match(metric: &prometheus::proto::Metric, expected: &[(&str, &str)]) -> bool {
        expected.iter().all(|(k, v)| {
            metric
                .get_label()
                .iter()
                .any(|l| l.name() == *k && l.value() == *v)
        })
    }

    #[test]
    fn records_item_received() {
        let registry = Registry::new();
        let metrics = BatchMetrics::new(&registry).unwrap();
        let recorder = metrics.recorder("test");

        recorder.item_received(Duration::from_millis(5));
        recorder.item_received(Duration::from_millis(10));

        let families = registry.gather();
        let labels = &[("batcher", "test")];

        assert_eq!(
            get_counter_value(&families, "batch_items_received_total", labels),
            2.0,
        );
        assert_eq!(
            get_histogram_count(&families, "batch_channel_duration_seconds", labels),
            2,
        );
        assert!(get_histogram_sum(&families, "batch_channel_duration_seconds", labels) > 0.0,);
    }

    #[test]
    fn records_item_rejected() {
        let registry = Registry::new();
        let metrics = BatchMetrics::new(&registry).unwrap();
        let recorder = metrics.recorder("test");

        recorder.item_rejected();

        let families = registry.gather();
        assert_eq!(
            get_counter_value(
                &families,
                "batch_items_rejected_total",
                &[("batcher", "test")]
            ),
            1.0,
        );
    }

    #[test]
    fn records_batch_completed() {
        let registry = Registry::new();
        let metrics = BatchMetrics::new(&registry).unwrap();
        let recorder = metrics.recorder("test");

        let batch_metrics = BatchStats::new(
            3,
            Duration::from_millis(50),
            true,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(110),
                Duration::from_millis(120),
            ],
            vec![
                Duration::from_millis(40),
                Duration::from_millis(30),
                Duration::from_millis(20),
            ],
        );
        recorder.batch_completed(&batch_metrics);

        let families = registry.gather();
        let success_labels = &[("batcher", "test"), ("success", "true")];
        let batcher_labels = &[("batcher", "test")];

        assert_eq!(
            get_counter_value(&families, "batch_batches_completed_total", success_labels),
            1.0,
        );
        assert_eq!(
            get_histogram_count(&families, "batch_batch_size", batcher_labels),
            1,
        );
        assert_eq!(
            get_histogram_sum(&families, "batch_batch_size", batcher_labels),
            3.0,
        );
        assert_eq!(
            get_histogram_count(
                &families,
                "batch_batch_processing_duration_seconds",
                success_labels,
            ),
            1,
        );
        assert_eq!(
            get_histogram_count(&families, "batch_item_latency_seconds", batcher_labels),
            3,
        );
        assert_eq!(
            get_histogram_count(&families, "batch_queue_duration_seconds", batcher_labels),
            3,
        );
    }

    #[test]
    fn records_resource_acquisition() {
        let registry = Registry::new();
        let metrics = BatchMetrics::new(&registry).unwrap();
        let recorder = metrics.recorder("test");

        recorder.resource_acquisition_completed(Duration::from_millis(25), true);
        recorder.resource_acquisition_completed(Duration::from_millis(100), false);

        let families = registry.gather();

        assert_eq!(
            get_histogram_count(
                &families,
                "batch_resource_acquisition_duration_seconds",
                &[("batcher", "test"), ("success", "true")],
            ),
            1,
        );
        assert_eq!(
            get_histogram_count(
                &families,
                "batch_resource_acquisition_duration_seconds",
                &[("batcher", "test"), ("success", "false")],
            ),
            1,
        );
    }

    #[test]
    fn records_gauge_metrics() {
        let registry = Registry::new();
        let metrics = BatchMetrics::new(&registry).unwrap();
        let recorder = metrics.recorder("test");

        recorder.active_keys_changed(5);
        recorder.processing_concurrency_changed(3, 2);
        recorder.queue_depth_changed(10, 4);

        let families = registry.gather();
        let labels = &[("batcher", "test")];

        assert_eq!(get_gauge_value(&families, "batch_active_keys", labels), 5.0);
        assert_eq!(
            get_gauge_value(&families, "batch_processing_concurrency", labels),
            3.0,
        );
        assert_eq!(
            get_gauge_value(
                &families,
                "batch_processing_concurrency_max_per_key",
                labels
            ),
            2.0,
        );
        assert_eq!(
            get_gauge_value(&families, "batch_queue_depth", labels),
            10.0
        );
        assert_eq!(
            get_gauge_value(&families, "batch_queue_depth_max_per_key", labels),
            4.0,
        );
    }

    #[test]
    fn multiple_batchers_have_separate_labels() {
        let registry = Registry::new();
        let metrics = BatchMetrics::new(&registry).unwrap();
        let recorder_a = metrics.recorder("batcher-a");
        let recorder_b = metrics.recorder("batcher-b");

        recorder_a.item_received(Duration::from_millis(1));
        recorder_a.item_received(Duration::from_millis(1));
        recorder_b.item_received(Duration::from_millis(1));

        let families = registry.gather();

        assert_eq!(
            get_counter_value(
                &families,
                "batch_items_received_total",
                &[("batcher", "batcher-a")],
            ),
            2.0,
        );
        assert_eq!(
            get_counter_value(
                &families,
                "batch_items_received_total",
                &[("batcher", "batcher-b")],
            ),
            1.0,
        );
    }

    #[test]
    fn custom_prefix() {
        let registry = Registry::new();
        let metrics = BatchMetrics::with_prefix(&registry, "myapp").unwrap();
        let recorder = metrics.recorder("test");

        recorder.item_received(Duration::from_millis(1));

        let families = registry.gather();
        assert!(get_metric(&families, "myapp_items_received_total").is_some());
        assert!(get_metric(&families, "batch_items_received_total").is_none());
    }
}
