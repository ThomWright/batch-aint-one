//! Metrics recording for batcher observability.

use std::{fmt::Debug, time::Duration};

/// Records metrics about batcher activity.
///
/// Implement this trait to bridge batcher metrics to your metrics system (e.g. Prometheus,
/// OpenTelemetry). All methods have default no-op implementations, so you only need to override
/// the ones you care about.
///
/// All methods are called synchronously on the background worker's event loop, so implementations
/// should be cheap and non-blocking.
pub trait MetricsRecorder: Debug + Send + Sync + 'static {
    /// An item was received by the worker.
    ///
    /// `channel_duration` is the time the item spent waiting in the channel between submission
    /// and the worker picking it up.
    fn item_received(&self, _channel_duration: Duration) {}

    /// An item was rejected because the batch queue for its key is full.
    fn item_rejected(&self) {}

    /// A batch finished processing and results were sent back to callers.
    fn batch_completed(&self, _metrics: &BatchStats) {}

    /// Resource acquisition completed.
    fn resource_acquisition_completed(&self, _duration: Duration, _success: bool) {}

    /// The number of active keys (keys with batch queues) changed.
    fn active_keys_changed(&self, _count: usize) {}

    /// The total number of batches currently processing changed.
    ///
    /// `max_per_key` is the highest concurrency across any single key, useful for detecting
    /// hotspots without per-key labels.
    fn processing_concurrency_changed(&self, _total: usize, _max_per_key: usize) {}

    /// The total number of batches queued for processing changed.
    ///
    /// `max_per_key` is the deepest queue across any single key, useful for detecting
    /// saturation without per-key labels.
    fn queue_depth_changed(&self, _total: usize, _max_per_key: usize) {}
}

/// Stats for a completed batch.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BatchStats {
    /// The number of items in the batch.
    pub size: usize,
    /// How long the batch took to process (including resource acquisition if not pre-acquired).
    pub processing_duration: Duration,
    /// Whether the batch processed successfully.
    pub success: bool,
    /// Time from submission to result delivery, for each item in the batch.
    pub item_latencies: Vec<Duration>,
    /// Time each item spent in the batch queue before processing started.
    ///
    /// Measured from when the worker received the item to when the batch began processing.
    /// Includes time waiting for the batch to fill, for concurrency capacity, and for
    /// resource acquisition.
    pub queue_durations: Vec<Duration>,
}

impl BatchStats {
    /// Create a new `BatchStats`.
    pub fn new(
        size: usize,
        processing_duration: Duration,
        success: bool,
        item_latencies: Vec<Duration>,
        queue_durations: Vec<Duration>,
    ) -> Self {
        Self {
            size,
            processing_duration,
            success,
            item_latencies,
            queue_durations,
        }
    }
}

/// Creates a [`MetricsRecorder`] for a named batcher.
pub trait MetricsRecorderFactory: Debug + Send + Sync + 'static {
    /// Create a [`MetricsRecorder`] for the given batcher name.
    fn create_recorder(&self, batcher_name: &str) -> Box<dyn MetricsRecorder>;
}

/// A no-op metrics recorder that discards all metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::{Batcher, BatchingPolicy, Limits, Processor};

    use super::*;

    #[derive(Debug, Default)]
    struct TestRecorderInner {
        items_received: usize,
        items_rejected: usize,
        batches_completed: usize,
        batch_sizes: Vec<usize>,
        item_latencies: Vec<Duration>,
        channel_durations: Vec<Duration>,
        resource_acquisitions: usize,
        active_keys: Vec<usize>,
        processing_concurrency: (usize, usize),
        queue_depth: (usize, usize),
    }

    #[derive(Debug)]
    struct TestRecorder(Arc<Mutex<TestRecorderInner>>);

    #[derive(Debug)]
    struct TestRecorderFactory(Arc<Mutex<TestRecorderInner>>);

    impl MetricsRecorderFactory for TestRecorderFactory {
        fn create_recorder(&self, _batcher_name: &str) -> Box<dyn MetricsRecorder> {
            Box::new(TestRecorder(self.0.clone()))
        }
    }

    fn test_metrics() -> (
        Arc<Mutex<TestRecorderInner>>,
        Box<dyn MetricsRecorderFactory>,
    ) {
        let state = Arc::new(Mutex::new(TestRecorderInner::default()));
        let factory = Box::new(TestRecorderFactory(state.clone()));
        (state, factory)
    }

    impl MetricsRecorder for TestRecorder {
        fn item_received(&self, channel_duration: Duration) {
            let mut inner = self.0.lock().unwrap();
            inner.items_received += 1;
            inner.channel_durations.push(channel_duration);
        }

        fn item_rejected(&self) {
            self.0.lock().unwrap().items_rejected += 1;
        }

        fn batch_completed(&self, metrics: &BatchStats) {
            let mut inner = self.0.lock().unwrap();
            inner.batches_completed += 1;
            inner.batch_sizes.push(metrics.size);
            inner
                .item_latencies
                .extend_from_slice(&metrics.item_latencies);
        }

        fn resource_acquisition_completed(&self, _duration: Duration, _success: bool) {
            self.0.lock().unwrap().resource_acquisitions += 1;
        }

        fn active_keys_changed(&self, count: usize) {
            self.0.lock().unwrap().active_keys.push(count);
        }

        fn processing_concurrency_changed(&self, total: usize, max_per_key: usize) {
            self.0.lock().unwrap().processing_concurrency = (total, max_per_key);
        }

        fn queue_depth_changed(&self, total: usize, max_per_key: usize) {
            self.0.lock().unwrap().queue_depth = (total, max_per_key);
        }
    }

    #[derive(Debug, Clone)]
    struct SimpleProcessor {
        process_delay: Duration,
    }

    impl SimpleProcessor {
        fn instant() -> Self {
            Self {
                process_delay: Duration::ZERO,
            }
        }
    }

    impl Processor for SimpleProcessor {
        type Key = String;
        type Input = String;
        type Output = String;
        type Error = String;
        type Resources = ();

        async fn acquire_resources(&self, _key: String) -> Result<(), String> {
            Ok(())
        }

        async fn process(
            &self,
            _key: String,
            inputs: impl Iterator<Item = String> + Send,
            _resources: (),
        ) -> Result<Vec<String>, String> {
            if self.process_delay > Duration::ZERO {
                tokio::time::sleep(self.process_delay).await;
            }
            Ok(inputs.map(|s| s + " done").collect())
        }
    }

    async fn shut_down(batcher: &Batcher<SimpleProcessor>) {
        let worker = batcher.worker_handle();
        worker.shut_down().await;
        tokio::time::timeout(Duration::from_secs(1), worker.wait_for_shutdown())
            .await
            .expect("Worker should shut down");
    }

    #[tokio::test]
    async fn records_metrics_for_successful_batch() {
        let (state, factory) = test_metrics();

        let batcher = Batcher::builder()
            .name("test")
            .processor(SimpleProcessor::instant())
            .limits(Limits::builder().max_batch_size(2).build())
            .batching_policy(BatchingPolicy::Size)
            .metrics(factory)
            .build();

        let (r1, r2) = tokio::join!(
            batcher.add("A".to_string(), "1".to_string()),
            batcher.add("A".to_string(), "2".to_string()),
        );
        assert!(r1.is_ok());
        assert!(r2.is_ok());

        shut_down(&batcher).await;

        let inner = state.lock().unwrap();
        assert_eq!(inner.items_received, 2);
        assert_eq!(inner.batches_completed, 1);
        assert_eq!(inner.items_rejected, 0);
        assert_eq!(inner.batch_sizes, vec![2]);
        assert_eq!(inner.item_latencies.len(), 2);
        assert!(inner.item_latencies.iter().all(|d| *d > Duration::ZERO));
        assert_eq!(inner.channel_durations.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn records_rejection_metrics() {
        let (state, factory) = test_metrics();

        let batcher = Batcher::builder()
            .name("test")
            .processor(SimpleProcessor {
                process_delay: Duration::from_millis(100),
            })
            .limits(
                Limits::builder()
                    .max_batch_size(1)
                    .max_key_concurrency(1)
                    .max_batch_queue_size(1)
                    .build(),
            )
            .batching_policy(BatchingPolicy::Size)
            .metrics(factory)
            .build();

        // With max_batch_size=1 and max_batch_queue_size=1:
        // - Item 1 fills a batch and starts processing (slow processor keeps it in flight)
        // - Item 2 fills the replacement batch (queue is now full)
        // - Item 3 is rejected because the queue is full
        let (r1, r2, r3) = tokio::join!(
            batcher.add("A".to_string(), "1".to_string()),
            batcher.add("A".to_string(), "2".to_string()),
            batcher.add("A".to_string(), "3".to_string()),
        );

        let results = [&r1, &r2, &r3];
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let rejections = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(successes, 2);
        assert_eq!(rejections, 1);

        shut_down(&batcher).await;

        assert_eq!(state.lock().unwrap().items_rejected, 1);
    }

    #[tokio::test]
    async fn records_gauge_metrics() {
        let (state, factory) = test_metrics();

        let batcher = Batcher::builder()
            .name("test")
            .processor(SimpleProcessor::instant())
            .limits(Limits::builder().max_batch_size(1).build())
            .batching_policy(BatchingPolicy::Size)
            .metrics(factory)
            .build();

        let r = batcher.add("A".to_string(), "1".to_string()).await;
        assert!(r.is_ok());

        shut_down(&batcher).await;

        let inner = state.lock().unwrap();
        assert!(!inner.active_keys.is_empty());
        assert!(inner.active_keys.iter().any(|&c| c > 0));
        // Key goes idle and is removed after the batch finishes.
        assert_eq!(*inner.active_keys.last().unwrap(), 0);
    }
}
