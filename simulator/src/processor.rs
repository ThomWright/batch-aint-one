//! Simulated processor implementation

use crate::latency::LatencyProfile;
use crate::metrics::{BatchMetrics, ItemMetrics, MetricsCollector};
use crate::pool::{Connection, ConnectionPool};
use batch_aint_one::Processor;
use bon::bon;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Input submitted to the batcher with timing information
#[derive(Debug, Clone)]
pub struct SimulatedInput {
    /// Unique item identifier
    pub item_id: usize,
    /// When this item was submitted
    pub submitted_at: tokio::time::Instant,
}

/// Output returned from processing with batch and timing information
#[derive(Debug, Clone)]
pub struct SimulatedOutput {
    /// Unique item identifier (matches input)
    pub item_id: usize,
    /// Batch identifier
    pub batch_id: usize,
    /// Number of items in this batch
    pub batch_size: usize,
    /// When processing completed
    pub completed_at: tokio::time::Instant,
}

/// Simulated processor with configurable latency distribution
#[derive(Clone)]
pub struct SimProcessor {
    batch_counter: Arc<AtomicUsize>,
    processing_latency: Arc<Mutex<LatencyProfile>>,
    metrics: Arc<Mutex<MetricsCollector>>,
    pool: Option<Arc<ConnectionPool>>,
}

#[bon]
impl SimProcessor {
    /// Create a new processor with Erlang-distributed latency
    #[builder]
    pub fn new(
        processing_latency: LatencyProfile,
        metrics: Arc<Mutex<MetricsCollector>>,
        pool: Option<Arc<ConnectionPool>>,
    ) -> Self {
        Self {
            batch_counter: Arc::new(AtomicUsize::new(0)),
            processing_latency: Arc::new(Mutex::new(processing_latency)),
            metrics,
            pool,
        }
    }
}

impl Processor for SimProcessor {
    type Key = String;
    type Input = SimulatedInput;
    type Output = SimulatedOutput;
    type Error = String;
    type Resources = Option<Connection>;

    async fn acquire_resources(&self, _key: Self::Key) -> Result<Self::Resources, Self::Error> {
        if let Some(pool) = &self.pool {
            Ok(Some(pool.acquire().await))
        } else {
            Ok(None)
        }
    }

    async fn process(
        &self,
        key: Self::Key,
        inputs: impl Iterator<Item = Self::Input> + Send,
        _connection: Self::Resources,
    ) -> Result<Vec<Self::Output>, Self::Error> {
        let inputs: Vec<_> = inputs.collect();
        let batch_size = inputs.len();
        let batch_id = self.batch_counter.fetch_add(1, Ordering::SeqCst);

        let processing_started_at = tokio::time::Instant::now();

        // Sample processing latency from distribution
        let latency = self
            .processing_latency
            .lock()
            .expect("should not panic while holding lock")
            .sample();

        tokio::time::sleep(latency).await;

        let completed_at = tokio::time::Instant::now();

        // Record batch metrics
        let batch_metrics = BatchMetrics {
            batch_id,
            key: key.clone(),
            batch_size,
            processing_started_at,
            completed_at,
        };
        self.metrics
            .lock()
            .expect("should not panic while holding lock")
            .record_batch(batch_metrics);

        // Return output for each input and record item metrics
        let outputs: Vec<_> = inputs
            .into_iter()
            .map(|input| {
                let item_metrics = ItemMetrics {
                    item_id: input.item_id,
                    key: key.clone(),
                    submitted_at: input.submitted_at,
                    completed_at,
                    batch_id,
                    batch_size,
                };
                self.metrics
                    .lock()
                    .expect("should not panic while holding lock")
                    .record_item(item_metrics);

                SimulatedOutput {
                    item_id: input.item_id,
                    batch_id,
                    batch_size,
                    completed_at,
                }
            })
            .collect();

        Ok(outputs)
    }
}
