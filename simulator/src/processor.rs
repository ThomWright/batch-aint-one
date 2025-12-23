//! Simulated processor implementation

use crate::latency::LatencyProfile;
use crate::metrics::{BatchMetrics, MetricsCollector};
use crate::pool::{Connection, ConnectionPool};
use batch_aint_one::Processor;
use bon::bon;
use rand::Rng;
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
    /// Key used for this item
    pub key: String,
    /// When this item was submitted
    pub submitted_at: tokio::time::Instant,
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

    acquire_error_rate: f64,
    process_error_rate: f64,
}

#[bon]
impl SimProcessor {
    /// Create a new processor with Erlang-distributed latency
    #[builder]
    pub fn new(
        processing_latency: LatencyProfile,
        metrics: Arc<Mutex<MetricsCollector>>,
        pool: Option<Arc<ConnectionPool>>,
        #[builder(default = 0.0)] acquire_error_rate: f64,
        #[builder(default = 0.0)] process_error_rate: f64,
    ) -> Self {
        assert!(
            (0.0..=1.0).contains(&acquire_error_rate),
            "acquire_error_rate must be between 0.0 and 1.0"
        );
        assert!(
            (0.0..=1.0).contains(&process_error_rate),
            "process_error_rate must be between 0.0 and 1.0"
        );

        Self {
            batch_counter: Arc::new(AtomicUsize::new(0)),
            processing_latency: Arc::new(Mutex::new(processing_latency)),
            metrics,
            pool,
            acquire_error_rate,
            process_error_rate,
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
            let conn = pool.acquire().await;
            if self.acquire_error_rate > 0.0 {
                let mut rng = rand::rng();
                let roll: f64 = rng.random_range(0.0..1.0);
                if roll < self.acquire_error_rate {
                    return Err("simulated acquire error".to_string());
                }
            }
            Ok(Some(conn))
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

        if self.process_error_rate > 0.0 {
            let mut rng = rand::rng();
            let roll: f64 = rng.random_range(0.0..1.0);
            if roll < self.process_error_rate {
                return Err("simulated process error".to_string());
            }
        }

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

        // Return output for each input
        let outputs: Vec<_> = inputs
            .into_iter()
            .map(|input| SimulatedOutput {
                item_id: input.item_id,
                key: key.clone(),
                submitted_at: input.submitted_at,
                batch_id,
                batch_size,
                completed_at,
            })
            .collect();

        Ok(outputs)
    }
}
