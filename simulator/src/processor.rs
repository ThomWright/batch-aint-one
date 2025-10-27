//! Simulated processor implementation

use batch_aint_one::Processor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Simple processor that tracks batch sizes and simulates fixed latency
#[derive(Clone)]
pub struct SimProcessor {
    batch_counter: Arc<AtomicUsize>,

    processing_duration: tokio::time::Duration,
}

impl SimProcessor {
    /// Create a new processor with fixed processing duration
    pub fn new(processing_duration: tokio::time::Duration) -> Self {
        Self {
            batch_counter: Arc::new(AtomicUsize::new(0)),
            processing_duration,
        }
    }
}

impl Processor for SimProcessor {
    type Key = String;
    type Input = SimulatedInput;
    type Output = SimulatedOutput;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: Self::Key) -> Result<Self::Resources, Self::Error> {
        Ok(())
    }

    async fn process(
        &self,
        _key: Self::Key,
        inputs: impl Iterator<Item = Self::Input> + Send,
        _resources: Self::Resources,
    ) -> Result<Vec<Self::Output>, Self::Error> {
        let inputs: Vec<_> = inputs.collect();
        let batch_size = inputs.len();
        let batch_id = self.batch_counter.fetch_add(1, Ordering::SeqCst);

        // Simulate processing latency
        tokio::time::sleep(self.processing_duration).await;

        let completed_at = tokio::time::Instant::now();

        // Return output for each input
        let outputs = inputs
            .into_iter()
            .map(|input| SimulatedOutput {
                item_id: input.item_id,
                batch_id,
                batch_size,
                completed_at,
            })
            .collect();

        Ok(outputs)
    }
}
