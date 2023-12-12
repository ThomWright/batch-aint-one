use std::{fmt::Display, hash::Hash};

use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::Span;

use crate::{
    batch::BatchItem,
    error::Result,
    worker::{Worker, WorkerHandle},
    BatchError, BatchingStrategy,
};

/// Groups items to be processed in batches.
///
/// Takes inputs (`I`) grouped by a key (`K`) and processes multiple together in a batch. An output
/// (`O`) is produced for each input.
///
/// Cheap to clone.
#[derive(Debug)]
pub struct Batcher<K, I, O = (), E = String> {
    worker: WorkerHandle<K, I, O, E>,
}

/// Process a batch of inputs.
#[async_trait]
pub trait Processor<K, I, O = (), E = String>
where
    E: Display,
{
    /// Process the batch.
    ///
    /// The order of the outputs in the returned `Vec` must be the same as the order of the inputs
    /// in the given iterator.
    async fn process(
        &self,
        key: K,
        inputs: impl Iterator<Item = I> + Send,
    ) -> std::result::Result<Vec<O>, E>;
}

impl<K, I, O, E> Batcher<K, I, O, E>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
{
    /// Create a new batcher.
    pub fn new<F>(processor: F, batching_strategy: BatchingStrategy) -> Self
    where
        F: 'static + Send + Clone + Processor<K, I, O, E>,
    {
        let handle = Worker::spawn(processor, batching_strategy);

        Self { worker: handle }
    }

    /// Add an item to the batch and await the result.
    pub async fn add(&self, key: K, input: I) -> Result<O, E> {
        // Record the span ID so we can link the shared processing span.
        let span_id = Span::current().id();

        let (tx, rx) = oneshot::channel();
        self.worker
            .send(BatchItem {
                key,
                input,
                tx,
                span_id,
            })
            .await?;

        rx.await?.map_err(BatchError::BatchFailed)
    }
}

impl<K, I, O> Clone for Batcher<K, I, O> {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
        }
    }
}
