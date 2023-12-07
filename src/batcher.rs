use std::{hash::Hash};

use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::Span;

use crate::{
    batch::BatchItem,
    error::Result,
    worker::{Worker, WorkerHandle},
    BatchingStrategy,
};

/// Groups items to be processed in batches.
///
/// Takes inputs (`I`) grouped by a key (`K`) and processes multiple together in a batch. An output
/// (`O`) is produced for each input.
#[derive(Debug, Clone)]
pub struct Batcher<K, I, O = ()> {
    worker: WorkerHandle<K, I, O>,
}

/// Process a batch of inputs.
#[async_trait]
pub trait Processor<I, O> {
    /// Process the batch.
    ///
    /// The order of the outputs in the returned `Vec` must be the same as the order of the inputs
    /// in the given iterator.
    async fn process(&self, inputs: impl Iterator<Item = I> + Send) -> Vec<O>;
}

impl<K, I, O> Batcher<K, I, O>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
{
    /// Create a new batcher.
    pub fn new<F>(processor: F, batching_strategy: BatchingStrategy) -> Self
    where
        F: 'static + Send + Clone + Processor<I, O>,
    {
        let handle = Worker::spawn(processor, batching_strategy);

        Self { worker: handle }
    }

    /// Add an item to the batch and await the result.
    pub async fn add(&self, key: K, input: I) -> Result<O> {
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

        Ok(rx.await?)
    }
}
