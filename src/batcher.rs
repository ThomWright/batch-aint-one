use std::{fmt::Display, hash::Hash, sync::Arc};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::Span;

use crate::{
    batch::BatchItem,
    error::Result,
    policies::{BatchingPolicy, Limits},
    worker::{Worker, WorkerHandle},
};

/// Groups items to be processed in batches.
///
/// Takes inputs (`I`) grouped by a key (`K`) and processes multiple together in a batch. An output
/// (`O`) is produced for each input.
///
/// Errors (`E`) can be returned from a batch.
///
/// Cheap to clone.
#[derive(Debug)]
pub struct Batcher<K, I, O = (), E = String>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
{
    worker: Arc<WorkerHandle>,
    item_tx: mpsc::Sender<BatchItem<K, I, O, E>>,
}

/// Process a batch of inputs for a given key.
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
    pub fn new<F>(processor: F, limits: Limits, batching_policy: BatchingPolicy) -> Self
    where
        F: 'static + Send + Clone + Processor<K, I, O, E>,
    {
        let (handle, item_tx) = Worker::spawn(processor, limits, batching_policy);

        Self {
            worker: Arc::new(handle),
            item_tx,
        }
    }

    /// Add an item to the batch and await the result.
    pub async fn add(&self, key: K, input: I) -> Result<O, E> {
        // Record the span ID so we can link the shared processing span.
        let span_id = Span::current().id();

        let (tx, rx) = oneshot::channel();
        self.item_tx
            .send(BatchItem {
                key,
                input,
                tx,
                span_id,
            })
            .await?;

        rx.await?
    }
}

impl<K, I, O, E> Clone for Batcher<K, I, O, E>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
{
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            item_tx: self.item_tx.clone(),
        }
    }
}
