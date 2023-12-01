use std::{future::Future, hash::Hash};

use async_trait::async_trait;
use pin_project::pin_project;
use tokio::sync::oneshot;

use crate::{
    batch::BatchItem,
    error::Result,
    limit::Limits,
    worker::{Worker, WorkerHandle},
};

/// Groups items to be processed in batches.
///
/// Takes inputs (`I`) grouped by a key (`K`) and processes multiple together in a batch. An output
/// (`O`) is produced for each input.
#[derive(Debug, Clone)]
pub struct Batcher<K, I, O> {
    worker: WorkerHandle<K, I, O>,
}

/// Awaits the output for an item added to a batch.
#[pin_project]
pub struct AddedToBatch<O> {
    #[pin]
    rx: oneshot::Receiver<O>,
}

/// Process a batch of inputs.
#[async_trait]
pub trait Processor<I, O> {
    /// Process the batch.
    ///
    /// The order of the outputs in the returned `Vec` must be the same as the order of the inputs in
    /// the given iterator.
    async fn process(&self, inputs: impl Iterator<Item = I> + Send) -> Vec<O>;
}

impl<K, I, O> Batcher<K, I, O>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
{
    pub(crate) fn new<F>(processor: F, limits: Limits<K, I, O>) -> Self
    where
        F: 'static + Send + Clone + Processor<I, O>,
    {
        let handle = Worker::spawn(processor, limits);

        Self { worker: handle }
    }

    /// Add an item to the batch.
    ///
    /// The returned future adds the item to the batch.
    pub async fn add(&self, key: K, input: I) -> Result<AddedToBatch<O>> {
        let (tx, rx) = oneshot::channel();
        self.worker.send(BatchItem { key, input, tx }).await?;

        Ok(AddedToBatch { rx })
    }

    /// Add an item to the batch.
    ///
    /// The returned future adds the item to the batch and awaits the result.
    pub async fn add_and_wait(&self, key: K, input: I) -> Result<O> {
        let (tx, rx) = oneshot::channel();
        self.worker.send(BatchItem { key, input, tx }).await?;

        Ok(rx.await?)
    }
}

impl<O> Future for AddedToBatch<O> {
    type Output = Result<O>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.rx.poll(cx).map_err(|err| err.into())
    }
}
