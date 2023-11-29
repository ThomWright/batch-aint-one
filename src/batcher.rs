use std::hash::Hash;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, error::SendError},
    oneshot::{self, error::RecvError},
};

use crate::{
    limit::Limits,
    worker::{BatchItem, Worker},
};

/// Groups items to be processed in batches.
///
/// Takes inputs (`I`) grouped by a key (`K`) and processes multiple together in a batch. An output
/// (`O`) is produced for each input.
#[derive(Debug, Clone)]
pub struct Batcher<K, I, O> {
    tx: mpsc::Sender<BatchItem<K, I, O>>,
}

#[derive(Error, Debug)]
pub enum BatchError {
    // TODO: better error variants
    #[error(transparent)]
    Rx(RecvError),
    #[error("UIUUGGGHH")]
    Tx,
}

/// Process a batch of inputs.
///
/// The order of the outputs in the returned `Vec` must be the same as the order of the inputs in
/// the given iterator.
#[async_trait]
pub trait Processor<I, O> {
    async fn process(&self, inputs: impl Iterator<Item = I>) -> Vec<O>;
}

type Result<T> = std::result::Result<T, BatchError>;

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
        let tx = Worker::spawn(processor, limits);

        Self { tx }
    }

    pub async fn add(&self, key: K, input: I) -> Result<O> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(BatchItem { key, input, tx }).await?;

        Ok(rx.await?)
    }
}

impl From<RecvError> for BatchError {
    fn from(rx_err: RecvError) -> Self {
        BatchError::Rx(rx_err)
    }
}

impl<T> From<SendError<T>> for BatchError {
    fn from(_tx_err: SendError<T>) -> Self {
        BatchError::Tx
    }
}

#[cfg(test)]
mod tests {}
