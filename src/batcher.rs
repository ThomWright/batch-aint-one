use std::{hash::Hash, time::Duration};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, error::SendError},
    oneshot::{self, error::RecvError},
};

use crate::worker::{BatchItem, Worker};

/// Groups items to be processed in batches.
///
/// Takes inputs (`I`) grouped by a key (`K`) and processes multiple together in a batch. An output
/// (`O`) is produced for each input.
#[derive(Debug, Clone)]
pub struct Batcher<K, I, O> {
    tx: mpsc::Sender<BatchItem<K, I, O>>,
}

pub enum BatchLimit {
    Duration(Duration),
    Size(usize),
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
/// The order of the outputs in the returned `Vec` must be the same as the order as the inputs in
/// the given iterator.
#[async_trait]
pub trait BatchFn<I, O> {
    async fn process_batch(&self, inputs: impl Iterator<Item = I>) -> Vec<O>;
}

type Result<T> = std::result::Result<T, BatchError>;

impl<K, I, O> Batcher<K, I, O>
where
    K: Send + Sync + 'static + Eq + Hash + Clone,
    I: Send + Sync + 'static,
    O: Send + 'static,
{
    pub fn new<F>(batch_fn: F, limits: impl Iterator<Item = BatchLimit>) -> Self
    where
        F: BatchFn<I, O> + Send + Sync + 'static,
    {
        let tx = Worker::spawn(batch_fn, limits);

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
