use std::fmt::Display;

use thiserror::Error;
use tokio::sync::{mpsc::error::SendError, oneshot::error::RecvError};

/// An error that occurred while trying to batch.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BatchError<E: Display> {
    /// Something went wrong while submitting an input for processing.
    ///
    /// Unrecoverable.
    #[error("Unable to send item to the worker for batching: channel closed")]
    Tx,

    /// Something went wrong while waiting for the output of a batch.
    ///
    /// Unrecoverable.
    #[error("Error while waiting for batch results: channel closed. {}", .0)]
    Rx(RecvError),

    /// The current batch is full so the item was rejected.
    ///
    /// Recoverable.
    #[error("Batch item rejected: {0}")]
    Rejected(RejectionReason),

    /// Something went wrong while processing a batch.
    #[error("The entire batch failed: {}", .0)]
    BatchFailed(E),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum RejectionReason {
    BatchFull,
    MaxConcurrency,
}

impl Display for RejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RejectionReason::BatchFull => "the batch is full",
            RejectionReason::MaxConcurrency => "the key has reached maximum concurrency",
        })
    }
}

pub type BatchResult<T, E> = std::result::Result<T, BatchError<E>>;

impl<E: Display> From<RecvError> for BatchError<E> {
    fn from(rx_err: RecvError) -> Self {
        BatchError::Rx(rx_err)
    }
}

impl<T, E: Display> From<SendError<T>> for BatchError<E> {
    fn from(_tx_err: SendError<T>) -> Self {
        BatchError::Tx
    }
}

impl<E> BatchError<E>
where
    E: Display,
{
    /// Get the inner error for general batch failures, otherwise self.
    pub fn inner(self) -> BatchResult<E, E> {
        match self {
            BatchError::BatchFailed(e) => Ok(e),
            _ => Err(self),
        }
    }
}
