use std::fmt::Display;

use thiserror::Error;
use tokio::sync::{mpsc::error::SendError, oneshot::error::RecvError};

/// An error that occurred while trying to batch.
#[derive(Error, Debug, Clone)]
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
    #[error("Error while waiting for batch results: channel closed. {0}")]
    Rx(RecvError),

    /// The current batch is full so the item was rejected.
    ///
    /// Recoverable.
    #[error("Batch item rejected: {0}")]
    Rejected(RejectionReason),

    /// Something went wrong while processing a batch.
    #[error("The entire batch failed: {0}")]
    BatchFailed(E),

    /// Something went wrong while acquiring resources for processing.
    #[error("Resource acquisition failed: {0}")]
    ResourceAcquisitionFailed(E),

    /// The batch was cancelled before completion.
    #[error("The batch was cancelled")]
    Cancelled,

    /// The batch processing (or resource acquisition) panicked.
    #[error("The batch processing panicked")]
    Panic,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum RejectionReason {
    /// The batch queue is full.
    BatchQueueFull(ConcurrencyStatus),
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ConcurrencyStatus {
    /// There is available concurrency to process another batch.
    ///
    /// It might be being used because batches are waiting to be processed.
    Available,
    /// The maximum concurrency for this key has been reached.
    MaxedOut,
}

impl Display for RejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RejectionReason::BatchQueueFull(concurrency) => match concurrency {
                ConcurrencyStatus::Available => "the batch queue is full",
                ConcurrencyStatus::MaxedOut => "the batch queue is full and maximum concurrency reached",
            },
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
            BatchError::BatchFailed(source) => Ok(source),
            _ => Err(self),
        }
    }
}
