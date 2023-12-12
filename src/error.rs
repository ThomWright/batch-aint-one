use thiserror::Error;
use tokio::sync::{mpsc::error::SendError, oneshot::error::RecvError};

/// An error that occurred while trying to batch.
#[derive(Error, Debug)]
pub enum BatchError {
    /// Something went wrong while submitting an input for processing.
    #[error("Unable to send item to the worker for batching: channel closed")]
    Tx,

    /// Something went wrong while waiting for the output of a batch.
    #[error("Error while waiting for batch results: channel closed. {}", .0)]
    Rx(RecvError),

    /// Something went wrong while processing a batch.
    #[error("The entire batch failed with message: {}", .0)]
    BatchFailed(String),
}

pub type Result<T> = std::result::Result<T, BatchError>;

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

impl From<String> for BatchError {
    fn from(s: String) -> Self {
        BatchError::BatchFailed(s)
    }
}
