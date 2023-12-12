use std::fmt::Display;

use thiserror::Error;
use tokio::sync::{mpsc::error::SendError, oneshot::error::RecvError};

/// An error that occurred while trying to batch.
#[derive(Error, Debug)]
pub enum BatchError<E: Display> {
    /// Something went wrong while submitting an input for processing.
    #[error("Unable to send item to the worker for batching: channel closed")]
    Tx,

    /// Something went wrong while waiting for the output of a batch.
    #[error("Error while waiting for batch results: channel closed. {}", .0)]
    Rx(RecvError),

    /// Something went wrong while processing a batch.
    #[error("The entire batch failed with message: {}", .0)]
    BatchFailed(E),
}

pub type Result<T, E> = std::result::Result<T, BatchError<E>>;

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

// impl<E> From<E> for BatchError<E> {
//     fn from(s: E) -> Self {
//         BatchError::BatchFailed(s)
//     }
// }
