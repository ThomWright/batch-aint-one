use thiserror::Error;
use tokio::sync::{mpsc::error::SendError, oneshot::error::RecvError};

/// An error that occurred while trying to batch.
#[derive(Error, Debug)]
pub enum BatchError {
    // TODO: better error variants
    /// Something went wrong while waiting for the output of a batch.
    #[error(transparent)]
    Rx(RecvError),

    /// Something went wrong while submitting an input for processing.
    #[error("UIUUGGGHH")]
    Tx,
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
