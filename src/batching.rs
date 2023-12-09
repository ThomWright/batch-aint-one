use std::{fmt::Debug, time::Duration};

use crate::batch::Batch;

/// When to process a batch.
#[derive(Debug)]
#[non_exhaustive]
pub enum BatchingStrategy {
    /// Process the batch when it reaches a given size.
    Size(usize),
    /// Process the batch a given duration after it was created.
    Duration(Duration),
    /// Process the batch a given duration after the most recent item was added.
    Debounce(Duration),
    /// Process the batch after the previous batch for the same key has finished.
    Sequential,
    // TODO: Duration/Debounce+Size
}

pub enum BatchingResult {
    Process,
    ProcessAfter(Duration),
    DoNothing,
}

impl BatchingStrategy {
    pub(crate) fn is_sequential(&self) -> bool {
        matches!(self, Self::Sequential)
    }

    pub(crate) fn apply<K, I, O>(&self, batch: &Batch<K, I, O>) -> BatchingResult
    where
        K: 'static + Send + Clone,
    {
        match self {
            Self::Size(size) if batch.len() >= *size => BatchingResult::Process,

            Self::Duration(dur) if batch.is_new_batch() => BatchingResult::ProcessAfter(*dur),

            Self::Debounce(dur) => BatchingResult::ProcessAfter(*dur),

            Self::Sequential if !batch.is_running() => BatchingResult::Process,
            _ => BatchingResult::DoNothing,
        }
    }
}
