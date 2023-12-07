use std::{fmt::Debug, time::Duration};

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

impl BatchingStrategy {
    pub(crate) fn is_sequential(&self) -> bool {
        matches!(self, BatchingStrategy::Sequential)
    }
}
