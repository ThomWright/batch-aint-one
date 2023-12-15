use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use crate::batch::Batch;

/// A policy controlling when batches get processed.
#[derive(Debug)]
#[non_exhaustive]
pub enum BatchingPolicy {
    /// Process the batch after the previous batch for the same key has finished.
    ///
    /// Rejects when full.
    Sequential,

    /// Process the batch when it reaches the maximum size.
    ///
    /// Warning: unbounded concurrency. There is no limit on the number of batches for the same key
    /// which can be processed concurrently.
    Size,

    /// Process the batch a given duration after it was created.
    ///
    /// Warning: unbounded concurrency when OnFull::Process is used. There is no limit on the number of batches for the same key
    /// which can be processed concurrently.
    Duration(Duration, OnFull),

    /// Process the batch a given duration after the most recent item was added.
    ///
    /// Warning: unbounded concurrency when OnFull::Process is used. There is no limit on the number
    /// of batches for the same key which can be processed concurrently.
    Debounce(Duration, OnFull),
}

/// What to do when a batch becomes full (reaches `max_size`).
#[derive(Debug)]
#[non_exhaustive]
pub enum OnFull {
    /// Immediately process the batch.
    Process,

    /// Reject any additional items. The batch will be processed
    /// when another condition is reached.
    Reject,
}

pub enum PolicyResult {
    AddAndProcess,
    AddAndProcessAfter(Duration),
    Reject,
    Add,
}

impl BatchingPolicy {
    pub(crate) fn is_sequential(&self) -> bool {
        matches!(self, Self::Sequential)
    }

    /// Should be applied _before_ adding the new item to the batch.
    pub(crate) fn apply<K, I, O, E: Display>(
        &self,
        max_size: usize,
        batch: &Batch<K, I, O, E>,
    ) -> PolicyResult
    where
        K: 'static + Send + Clone,
    {
        match self {
            Self::Size if batch.len() >= max_size - 1 => PolicyResult::AddAndProcess,

            Self::Duration(_dur, on_full) if batch.len() >= max_size - 1 => match on_full {
                OnFull::Process => PolicyResult::AddAndProcess,
                OnFull::Reject => {
                    if batch.len() >= max_size {
                        PolicyResult::Reject
                    } else {
                        PolicyResult::Add
                    }
                }
            },
            Self::Duration(dur, _on_full) if batch.is_new_batch() => {
                PolicyResult::AddAndProcessAfter(*dur)
            }

            Self::Debounce(_dur, on_full) if batch.len() >= max_size - 1 => match on_full {
                OnFull::Process => PolicyResult::AddAndProcess,
                OnFull::Reject => {
                    if batch.len() >= max_size {
                        PolicyResult::Reject
                    } else {
                        PolicyResult::Add
                    }
                }
            },
            Self::Debounce(dur, _on_full) => PolicyResult::AddAndProcessAfter(*dur),

            Self::Sequential if batch.len() >= max_size => PolicyResult::Reject,
            Self::Sequential if !batch.is_running() => PolicyResult::AddAndProcess,

            _ => PolicyResult::Add,
        }
    }
}
