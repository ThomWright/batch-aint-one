use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use crate::{batch::Batch, error::RejectionReason};

/// A policy controlling when batches get processed.
#[derive(Debug)]
#[non_exhaustive]
pub enum BatchingPolicy {
    /// Immediately process the batch if possible.
    ///
    /// Will process as many batches concurrently as the limit allows. When concurrency is
    /// maximised, as soon as a batch finishes the next batch will start. When concurrency is
    /// limited to 1, it will run batches serially.
    ///
    /// Prioritises low latency.
    Immediate,

    /// Process the batch when it reaches the maximum size.
    ///
    /// Prioritises high batch utilisation.
    Size,

    /// Process the batch a given duration after it was created.
    ///
    /// Prioritises regularity.
    Duration(Duration, OnFull),
}

/// A policy controlling limits on batch sizes and concurrency.
///
/// New items will be rejected when both the limits have been reached.
#[derive(Debug)]
#[non_exhaustive]
pub struct Limits {
    pub(crate) max_batch_size: usize,
    pub(crate) max_key_concurrency: usize,
}

/// What to do when a batch becomes full.
#[derive(Debug)]
#[non_exhaustive]
pub enum OnFull {
    /// Immediately attempt process the batch. If the maximum concurrency has been reached for the
    /// key, it will reject.
    Process,

    /// Reject any additional items. The batch will be processed when another condition is reached.
    Reject,
}

pub enum PreAdd {
    AddAndProcess,
    AddAndProcessAfter(Duration),
    Reject(RejectionReason),
    Add,
}

pub enum PostFinish {
    Process,
    DoNothing,
}

impl Limits {
    /// Limits the maximum size of a batch.
    pub fn max_batch_size(self, max: usize) -> Self {
        Self {
            max_batch_size: max,
            ..self
        }
    }

    /// Limits the maximum number of batches that can be processed concurrently for a key.
    pub fn max_key_concurrency(self, max: usize) -> Self {
        Self {
            max_key_concurrency: max,
            ..self
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_key_concurrency: 10,
        }
    }
}

impl BatchingPolicy {
    /// Should be applied _before_ adding the new item to the batch.
    pub(crate) fn pre_add<K, I, O, E: Display>(
        &self,
        limits: &Limits,
        batch: &Batch<K, I, O, E>,
    ) -> PreAdd
    where
        K: 'static + Send + Clone,
    {
        if batch.is_full(limits.max_batch_size) {
            if batch.processing() >= limits.max_key_concurrency {
                return PreAdd::Reject(RejectionReason::MaxConcurrency);
            } else {
                return PreAdd::Reject(RejectionReason::BatchFull);
            }
        }

        match self {
            Self::Size if batch.has_single_space(limits.max_batch_size) => {
                if batch.processing() >= limits.max_key_concurrency {
                    PreAdd::Add
                } else {
                    PreAdd::AddAndProcess
                }
            }

            Self::Duration(_dur, on_full) if batch.has_single_space(limits.max_batch_size) => {
                if batch.processing() >= limits.max_key_concurrency {
                    PreAdd::Add
                } else if matches!(on_full, OnFull::Process) {
                    PreAdd::AddAndProcess
                } else {
                    PreAdd::Add
                }
            }

            Self::Duration(dur, _on_full) if batch.is_new_batch() => {
                PreAdd::AddAndProcessAfter(*dur)
            }

            Self::Immediate if batch.processing() < limits.max_key_concurrency => {
                PreAdd::AddAndProcess
            }

            _ => PreAdd::Add,
        }
    }

    pub(crate) fn post_finish<K, I, O, E: Display>(
        &self,
        limits: &Limits,
        next_batch: &Batch<K, I, O, E>,
    ) -> PostFinish {
        if next_batch.processing() < limits.max_key_concurrency {
            match self {
                BatchingPolicy::Immediate => PostFinish::Process,
                _ => {
                    if next_batch.is_full(limits.max_batch_size) {
                        PostFinish::Process
                    } else {
                        PostFinish::DoNothing
                    }
                }
            }
        } else {
            PostFinish::DoNothing
        }
    }
}
