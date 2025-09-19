use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use crate::{batch_queue::BatchQueue, error::RejectionReason};

/// A policy controlling when batches get processed.
#[derive(Debug)]
#[non_exhaustive]
pub enum BatchingPolicy {
    /// Immediately process the batch if possible.
    ///
    /// When concurrency and resources are available, new items will be processed immediately (with
    /// a batch size of one).
    ///
    /// When resources are not immediately available, then the batch will remain open while
    /// acquiring resources  to allow more items to be added, up to the maximum batch size.
    ///
    /// In this way, we try to prioritise larger batch sizes, while still keeping latency low.
    ///
    /// When concurrency is maximised, new items will added to the next batch (up to the maximum
    /// batch size). As soon as a batch finishes the next batch will start. When concurrency is
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
///
/// `max_key_concurrency * max_batch_size` is both:
///
/// - The number of items that can be processed concurrently.
/// - The number of items that can be queued concurrently.
///
/// So the total number of items in the system can be up to `2 * max_key_concurrency *
/// max_batch_size`.
#[derive(Debug, Clone, Copy)]
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

#[derive(Debug)]
pub(crate) enum PreAdd {
    AddAndProcess,
    AddAndAcquireResources,
    AddAndProcessAfter(Duration),
    Reject(RejectionReason),
    Add,
}

pub(crate) enum PostFinish {
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
    pub(crate) fn pre_add<K, I, O, E: Display, R>(
        &self,
        batch_queue: &BatchQueue<K, I, O, E, R>,
    ) -> PreAdd
    where
        K: 'static + Send + Clone,
    {
        // Check if we have capacity to process this item.
        if batch_queue.is_full() {
            if batch_queue.at_max_processing_capacity() {
                return PreAdd::Reject(RejectionReason::MaxConcurrency);
            } else {
                // We might still be waiting to process the next batch.
                return PreAdd::Reject(RejectionReason::BatchFull);
            }
        }

        match self {
            Self::Size if batch_queue.last_space_in_batch() => {
                if batch_queue.at_max_processing_capacity() {
                    PreAdd::Add
                } else {
                    PreAdd::AddAndProcess
                }
            }

            Self::Duration(_dur, on_full) if batch_queue.last_space_in_batch() => {
                if batch_queue.at_max_processing_capacity() {
                    PreAdd::Add
                } else if matches!(on_full, OnFull::Process) {
                    PreAdd::AddAndProcess
                } else {
                    PreAdd::Add
                }
            }

            Self::Duration(dur, _on_full) if batch_queue.adding_to_new_batch() => {
                PreAdd::AddAndProcessAfter(*dur)
            }

            Self::Immediate if !batch_queue.at_max_processing_capacity() => {
                // We want to process the batch as soon as possible, but we can't process it until
                // we have the resources to do so. So we should acquire the resources first before
                // starting to process.
                //
                // In the meantime, we should continue adding to the current batch.
                PreAdd::AddAndAcquireResources
            }

            _ => PreAdd::Add,
        }
    }

    pub(crate) fn post_finish<K, I, O, E: Display, R>(
        &self,
        batch_queue: &BatchQueue<K, I, O, E, R>,
    ) -> PostFinish {
        if !batch_queue.at_max_processing_capacity() {
            match self {
                BatchingPolicy::Immediate => PostFinish::Process,

                _ => {
                    if batch_queue.is_next_batch_full() {
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
