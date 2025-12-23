//! Batching policies that control when batches get processed.
//!
//! This module provides different batching strategies, each optimised for different use cases:
//!
//! - [`Immediate`](BatchingPolicy::Immediate): Prioritises low latency
//! - [`Size`](BatchingPolicy::Size): Prioritises high batch utilisation
//! - [`Duration`](BatchingPolicy::Duration): Prioritises regularity
//! - [`Balanced`](BatchingPolicy::Balanced): Balances resource efficiency and latency

use std::{
    fmt::{self, Debug, Display},
    time::Duration,
};

use crate::{
    Limits, Processor,
    batch_inner::Generation,
    batch_queue::BatchQueue,
    error::{ConcurrencyStatus, RejectionReason},
};

mod balanced;
mod duration;
mod immediate;
mod size;

#[cfg(test)]
mod test_utils;

/// A policy controlling when batches get processed.
#[derive(Debug, Clone)]
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
    /// If using `OnFull::Process`, then process the batch when either the duration elapses or the
    /// batch becomes full, whichever happens first.
    ///
    /// Prioritises regularity.
    Duration(Duration, OnFull),

    /// Balance between resource efficiency and latency based on system load.
    ///
    /// When no batches are processing, the first item processes immediately.
    ///
    /// When batches are already processing:
    /// - If batch size < `min_size_hint`: Wait for either the batch to reach `min_size_hint` or
    ///   any batch to complete
    /// - If batch size >= `min_size_hint`: Start acquiring resources and process immediately
    ///
    /// The `min_size_hint` must be <= `max_batch_size`.
    ///
    /// Prioritises efficient resource usage while maintaining reasonable latency.
    Balanced {
        /// The minimum batch size to prefer before using additional concurrency.
        min_size_hint: usize,
    },
}

/// What to do when a batch becomes full.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum OnFull {
    /// Immediately attempt process the batch. If the maximum concurrency has been reached for the
    /// key, it will reject.
    Process,

    /// Reject any additional items. The batch will be processed when another condition is reached.
    Reject,
}

/// Action to take when adding an item to a batch.
#[derive(Debug)]
pub(crate) enum OnAdd {
    AddAndProcess,
    AddAndAcquireResources,
    AddAndProcessAfter(Duration),
    Reject(RejectionReason),
    Add,
}

/// Action to take when a specific generation times out or acquires resources.
#[derive(Debug)]
pub(crate) enum OnGenerationEvent {
    Process,
    DoNothing,
}

/// Action to take when a batch finishes processing.
#[derive(Debug)]
pub(crate) enum OnFinish {
    ProcessNext,
    ProcessNextReady,
    DoNothing,
}

impl Display for BatchingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchingPolicy::Immediate => write!(f, "Immediate"),
            BatchingPolicy::Size => write!(f, "Size"),
            BatchingPolicy::Duration(duration, on_full) => {
                write!(f, "Duration({}ms, {:?})", duration.as_millis(), on_full)
            }
            BatchingPolicy::Balanced { min_size_hint } => {
                write!(f, "Balanced(min_size: {})", min_size_hint)
            }
        }
    }
}

impl BatchingPolicy {
    /// Normalise the policy to ensure it's valid for the given limits.
    pub(crate) fn normalise(self, limits: Limits) -> Self {
        match self {
            BatchingPolicy::Balanced { min_size_hint } => BatchingPolicy::Balanced {
                min_size_hint: min_size_hint.min(limits.max_batch_size),
            },
            other => other,
        }
    }

    /// Should be applied _before_ adding the new item to the batch.
    pub(crate) fn on_add<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> OnAdd {
        if let Some(rejection) = self.should_reject(batch_queue) {
            return OnAdd::Reject(rejection);
        }

        match self {
            Self::Immediate => immediate::on_add(batch_queue),
            Self::Size => size::on_add(batch_queue),
            Self::Duration(dur, on_full) => duration::on_add(*dur, *on_full, batch_queue),
            Self::Balanced { min_size_hint } => balanced::on_add(*min_size_hint, batch_queue),
        }
    }

    /// Check if the item should be rejected due to capacity constraints.
    fn should_reject<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> Option<RejectionReason> {
        if batch_queue.is_full() {
            if batch_queue.at_max_total_processing_capacity() {
                Some(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
            } else {
                Some(RejectionReason::BatchQueueFull(
                    ConcurrencyStatus::Available,
                ))
            }
        } else {
            None
        }
    }

    pub(crate) fn on_timeout<P: Processor>(
        &self,
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> OnGenerationEvent {
        if batch_queue.at_max_total_processing_capacity() {
            OnGenerationEvent::DoNothing
        } else {
            Self::process_generation_if_ready(generation, batch_queue)
        }
    }

    pub(crate) fn on_resources_acquired<P: Processor>(
        &self,
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> OnGenerationEvent {
        if batch_queue.at_max_total_processing_capacity() {
            debug_assert!(
                false,
                "on_resources_acquired called when at max processing capacity"
            );
            OnGenerationEvent::DoNothing
        } else {
            Self::process_generation_if_ready(generation, batch_queue)
        }
    }

    pub(crate) fn on_finish<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> OnFinish {
        if batch_queue.at_max_total_processing_capacity() {
            debug_assert!(false, "on_finish called when at max processing capacity");
            return OnFinish::DoNothing;
        }
        match self {
            BatchingPolicy::Immediate => immediate::on_finish(batch_queue),
            BatchingPolicy::Size => size::on_finish(batch_queue),
            BatchingPolicy::Duration(_, _) => duration::on_finish(batch_queue),
            BatchingPolicy::Balanced { .. } => balanced::on_finish(batch_queue),
        }
    }

    fn process_generation_if_ready<P: Processor>(
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> OnGenerationEvent {
        if batch_queue.is_generation_ready(generation) {
            OnGenerationEvent::Process
        } else {
            OnGenerationEvent::DoNothing
        }
    }
}
