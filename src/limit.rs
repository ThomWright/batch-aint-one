/*!
 * Limits to control when batches start processing.
 */

use std::{fmt::Debug, time::Duration};

use crate::batch::Batch;

/// Controls when processing starts for a batch.
pub trait LimitStrategy<K, I, O>: Debug + Send + Sync {
    /// Has the batch reached some limit which means we should start processing it?
    fn limit(&self, batch: &Batch<K, I, O>) -> LimitResult;
}

pub(crate) type Limits<K, I, O> = Vec<Box<dyn LimitStrategy<K, I, O>>>;

/// Whether a batch limit has been hit.
#[derive(Debug)]
pub enum LimitResult {
    /// Process now.
    Process,
    /// Process after a delay. Overrides any previously set delay (as long as it hasn't already
    /// passed).
    ProcessAfter(Duration),
    /// Continue waiting.
    DoNothing,
}

/// Process a batch after it reaches a given size.
#[derive(Debug, Clone)]
pub struct SizeLimit {
    size: usize,
}

/// Process a batch a given duration after it was first created.
#[derive(Debug, Clone)]
pub struct DurationLimit {
    duration: Duration,
}

/// Process a batch a given duration after the most recent item was added.
#[derive(Debug, Clone)]
pub struct DebounceLimit {
    duration: Duration,
}

impl SizeLimit {
    /// Create a limit on the size of a batch.
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}

impl<K, I, O> LimitStrategy<K, I, O> for SizeLimit {
    fn limit(&self, batch: &Batch<K, I, O>) -> LimitResult {
        if batch.len() >= self.size {
            LimitResult::Process
        } else {
            LimitResult::DoNothing
        }
    }
}

impl DurationLimit {
    /// Create a limit on how long we wait for new items in a batch.
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl<K, I, O> LimitStrategy<K, I, O> for DurationLimit {
    fn limit(&self, batch: &Batch<K, I, O>) -> LimitResult {
        if batch.len() == 1 {
            LimitResult::ProcessAfter(self.duration)
        } else {
            LimitResult::DoNothing
        }
    }
}

impl DebounceLimit {
    /// Create a limit on how long we wait in between new items being added to a batch.
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl<K, I, O> LimitStrategy<K, I, O> for DebounceLimit {
    fn limit(&self, _: &Batch<K, I, O>) -> LimitResult {
        LimitResult::ProcessAfter(self.duration)
    }
}
