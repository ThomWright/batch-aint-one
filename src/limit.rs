use std::{fmt::Debug, time::Duration};

use crate::batch::Batch;

/// Controls when processing starts for a batch.
pub trait LimitStrategy<K, I, O>: Debug + Send + Sync {
    fn limit(&self, batch: &Batch<K, I, O>) -> LimitResult;
}

pub(crate) type Limits<K, I, O> = Vec<Box<dyn LimitStrategy<K, I, O>>>;

/// Whether a batch limit has been hit.
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
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl<K, I, O> LimitStrategy<K, I, O> for DebounceLimit {
    fn limit(&self, _: &Batch<K, I, O>) -> LimitResult {
        LimitResult::ProcessAfter(self.duration)
    }
}
