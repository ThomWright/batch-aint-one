use std::{fmt::Debug, time::Duration};

use crate::batch::Batch;

pub trait LimitStrategy<K, I, O>: Debug + Send + Sync {
    fn limit(&self, batch: &Batch<K, I, O>) -> LimitResult;
}

pub type Limits<K, I, O> = Vec<Box<dyn LimitStrategy<K, I, O>>>;

pub enum LimitResult {
    Process,
    ProcessAfter(Duration),
    DoNothing,
}

#[derive(Debug, Clone)]
pub struct SizeLimit {
    size: usize,
}

#[derive(Debug, Clone)]
pub struct DurationLimit {
    duration: Duration,
}

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

impl<K, I, O> LimitStrategy<K, I, O> for DurationLimit
where
    K: Clone + Send + Sync,
    I: Send + Sync,
    O: Send + Sync,
{
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

impl<K, I, O> LimitStrategy<K, I, O> for DebounceLimit
where
    K: Clone + Send + Sync,
    I: Send + Sync,
    O: Send + Sync,
{
    fn limit(&self, _: &Batch<K, I, O>) -> LimitResult {
        LimitResult::ProcessAfter(self.duration)
    }
}
