//! Batch up multiple items for processing as a single unit.
//!
//! _I got 99 problems, but a batch ain't one..._
//!
//! Sometimes it is more efficient to process many items at once rather than one at a time.
//! Especially when the processing step has overheads which can be shared between many items.
//!
//! Often applications work with one item at a time, e.g. _select one row_ or _insert one row_. Many
//! of these operations can be batched up into more efficient versions: _select many rows_ and
//! _insert many rows_.
//!
//! A worker task is run in the background. Many client tasks (e.g. message handlers) can submit
//! items to the worker and wait for them to be processed. The worker task batches together many
//! items and processes them as one unit, before sending a result back to each calling task.
//!
//! See the README for an example.

#![deny(missing_docs)]

#[cfg(doctest)]
use doc_comment::doctest;
#[cfg(doctest)]
doctest!("../README.md");

/// Check an invariant which should never be violated: panic in debug builds (and tests), but
/// only log a warning in release builds rather than bringing the process down.
macro_rules! soft_assert {
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            tracing::warn!($($arg)+);
            debug_assert!(false, $($arg)+);
        }
    };
}

mod batch;
mod batch_inner;
mod batch_queue;
mod batcher;
pub mod error;
mod limits;
pub mod metrics;
mod policies;
mod processor;
mod timeout;
mod worker;

pub use batcher::Batcher;
pub use error::BatchError;
pub use limits::Limits;
pub use metrics::{BatchStats, MetricsRecorder, MetricsRecorderFactory};
pub use policies::{BatchingPolicy, OnFull};
pub use processor::Processor;
pub use worker::WorkerHandle;
