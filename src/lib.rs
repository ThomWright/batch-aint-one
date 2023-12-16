//! Batch up multiple items for processing as a single unit.
//!
//! _I got 99 problems, but a batch ain't one..._
//!
//! Sometimes it is more efficient to process many items at once rather than one at a time.
//! Especially when the processing step has overheads which can be shared between many items.
//!
//! A worker task is run in the background and items are submitted to it for batching. Batches are
//! processed in their own tasks, concurrently.
//!
//! See the README for an example.

#![deny(missing_docs)]

#[cfg(doctest)]
use doc_comment::doctest;
#[cfg(doctest)]
doctest!("../README.md");

mod batch;
mod batcher;
mod batching;
mod error;
mod worker;

pub use batcher::{Batcher, Processor};
pub use batching::{BatchingPolicy, Limits, OnFull};
pub use error::BatchError;
