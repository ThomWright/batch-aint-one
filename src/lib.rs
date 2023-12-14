/*!
Batch up multiple items for processing as a single unit.
*/

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
pub use batching::{BatchingPolicy, OnFull};
pub use error::BatchError;
