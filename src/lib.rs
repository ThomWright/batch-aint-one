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
mod builder;
pub mod limit;
mod worker;

pub use batcher::{BatchError, Batcher, Processor};
pub use builder::BatcherBuilder;
