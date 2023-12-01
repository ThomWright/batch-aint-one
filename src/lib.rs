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
mod error;
pub mod limit;
mod worker;

pub use batch::Batch;
pub use batcher::{Batcher, Processor};
pub use builder::BatcherBuilder;
pub use error::BatchError;
