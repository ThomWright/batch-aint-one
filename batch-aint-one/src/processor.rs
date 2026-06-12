use std::{
    fmt::{Debug, Display},
    future::Future,
    hash::Hash,
};

/// Process a batch of inputs for a given key.
///
/// Should be cheap to clone.
pub trait Processor: 'static + Send + Clone {
    /// The key used to group inputs.
    type Key: 'static + Debug + Eq + Hash + Clone + Send + Sync;
    /// The input type for each item.
    type Input: Send;
    /// The output type for each item.
    type Output: Send;
    /// The error type that can be returned when processing a batch.
    type Error: Send + Clone + Display + Debug;
    /// The resources that will be acquired before processing each batch, and can be used during
    /// processing.
    type Resources: Send;

    /// Acquire resources to be used for processing the next batch with the given key.
    ///
    /// This method is called before processing each batch.
    ///
    /// For the `Immediate` and `Balanced` batching policies, resources can be acquired before the
    /// batch is processed, and the batch will keep accumulating items until the resources are
    /// acquired.
    ///
    /// Can be used to e.g. acquire a database connection from a pool.
    fn acquire_resources(
        &self,
        key: Self::Key,
    ) -> impl Future<Output = Result<Self::Resources, Self::Error>> + Send;

    /// Process the batch.
    ///
    /// Must return exactly one output per input, in the same order as the inputs in the given
    /// iterator. If the number of outputs is wrong, every item in the batch receives a
    /// `BatchError::ProcessorInvariantViolation` error.
    fn process(
        &self,
        key: Self::Key,
        inputs: impl Iterator<Item = Self::Input> + Send,
        resources: Self::Resources,
    ) -> impl Future<Output = Result<Vec<Self::Output>, Self::Error>> + Send;
}
