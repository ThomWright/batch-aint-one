use std::{fmt::Display, future::Future};

/// Process a batch of inputs for a given key.
///
/// Should be cheap to clone.
pub trait Processor<K, I, O = (), E = String, R = ()>
where
    E: Display,
{
    /// Acquire resources to be used for processing the next batch with the given key.
    ///
    /// This method is called before processing each batch.
    ///
    /// For `BatchingPolicy::Immediate`, the batch will keep accumulating items until the resources
    /// are acquired.
    ///
    /// Can be used to e.g. acquire a database connection from a pool.
    fn acquire_resources(&self, key: K) -> impl Future<Output = Result<R, E>> + Send;

    /// Process the batch.
    ///
    /// The order of the outputs in the returned `Vec` must be the same as the order of the inputs
    /// in the given iterator.
    fn process(
        &self,
        key: K,
        inputs: impl Iterator<Item = I> + Send,
        resources: R,
    ) -> impl Future<Output = Result<Vec<O>, E>> + Send;
}

// pub trait ResourceAcquirer<K, E = String, R = ()>
// where
//     E: Display,
// {
//     /// Acquire resources to be used for processing a batch.
//     ///
//     /// This method is called before processing each batch. It should return the resources to be
//     /// used for processing the batch.
//     ///
//     /// For `BatchingPolicy::Immediate`, the batch will keep accumulating items until the resources
//     /// are acquired.
//     ///
//     /// Useful for e.g. acquiring a database connection from a pool.
//     fn acquire_resources(&self, key: K) -> impl Future<Output = Result<R, E>> + Send;
// }
