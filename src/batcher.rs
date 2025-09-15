use std::{fmt::Display, future::Future, hash::Hash, sync::Arc};

use tokio::sync::{mpsc, oneshot};
use tracing::{span, Level, Span};

use crate::{
    batch::BatchItem,
    error::BatchResult,
    policies::{BatchingPolicy, Limits},
    worker::{Worker, WorkerHandle},
};

/// Groups items to be processed in batches.
///
/// Takes inputs (`I`) grouped by a key (`K`) and processes multiple together in a batch. An output
/// (`O`) is produced for each input.
///
/// Errors (`E`) can be returned from a batch.
///
/// Cheap to clone.
#[derive(Debug)]
pub struct Batcher<K, I, O = (), E = String>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
{
    worker: Arc<WorkerHandle>,
    item_tx: mpsc::Sender<BatchItem<K, I, O, E>>,
}

/// Process a batch of inputs for a given key.
pub trait Processor<K, I, O = (), E = String>
where
    E: Display,
{
    /// Process the batch.
    ///
    /// The order of the outputs in the returned `Vec` must be the same as the order of the inputs
    /// in the given iterator.
    fn process(
        &self,
        key: K,
        inputs: impl Iterator<Item = I> + Send,
    ) -> impl Future<Output = std::result::Result<Vec<O>, E>> + Send;
}

impl<K, I, O, E> Batcher<K, I, O, E>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
{
    /// Create a new batcher.
    pub fn new<F>(processor: F, limits: Limits, batching_policy: BatchingPolicy) -> Self
    where
        F: 'static + Send + Clone + Processor<K, I, O, E>,
    {
        let (handle, item_tx) = Worker::spawn(processor, limits, batching_policy);

        Self {
            worker: Arc::new(handle),
            item_tx,
        }
    }

    /// Add an item to the batch and await the result.
    pub async fn add(&self, key: K, input: I) -> BatchResult<O, E> {
        // Record the span ID so we can link the shared processing span.
        let requesting_span = Span::current().clone();

        let (tx, rx) = oneshot::channel();
        self.item_tx
            .send(BatchItem {
                key,
                input,
                tx,
                requesting_span,
            })
            .await?;

        let (output, batch_span) = rx.await?;

        {
            let link_back_span = span!(Level::INFO, "batch finished");
            if let Some(span) = batch_span {
                // WARNING: It's very important that we don't drop the span until _after_
                // follows_from().
                //
                // If we did e.g. `.follows_from(span)` then the span would get converted into an ID
                // and dropped. Any attempt to look up the span by ID _inside_ follows_from() would
                // then panic, because the span will have been closed and no longer exist.
                //
                // Don't ask me how long this took me to debug.
                link_back_span.follows_from(&span);
                link_back_span.in_scope(|| {
                    // Do nothing. This span is just here to work around a Honeycomb limitation:
                    //
                    // If the batch span is linked to a parent span like so:
                    //
                    // parent_span_1 <-link- batch_span
                    //
                    // then in Honeycomb, the link is only shown on the batch span. It it not possible
                    // to click through to the batch span from the parent.
                    //
                    // So, here we link back to the batch to make this easier.
                });
            }
        }
        output
    }
}

impl<K, I, O, E> Clone for Batcher<K, I, O, E>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
{
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            item_tx: self.item_tx.clone(),
        }
    }
}
