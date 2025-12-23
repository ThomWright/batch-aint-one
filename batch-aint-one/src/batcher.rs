use std::{fmt::Debug, sync::Arc};

use bon::bon;
use tokio::sync::{mpsc, oneshot};
use tracing::{Level, Span, span};

use crate::{
    batch::BatchItem,
    error::BatchResult,
    policies::{BatchingPolicy},
    limits::Limits,
    processor::Processor,
    worker::{Worker, WorkerDropGuard, WorkerHandle},
};

/// Groups items to be processed in batches.
///
/// Takes inputs one at a time and sends them to a background worker task which groups them into
/// batches according to the specified [`BatchingPolicy`] and [`Limits`], and processes them using
/// the provided [`Processor`].
///
/// Cheap to clone. Cloned instances share the same background worker task.
///
/// ## Drop
///
/// When the last instance of a `Batcher` is dropped, the worker task will be aborted (ungracefully
/// shut down).
///
/// If you want to shut down the worker gracefully, call [`WorkerHandle::shut_down()`].
#[derive(Debug)]
pub struct Batcher<P: Processor> {
    name: String,
    worker: Arc<WorkerHandle>,
    worker_guard: Arc<WorkerDropGuard>,
    item_tx: mpsc::Sender<BatchItem<P>>,
}

#[bon]
impl<P: Processor> Batcher<P> {
    /// Create a new batcher.
    ///
    /// # Notes
    ///
    /// If `batching_policy` is `Balanced { min_size_hint }` where `min_size_hint` is greater than
    /// `limits.max_batch_size`, the `min_size_hint` will be clamped to `max_batch_size`.
    #[builder]
    pub fn new(
        name: impl Into<String>,
        processor: P,
        limits: Limits,
        batching_policy: BatchingPolicy,
    ) -> Self {
        let name = name.into();

        let batching_policy = batching_policy.normalise(limits);

        let (handle, worker_guard, item_tx) =
            Worker::spawn(name.clone(), processor, limits, batching_policy);

        Self {
            name,
            worker: Arc::new(handle),
            worker_guard: Arc::new(worker_guard),
            item_tx,
        }
    }

    /// Add an item to be batched and processed, and await the result.
    pub async fn add(&self, key: P::Key, input: P::Input) -> BatchResult<P::Output, P::Error> {
        // Record the span ID so we can link the shared processing span.
        let requesting_span = Span::current().clone();

        let (tx, rx) = oneshot::channel();
        self.item_tx
            .send(BatchItem {
                key,
                input,
                submitted_at: tokio::time::Instant::now(),
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

    /// Get a handle to the worker.
    pub fn worker_handle(&self) -> Arc<WorkerHandle> {
        Arc::clone(&self.worker)
    }
}

impl<P: Processor> Clone for Batcher<P> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            worker: self.worker.clone(),
            worker_guard: self.worker_guard.clone(),
            item_tx: self.item_tx.clone(),
        }
    }
}
