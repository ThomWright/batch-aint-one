use std::{fmt::Debug, sync::Arc};

use tokio::sync::{mpsc, oneshot};
use tracing::{span, Level, Span};

use crate::{
    batch::BatchItem,
    error::BatchResult,
    policies::{BatchingPolicy, Limits},
    processor::Processor,
    worker::{Worker, WorkerDropGuard, WorkerHandle},
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
pub struct Batcher<P: Processor> {
    worker: Arc<WorkerHandle>,
    worker_guard: Arc<WorkerDropGuard>,
    item_tx: mpsc::Sender<BatchItem<P>>,
}

impl<P: Processor> Batcher<P> {
    /// Create a new batcher.
    pub fn new(processor: P, limits: Limits, batching_policy: BatchingPolicy) -> Self {
        let (handle, worker_guard, item_tx) = Worker::spawn(processor, limits, batching_policy);

        Self {
            worker: Arc::new(handle),
            worker_guard: Arc::new(worker_guard),
            item_tx,
        }
    }

    /// Add an item to the batch and await the result.
    pub async fn add(&self, key: P::Key, input: P::Input) -> BatchResult<P::Output, P::Error> {
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

    /// Get a handle to the worker.
    pub fn worker_handle(&self) -> Arc<WorkerHandle> {
        Arc::clone(&self.worker)
    }
}

impl<P: Processor> Clone for Batcher<P> {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            worker_guard: self.worker_guard.clone(),
            item_tx: self.item_tx.clone(),
        }
    }
}
