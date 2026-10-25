use std::{fmt::Debug, iter, mem, sync::Arc, time::Duration};

use tokio::sync::{mpsc, oneshot};
use tracing::{Level, Span, info, instrument::WithSubscriber, span};

use crate::{
    BatchError, batch_inner::BatchInner, error::BatchResult, metrics::BatchStats,
    processor::Processor, worker::Message,
};

#[derive(Debug)]
pub(crate) struct BatchItem<P: Processor> {
    pub key: P::Key,
    pub input: P::Input,
    pub submitted_at: tokio::time::Instant,
    pub received_at: Option<tokio::time::Instant>,
    /// Used to send the output back.
    pub tx: SendOutput<P::Output, P::Error>,
    /// This item was added to the batch as part of this span.
    pub requesting_span: Span,
}

type SendOutput<O, E> = oneshot::Sender<(BatchResult<O, E>, Option<Span>)>;

/// A batch of items to process, taken from its key's queue once resources have been acquired for
/// it.
pub(crate) struct Batch<P: Processor> {
    inner: Arc<BatchInner<P>>,
    items: Vec<BatchItem<P>>,
}

impl<P: Processor> Debug for Batch<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Batch { inner, items } = self;
        f.debug_struct("Batch")
            .field("name", &inner.name())
            .field("key", &inner.key())
            .field("items_len", &items.len())
            .finish()
    }
}

impl<P: Processor> Batch<P> {
    pub(crate) fn new(inner: Arc<BatchInner<P>>, items: Vec<BatchItem<P>>) -> Self {
        Self { inner, items }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Process the batch with the given resources.
    ///
    /// Once finished, a message will be sent back to the worker.
    pub(crate) fn process(
        self,
        processor: P,
        resources: P::Resources,
        acquire_span: Span,
        on_finished: mpsc::UnboundedSender<Message<P>>,
    ) {
        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(
            self.process_inner(processor, resources, acquire_span, on_finished)
                .with_current_subscriber(),
        );
    }

    async fn process_inner(
        mut self,
        processor: P,
        resources: P::Resources,
        acquire_span: Span,
        on_finished: mpsc::UnboundedSender<Message<P>>,
    ) {
        let batch_size = self.items.len();
        let name = self.inner.name();
        let key = self.inner.key();

        let delay_since_first_submission = self
            .first_submission()
            .map(|input| input.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        let outer_span = span!(Level::INFO, "process batch",
            batch.name = &name,
            batch.key = ?key,
            // Convert to u64 so tracing will treat this as an integer instead of a string.
            batch.size = batch_size as u64,
            batch.first_item_wait_time_secs = delay_since_first_submission,
        );
        outer_span.follows_from(&acquire_span);

        // Extract the inputs, response channels, and timestamps from the batch items.
        let mut inputs = Vec::with_capacity(batch_size);
        let mut response_channels = Vec::with_capacity(batch_size);
        let mut submitted_ats = Vec::with_capacity(batch_size);
        let mut received_ats = Vec::with_capacity(batch_size);

        for item in mem::take(&mut self.items) {
            // Link the shared batch processing span to the span for each batch item. We
            // don't use a parent relationship because that's 1:many (parent:child), and
            // this is many:1.
            outer_span.follows_from(&item.requesting_span);
            inputs.push(item.input);
            response_channels.push(item.tx);
            submitted_ats.push(item.submitted_at);
            received_ats.push(item.received_at);
        }

        // Process the batch.
        let process_start = tokio::time::Instant::now();

        let outputs = Arc::clone(&self.inner)
            .process(processor, inputs, resources, outer_span.clone())
            .await;

        let processing_duration = process_start.elapsed();
        let success = outputs.iter().all(|o| o.is_ok());

        // Send the outputs back to the requesters.
        Self::send_outputs(response_channels, outputs, Some(outer_span));

        // Compute per-item durations.
        let now = tokio::time::Instant::now();
        let item_latencies = submitted_ats.iter().map(|t| now - *t).collect();
        let queue_durations = received_ats
            .iter()
            .map(|t| t.map_or(Duration::ZERO, |t| process_start - t))
            .collect();

        let metrics = BatchStats::new(
            batch_size,
            processing_duration,
            success,
            item_latencies,
            queue_durations,
        );

        // Signal that we're finished with this batch.
        Self::finalise(key.clone(), metrics, on_finished);
    }

    /// Send a failure result to every item in the batch.
    pub(crate) fn fail(mut self, err: BatchError<P::Error>) {
        let response_channels: Vec<_> = mem::take(&mut self.items)
            .into_iter()
            .map(|item| item.tx)
            .collect();
        let outputs = iter::repeat_n(err, response_channels.len())
            .map(Err)
            .collect();

        Self::send_outputs(response_channels, outputs, None);
    }

    fn send_outputs(
        response_channels: Vec<SendOutput<P::Output, P::Error>>,
        outputs: Vec<Result<P::Output, BatchError<P::Error>>>,
        process_span: Option<Span>,
    ) {
        for (tx, output) in response_channels.into_iter().zip(outputs) {
            if tx.send((output, process_span.clone())).is_err() {
                // Whatever was waiting for the output must have shut down. Presumably it
                // doesn't care anymore, but we log here anyway. There's not much else we can do
                // here.
                info!("Unable to send output over oneshot channel. Receiver deallocated.");
            }
        }
    }

    fn finalise(key: P::Key, metrics: BatchStats, on_finished: mpsc::UnboundedSender<Message<P>>) {
        if on_finished
            .send(Message::Finished { key, metrics })
            .is_err()
        {
            // The worker must have shut down. In this case, we don't want to process any more
            // batches anyway.
            info!("Tried to signal a batch had finished but the worker has shut down");
        }
    }

    fn first_submission(&self) -> Option<tokio::time::Instant> {
        self.items.first().map(|item| item.submitted_at)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{sync::oneshot, time};
    use tracing::Span;

    use super::{Batch, BatchItem};
    use crate::batch_inner::BatchInner;

    #[derive(Clone)]
    struct DummyProcessor;
    impl crate::Processor for DummyProcessor {
        type Key = String;
        type Input = String;
        type Output = String;
        type Error = String;
        type Resources = ();
        async fn acquire_resources(&self, _key: String) -> Result<(), String> {
            Ok(())
        }
        async fn process(
            &self,
            _key: String,
            _inputs: impl Iterator<Item = String> + Send,
            _resources: (),
        ) -> Result<Vec<String>, String> {
            Ok(vec![])
        }
    }

    #[tokio::test(start_paused = true)]
    async fn first_submission() {
        let mut items = Vec::new();

        let (tx, _rx) = oneshot::channel();
        items.push(BatchItem {
            key: "key".to_string(),
            input: "item".to_string(),
            submitted_at: tokio::time::Instant::now(),
            received_at: None,
            tx,
            requesting_span: Span::none(),
        });

        time::advance(Duration::from_millis(50)).await;

        let (tx, _rx) = oneshot::channel();
        items.push(BatchItem {
            key: "key".to_string(),
            input: "item".to_string(),
            submitted_at: tokio::time::Instant::now(),
            received_at: None,
            tx,
            requesting_span: Span::none(),
        });
        let batch: Batch<DummyProcessor> = Batch::new(
            Arc::new(BatchInner::new(
                "test batcher".to_string(),
                "key".to_string(),
            )),
            items,
        );

        assert!(batch.items.len() == 2, "batch should have 2 items");
        assert!(
            batch.first_submission().is_some(),
            "first submission should be set"
        );
        assert_eq!(
            batch.first_submission().unwrap().elapsed().as_millis(),
            50,
            "first submission elapsed time should be 50 ms ago"
        );
    }
}
