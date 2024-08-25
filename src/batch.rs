use std::{
    fmt::Display,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tracing::{debug, span, Instrument, Level, Span};

use crate::{batcher::Processor, error::BatchResult, worker::Message, BatchError};

#[derive(Debug)]
pub(crate) struct BatchItem<K, I, O, E: Display> {
    pub key: K,
    pub input: I,
    /// Used to send the output back.
    pub tx: SendOutput<O, E>,
    /// This item was added to the batch as part of this span.
    pub requesting_span: Span,
}

type SendOutput<O, E> = oneshot::Sender<(BatchResult<O, E>, Option<Span>)>;

/// A batch of items to process.
#[derive(Debug)]
pub(crate) struct Batch<K, I, O, E: Display> {
    key: K,
    generation: Generation,
    items: Vec<BatchItem<K, I, O, E>>,

    timeout_deadline: Option<Instant>,
    timeout_handle: Option<JoinHandle<()>>,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,
}

/// Generations are used to handle the case where a timer goes off after the associated batch has
/// already been processed, and a new batch has already been created with the same key.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub(crate) struct Generation(u32);

impl Generation {
    fn next(&self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl<K, I, O, E: Display> Batch<K, I, O, E> {
    pub(crate) fn new(key: K, processing: Arc<AtomicUsize>) -> Self {
        Self {
            key,
            generation: Generation::default(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            processing,
        }
    }

    /// The size of the batch.
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_new_batch(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn is_full(&self, max: usize) -> bool {
        self.len() >= max
    }

    pub(crate) fn has_single_space(&self, max: usize) -> bool {
        self.len() == max - 1
    }

    pub(crate) fn generation(&self) -> Generation {
        self.generation
    }

    pub(crate) fn is_generation(&self, generation: Generation) -> bool {
        self.generation == generation
    }

    pub(crate) fn is_processable(&self) -> bool {
        // To be processable, we must have some items to process...
        self.len() > 0
            // ... and if there is a timeout deadline, it must be in the past.
            && self
                .timeout_deadline
                .map_or(true, |deadline| deadline.checked_duration_since(Instant::now()).is_none())
    }

    pub(crate) fn push(&mut self, item: BatchItem<K, I, O, E>) {
        self.items.push(item);
    }

    pub(crate) fn cancel_timeout(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }
}

impl<K, I, O, E: Display> Batch<K, I, O, E>
where
    K: Clone,
{
    /// Get the key for this batch.
    pub fn key(&self) -> K {
        self.key.clone()
    }
}

impl<K, I, O, E> Batch<K, I, O, E>
where
    K: 'static + Send + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
{
    pub(crate) fn new_generation(&self) -> Self {
        Self {
            key: self.key.clone(),
            generation: self.generation.next(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            processing: self.processing.clone(),
        }
    }

    pub(crate) fn process<F>(mut self, processor: F, on_finished: mpsc::Sender<Message<K>>)
    where
        F: 'static + Send + Clone + Processor<K, I, O, E>,
    {
        self.processing.fetch_add(1, Ordering::AcqRel);

        self.cancel_timeout();

        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(async move {
            let batch_size = self.items.len();
            // Convert to u64 so tracing will treat this as an integer instead of a string.
            let span = span!(Level::INFO, "process batch", batch_size = batch_size as u64);

            // Replace with a placeholder to keep the Drop impl working. TODO: is there a better
            // way?!
            let mut items = Vec::new();
            mem::swap(&mut self.items, &mut items);

            let (inputs, txs): (Vec<I>, Vec<SendOutput<O, E>>) = items
                .into_iter()
                .map(|item| {
                    // Link the shared batch processing span to the span for each batch item. We
                    // don't use a parent relationship because that's 1:many (parent:child), and
                    // this is many:1.
                    span.follows_from(item.requesting_span.id());

                    (item.input, item.tx)
                })
                .unzip();

            let result = processor
                .process(self.key.clone(), inputs.into_iter())
                .instrument(span.clone())
                .await;

            let outputs: Vec<_> = match result {
                Ok(outputs) => outputs.into_iter().map(|o| Ok(o)).collect(),
                Err(err) => std::iter::repeat(err)
                    .take(batch_size)
                    .map(|e| Err(e))
                    .collect(),
            };

            for (tx, output) in txs.into_iter().zip(outputs) {
                if tx
                    .send((output.map_err(BatchError::BatchFailed), Some(span.clone())))
                    .is_err()
                {
                    // Whatever was waiting for the output must have shut down. Presumably it
                    // doesn't care anymore, but we log here anyway. There's not much else we can do
                    // here.
                    debug!("Unable to send output over oneshot channel. Receiver deallocated.");
                }
            }

            self.processing.fetch_sub(1, Ordering::AcqRel);

            // We're finished with this batch
            if on_finished
                .send(Message::Finished(self.key.clone()))
                .await
                .is_err()
            {
                // The worker must have shut down. In this case, we don't want to process any more
                // batches anyway.
                debug!("Tried to signal a batch had finished but the worker has shut down");
            }
        });
    }
}

impl<K, I, O, E> Batch<K, I, O, E>
where
    K: 'static + Send + Clone,
    E: Display,
{
    pub(crate) fn time_out_after(&mut self, duration: Duration, tx: mpsc::Sender<Message<K>>) {
        self.cancel_timeout();

        let new_deadline = Instant::now() + duration;
        self.timeout_deadline = Some(new_deadline);

        let key = self.key();
        let generation = self.generation();

        let new_handle = tokio::spawn(async move {
            tokio::time::sleep_until(new_deadline).await;

            if tx.send(Message::Process(key, generation)).await.is_err() {
                // The worker must have shut down. In this case, we don't want to process any more
                // batches anyway.
                debug!("A batch reached a timeout but the worker has shut down");
            }
        });

        self.timeout_handle = Some(new_handle);
    }
}

impl<K, I, O, E: Display> Drop for Batch<K, I, O, E> {
    fn drop(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{
        sync::{mpsc, oneshot},
        time,
    };
    use tracing::Span;

    use super::{Batch, BatchItem};

    #[tokio::test]
    async fn is_processable_timeout() {
        time::pause();

        let mut batch: Batch<String, String, String, String> =
            Batch::new("key".to_string(), Arc::default());

        let (tx, _rx) = oneshot::channel();

        batch.push(BatchItem {
            key: "key".to_string(),
            input: "item".to_string(),
            tx,
            requesting_span: Span::none(),
        });

        let (tx, _rx) = mpsc::channel(1);
        batch.time_out_after(Duration::from_millis(50), tx);

        assert!(
            !batch.is_processable(),
            "should not be processable initially"
        );

        time::advance(Duration::from_millis(49)).await;

        assert!(
            !batch.is_processable(),
            "should not be processable after 49ms",
        );

        time::advance(Duration::from_millis(1)).await;

        assert!(
            !batch.is_processable(),
            "should not be processable after 50ms"
        );

        time::advance(Duration::from_millis(1)).await;

        assert!(batch.is_processable(), "should be processable after 51ms");
    }
}
