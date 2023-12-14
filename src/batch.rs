use std::{
    fmt::Display,
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tracing::{debug, span, Instrument, Level};

use crate::{batcher::Processor, error::Result, BatchError};

#[derive(Debug)]
pub(crate) struct BatchItem<K, I, O, E: Display> {
    pub key: K,
    pub input: I,
    /// Used to send the output back.
    pub tx: oneshot::Sender<Result<O, E>>,
    /// This item was added to the batch as part of this span.
    pub span_id: Option<span::Id>,
}

/// A batch of items to process.
#[derive(Debug)]
pub(crate) struct Batch<K, I, O, E: Display> {
    key: K,
    generation: u32,
    items: Vec<BatchItem<K, I, O, E>>,

    timeout_deadline: Option<Instant>,
    timeout_handle: Option<JoinHandle<()>>,

    key_currently_processing: Arc<AtomicBool>,
}

/// Generations are used to handle the case where a timer goes off after the associated batch has
/// already been processed, and a new batch has already been created with the same key.
///
/// TODO: garbage collection of old generation placeholders.
pub(crate) type Generation = u32;

impl<K, I, O, E: Display> Batch<K, I, O, E> {
    pub(crate) fn new(key: K) -> Self {
        Self {
            key,
            generation: Generation::default(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            key_currently_processing: Arc::<AtomicBool>::default(),
        }
    }

    /// The size of the batch.
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_new_batch(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn is_running(&self) -> bool {
        self.key_currently_processing
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn generation(&self) -> Generation {
        self.generation
    }

    pub(crate) fn is_processable(&self, generation: Generation) -> bool {
        self.generation == generation && self.len() > 0
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
    fn new_generation(&self) -> Self {
        Self {
            key: self.key.clone(),
            generation: self.generation.wrapping_add(1),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            key_currently_processing: self.key_currently_processing.clone(),
        }
    }

    /// If a batch exists for the given generation, returns it and replaces it with an empty
    /// placeholder for the next generation.
    pub fn take_batch_for_processing(
        &mut self,
        generation: Generation,
    ) -> Option<Batch<K, I, O, E>> {
        if self.is_processable(generation) {
            let batch = std::mem::replace(self, self.new_generation());

            Some(batch)
        } else {
            None
        }
    }

    pub(crate) fn process<F>(
        mut self,
        processor: F,
        process_next: Option<mpsc::Sender<(K, Generation)>>,
    ) where
        F: 'static + Send + Clone + Processor<K, I, O, E>,
    {
        self.key_currently_processing.store(true, Ordering::Release);

        self.cancel_timeout();

        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(async move {
            let span = span!(Level::DEBUG, "process batch");

            // Replace with a placeholder to keep the Drop impl working. TODO: is there a better
            // way?!
            let mut items = Vec::new();
            mem::swap(&mut self.items, &mut items);

            let (inputs, txs): (Vec<I>, Vec<oneshot::Sender<Result<O, E>>>) = items
                .into_iter()
                .map(|item| {
                    // Link the shared batch processing span to the span for each batch item. We
                    // don't use a parent relationship because that's 1:many (parent:child), and
                    // this is many:1.
                    span.follows_from(item.span_id);

                    (item.input, item.tx)
                })
                .unzip();

            let size = inputs.len();

            let result = processor
                .process(self.key.clone(), inputs.into_iter())
                .instrument(span)
                .await;

            let outputs: Vec<_> = match result {
                Ok(outputs) => outputs.into_iter().map(|o| Ok(o)).collect(),
                Err(err) => std::iter::repeat(err).take(size).map(|e| Err(e)).collect(),
            };

            for (tx, output) in txs.into_iter().zip(outputs) {
                if tx.send(output.map_err(BatchError::BatchFailed)).is_err() {
                    // Whatever was waiting for the output must have shut down. Presumably it
                    // doesn't care anymore, but we log here anyway. There's not much else we can do
                    // here.
                    debug!("Unable to send output over oneshot channel. Receiver deallocated.");
                }
            }

            self.key_currently_processing
                .store(false, Ordering::Release);

            // We're finished with this batch, maybe we can process the next one
            if let Some(tx) = process_next {
                if tx
                    .send((self.key.clone(), self.generation.wrapping_add(1)))
                    .await
                    .is_err()
                {
                    // The worker must have shut down. In this case, we don't want to process any more
                    // batches anyway.
                    debug!("Tried to signal a batch had finished but the worker has shut down");
                }
            };
        });
    }
}

impl<K, I, O, E> Batch<K, I, O, E>
where
    K: 'static + Send + Clone,
    E: Display,
{
    pub(crate) fn time_out_after(&mut self, duration: Duration, tx: mpsc::Sender<(K, Generation)>) {
        self.cancel_timeout();

        let new_deadline = Instant::now() + duration;
        self.timeout_deadline = Some(new_deadline);

        let key = self.key();
        let generation = self.generation();

        let new_handle = tokio::spawn(async move {
            tokio::time::sleep_until(new_deadline).await;

            if tx.send((key, generation)).await.is_err() {
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
