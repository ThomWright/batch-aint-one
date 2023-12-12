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

use crate::Processor;

#[derive(Debug)]
pub(crate) struct BatchItem<K, I, O, E> {
    pub key: K,
    pub input: I,
    /// Used to send the output back.
    pub tx: oneshot::Sender<Result<O, E>>,
    /// This item was added to the batch as part of this span.
    pub span_id: Option<span::Id>,
}

/// A batch of items to process.
#[derive(Debug)]
pub(crate) struct Batch<K, I, O, E> {
    key: K,
    generation: u32,
    items: Vec<BatchItem<K, I, O, E>>,

    timeout_deadline: Option<Instant>,
    timeout_handle: Option<JoinHandle<()>>,

    key_currently_processing: Arc<AtomicBool>,
}

pub(crate) type Generation = u32;

pub(crate) struct NextGen<K> {
    key: K,
    generation: Generation,
    key_currently_processing: Arc<AtomicBool>,
}

/// Generations are used to handle the case where a timer goes off after the associated batch has
/// already been processed, and a new batch has already been created with the same key.
///
/// TODO: garbage collection of old generation placeholders.
pub(crate) enum GenerationalBatch<K, I, O, E> {
    Batch(Batch<K, I, O, E>),
    NextGeneration(NextGen<K>),
}

impl<K, I, O, E> Batch<K, I, O, E> {
    pub(crate) fn new(key: K, gen: Option<NextGen<K>>) -> Self {
        Self {
            key,
            generation: gen.as_ref().map(|g| g.generation).unwrap_or_default(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            key_currently_processing: gen
                .map(|g| g.key_currently_processing.clone())
                .unwrap_or_default(),
        }
    }

    /// The size of the batch.
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_new_batch(&self) -> bool {
        self.len() == 1
    }

    pub(crate) fn is_running(&self) -> bool {
        self.key_currently_processing
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn generation(&self) -> Generation {
        self.generation
    }

    pub(crate) fn is_generation(&self, generation: Generation) -> bool {
        self.generation == generation
    }

    pub(crate) fn add_item(&mut self, item: BatchItem<K, I, O, E>) {
        self.items.push(item);
    }

    pub(crate) fn cancel_timeout(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }
}

impl<K, I, O, E> Batch<K, I, O, E>
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
                if tx.send(output).is_err() {
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

impl<K, I, O, E> From<&NextGen<K>> for Batch<K, I, O, E>
where
    K: Clone,
{
    fn from(gen: &NextGen<K>) -> Self {
        Self {
            key: gen.key.clone(),
            generation: gen.generation,
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            key_currently_processing: gen.key_currently_processing.clone(),
        }
    }
}

impl<K, I, O, E> Drop for Batch<K, I, O, E> {
    fn drop(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }
}

impl<K, I, O, E> GenerationalBatch<K, I, O, E>
where
    K: Clone,
{
    pub fn add_item(&mut self, item: BatchItem<K, I, O, E>) -> &mut Batch<K, I, O, E> {
        match self {
            Self::Batch(batch) => {
                batch.add_item(item);

                batch
            }

            Self::NextGeneration(generation) => {
                let mut new_batch = Batch::from(&*generation);
                new_batch.add_item(item);

                *self = Self::Batch(new_batch);

                match self {
                    Self::Batch(batch) => batch,
                    _ => panic!("should be a Batch, we just set it"),
                }
            }
        }
    }

    /// If a batch exists for the given generation, returns it and replaces it with a
    /// `NextGeneration` placeholder.
    pub fn take_batch_for_processing(
        &mut self,
        generation: Generation,
    ) -> Option<Batch<K, I, O, E>> {
        match self {
            Self::Batch(batch) if batch.is_generation(generation) => {
                // TODO: there must be a nicer way?
                let batch = std::mem::replace(batch, Batch::new(batch.key(), None));

                *self = Self::NextGeneration(NextGen {
                    key: batch.key.clone(),
                    generation: batch.generation().wrapping_add(1),
                    key_currently_processing: batch.key_currently_processing.clone(),
                });

                Some(batch)
            }

            _ => None,
        }
    }
}
