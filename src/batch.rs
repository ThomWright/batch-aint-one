use std::time::Duration;

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

#[derive(Debug)]
pub(crate) struct BatchItem<K, I, O> {
    pub key: K,
    pub input: I,
    pub tx: oneshot::Sender<O>,
}

#[derive(Debug)]
pub struct Batch<K, I, O> {
    key: K,
    generation: u32,
    items: Vec<BatchItem<K, I, O>>,

    timeout_deadline: Option<Instant>,
    timeout_handle: Option<JoinHandle<()>>,
}

pub(crate) type Generation = u32;

/// Generations are used to handle the case where a timer goes off after the associated batch has
/// already been processed, and a new batch has already been created with the same key.
///
/// TODO: garbage collection of old generation placeholders.
pub(crate) enum GenerationalBatch<K, I, O> {
    Batch(Batch<K, I, O>),
    NextGeneration(Generation),
}

impl<K, I, O> Batch<K, I, O> {
    pub(crate) fn new(key: K, generation: Generation) -> Self {
        Self {
            key,
            generation,
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn generation(&self) -> Generation {
        self.generation
    }

    pub(crate) fn is_generation(&self, generation: Generation) -> bool {
        self.generation == generation
    }

    pub fn key_ref(&self) -> &K {
        &self.key
    }

    pub(crate) fn add_item(&mut self, item: BatchItem<K, I, O>) {
        self.items.push(item);
    }

    pub(crate) fn cancel_timeout(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }

    pub(crate) fn start_processing(mut self) -> (Vec<I>, Vec<oneshot::Sender<O>>) {
        self.cancel_timeout();

        self.items
            .into_iter()
            .map(|item| (item.input, item.tx))
            .unzip()
    }
}

impl<K, I, O> Batch<K, I, O>
where
    K: Clone,
{
    pub fn key(&self) -> K {
        self.key.clone()
    }
}

impl<K, I, O> Batch<K, I, O>
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

            // FIXME: handle error
            tx.send((key, generation))
                .await
                .unwrap_or_else(|_| panic!("TODO: fix"));
        });

        self.timeout_handle = Some(new_handle);
    }
}

impl<K, I, O> GenerationalBatch<K, I, O>
where
    K: Clone,
{
    pub fn add_item(&mut self, item: BatchItem<K, I, O>) -> &mut Batch<K, I, O> {
        match self {
            Self::Batch(batch) => {
                batch.add_item(item);

                batch
            }

            Self::NextGeneration(generation) => {
                let mut new_batch = Batch::new(item.key.clone(), *generation);
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
    pub fn take_batch(&mut self, generation: Generation) -> Option<Batch<K, I, O>> {
        match self {
            Self::Batch(batch) if batch.is_generation(generation) => {
                // TODO: there must be a nicer way?
                let batch = std::mem::replace(batch, Batch::new(batch.key(), 0));
                *self = Self::NextGeneration(batch.generation().wrapping_add(1));

                Some(batch)
            }

            _ => None,
        }
    }
}
