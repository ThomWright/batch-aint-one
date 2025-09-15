use std::{
    collections::VecDeque,
    fmt::Display,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use tokio::sync::mpsc;

use crate::{
    batch::{Batch, BatchItem, Generation},
    processor::Processor,
    worker::Message,
    Limits,
};

/// A double-ended queue for queueing up multiple batches for later processing.
pub(crate) struct BatchQueue<K, I, O, E: Display, R = ()> {
    queue: VecDeque<Batch<K, I, O, E, R>>,

    limits: Limits,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,
}

impl<K, I, O, E: Display, R> BatchQueue<K, I, O, E, R> {
    pub(crate) fn new(key: K, limits: Limits) -> Self {
        // The queue size is the same as the max processing capacity.
        let mut queue = VecDeque::with_capacity(limits.max_key_concurrency);

        let processing = Arc::<AtomicUsize>::default();
        queue.push_back(Batch::new(key, processing.clone()));

        Self {
            queue,
            limits,
            processing,
        }
    }

    pub(crate) fn is_next_batch_full(&self) -> bool {
        let next = self.queue.front().expect("Should always be non-empty");
        next.is_full(self.limits.max_batch_size)
    }

    pub(crate) fn is_full(&self) -> bool {
        let back = self.queue.back().expect("Should always be non-empty");
        self.queue.len() == self.limits.max_key_concurrency
            && back.len() == self.limits.max_batch_size
    }

    pub(crate) fn last_space_in_batch(&self) -> bool {
        let back = self.queue.back().expect("Should always be non-empty");
        back.has_single_space(self.limits.max_batch_size)
    }

    pub(crate) fn adding_to_new_batch(&self) -> bool {
        let back = self.queue.back().expect("Should always be non-empty");
        back.is_new_batch()
    }

    /// Are we currently processing the maximum number of batches for this key (according to
    /// the `Limits`)?
    pub(crate) fn at_max_processing_capacity(&self) -> bool {
        self.processing.load(std::sync::atomic::Ordering::Acquire)
            >= self.limits.max_key_concurrency
    }
}

impl<K, I, O, E, R> BatchQueue<K, I, O, E, R>
where
    K: 'static + Send + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
    R: 'static + Send,
{
    pub(crate) fn push(&mut self, item: BatchItem<K, I, O, E>) {
        let back = self.queue.back_mut().expect("Should always be non-empty");

        if back.is_full(self.limits.max_batch_size) {
            let mut new_back = back.new_generation();
            new_back.push(item);
            self.queue.push_back(new_back);
        } else {
            back.push(item);
        }
    }

    fn next_batch_mut(&mut self) -> &mut Batch<K, I, O, E, R> {
        self.queue.front_mut().expect("Should always be non-empty")
    }

    pub(crate) fn take_next_batch(&mut self) -> Option<Batch<K, I, O, E, R>> {
        let batch = self.next_batch_mut();
        if batch.is_processable() {
            let batch = self.queue.pop_front().expect("Should always be non-empty");
            if self.queue.is_empty() {
                self.queue.push_back(batch.new_generation())
            }
            return Some(batch);
        }

        None
    }

    pub(crate) fn take_generation(
        &mut self,
        generation: Generation,
    ) -> Option<Batch<K, I, O, E, R>> {
        for (index, batch) in self.queue.iter().enumerate() {
            if batch.is_generation(generation) {
                if batch.is_processable() {
                    let batch = self
                        .queue
                        .remove(index)
                        .expect("Should exist, we just found it");

                    if self.queue.is_empty() {
                        self.queue.push_back(batch.new_generation())
                    }

                    return Some(batch);
                } else {
                    return None;
                }
            }
        }

        None
    }

    /// Acquire resources ahead of processing for the next batch.
    pub(crate) fn pre_acquire_resources<F>(&mut self, processor: F, tx: mpsc::Sender<Message<K>>)
    where
        F: 'static + Send + Processor<K, I, O, E, R>,
    {
        let batch = self.next_batch_mut();
        batch.pre_acquire_resources(processor, tx);
    }
}

impl<K, I, O, E, R> BatchQueue<K, I, O, E, R>
where
    K: 'static + Send + Clone,
    E: Display,
{
    pub(crate) fn process_after(&mut self, duration: Duration, tx: mpsc::Sender<Message<K>>) {
        let back = self.queue.back_mut().expect("Should always be non-empty");
        back.process_after(duration, tx);
    }
}
