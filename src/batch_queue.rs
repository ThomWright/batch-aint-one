use std::{
    collections::VecDeque,
    fmt::Display,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use tokio::sync::mpsc;

use crate::{
    batch::{Batch, BatchItem, Generation},
    worker::Message,
    Limits,
};

/// A double-ended queue for queueing up multiple batches for later processing.
pub(crate) struct BatchQueue<K, I, O, E: Display> {
    queue: VecDeque<Batch<K, I, O, E>>,

    limits: Limits,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,
}

impl<K, I, O, E: Display> BatchQueue<K, I, O, E> {
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

    pub(crate) fn at_max_processing_capacity(&self) -> bool {
        self.processing.load(std::sync::atomic::Ordering::Acquire)
            >= self.limits.max_key_concurrency
    }
}

impl<K, I, O, E> BatchQueue<K, I, O, E>
where
    K: 'static + Send + Clone,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
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

    pub(crate) fn take_next_batch(&mut self) -> Option<Batch<K, I, O, E>> {
        let batch = self.queue.front().expect("Should always be non-empty");
        if batch.is_processable() {
            let batch = self.queue.pop_front().expect("Should always be non-empty");
            if self.queue.is_empty() {
                self.queue.push_back(batch.new_generation())
            }
            return Some(batch);
        }

        None
    }

    pub(crate) fn take_generation(&mut self, generation: Generation) -> Option<Batch<K, I, O, E>> {
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
}

impl<K, I, O, E> BatchQueue<K, I, O, E>
where
    K: 'static + Send + Clone,
    E: Display,
{
    pub(crate) fn time_out_after(&mut self, duration: Duration, tx: mpsc::Sender<Message<K>>) {
        let back = self.queue.back_mut().expect("Should always be non-empty");
        back.time_out_after(duration, tx);
    }
}
