use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use tokio::sync::mpsc;

use crate::{
    Limits,
    batch::{Batch, BatchItem, Generation},
    processor::Processor,
    worker::Message,
};

/// A double-ended queue for queueing up multiple batches for later processing.
pub(crate) struct BatchQueue<P: Processor> {
    batcher_name: String,

    queue: VecDeque<Batch<P>>,

    limits: Limits,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,
}

impl<P: Processor> BatchQueue<P> {
    pub(crate) fn new(batcher_name: String, key: P::Key, limits: Limits) -> Self {
        // The queue size is the same as the max processing capacity.
        let mut queue = VecDeque::with_capacity(limits.max_key_concurrency);

        let processing = Arc::<AtomicUsize>::default();
        queue.push_back(Batch::new(batcher_name.clone(), key, processing.clone()));

        Self {
            batcher_name,
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

    pub(crate) fn is_empty(&self) -> bool {
        // We always have at least one (possibly empty) batch in the queue.
        self.queue.len() == 1
            && self
                .queue
                .front()
                .expect("Should always be non-empty")
                .is_empty()
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

    /// Are we currently processing any batches for this key?
    pub(crate) fn is_processing(&self) -> bool {
        self.processing.load(std::sync::atomic::Ordering::Acquire) > 0
    }
}

impl<P: Processor> BatchQueue<P> {
    pub(crate) fn push(&mut self, item: BatchItem<P>) {
        let back = self.queue.back_mut().expect("Should always be non-empty");

        if back.is_full(self.limits.max_batch_size) {
            let mut new_back = back.new_generation();
            new_back.push(item);
            self.queue.push_back(new_back);
        } else {
            back.push(item);
        }
    }

    pub(crate) fn take_next_batch(&mut self) -> Option<Batch<P>> {
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

    pub(crate) fn take_generation(&mut self, generation: Generation) -> Option<Batch<P>> {
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
    pub(crate) fn pre_acquire_resources(
        &mut self,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let batch = self.queue.front_mut().expect("Should always be non-empty");
        batch.pre_acquire_resources(processor, tx);
    }

    pub(crate) fn process_after(
        &mut self,
        duration: Duration,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let back = self.queue.back_mut().expect("Should always be non-empty");
        back.process_after(duration, tx);
    }
}

impl<P: Processor> Debug for BatchQueue<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            batcher_name,
            queue,
            limits,
            processing,
        } = self;
        f.debug_struct("BatchQueue")
            .field("batcher_name", &batcher_name)
            .field("queue_len", &queue.len())
            .field(
                "processing",
                &processing.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field("limits", limits)
            .finish()
    }
}
