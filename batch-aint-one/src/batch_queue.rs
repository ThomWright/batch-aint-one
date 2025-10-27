use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::mpsc;

use crate::{
    Limits,
    batch::{Batch, BatchItem},
    batch_inner::Generation,
    processor::Processor,
    worker::Message,
};

/// A double-ended queue for queueing up multiple batches for later processing.
pub(crate) struct BatchQueue<P: Processor> {
    batcher_name: String,

    queue: VecDeque<Batch<P>>,

    // /// Generations we've been asked to process but haven't yet started processing.
    // queued_generations: VecDeque<Generation>,
    limits: Limits,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,

    /// The number of batches with this key that are currently pre-acquiring resources.
    pre_acquiring: Arc<AtomicUsize>,
}

impl<P: Processor> BatchQueue<P> {
    pub(crate) fn new(batcher_name: String, key: P::Key, limits: Limits) -> Self {
        let mut queue = VecDeque::with_capacity(limits.max_batch_queue_size);

        let processing = Arc::<AtomicUsize>::default();
        let pre_acquiring = Arc::<AtomicUsize>::default();
        queue.push_back(Batch::new(
            batcher_name.clone(),
            key,
            processing.clone(),
            pre_acquiring.clone(),
        ));

        Self {
            batcher_name,
            queue,
            limits,
            processing,
            pre_acquiring,
        }
    }

    pub(crate) fn is_next_batch_full(&self) -> bool {
        let next = self.queue.front().expect("Should always be non-empty");
        next.is_full(self.limits.max_batch_size)
    }

    /// Check if the next batch has reached the specified size.
    pub(crate) fn has_next_batch_reached_size(&self, size: usize) -> bool {
        let back = self.queue.front().expect("Should always be non-empty");
        back.len() >= size
    }

    pub(crate) fn is_next_batch_acquiring_resources(&self) -> bool {
        let next = self.queue.front().expect("Should always be non-empty");
        next.has_started_acquiring()
    }

    pub(crate) fn has_next_batch_timeout_expired(&self) -> bool {
        let next = self.queue.front().expect("Should always be non-empty");
        next.has_timeout_expired()
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
        back.is_new_batch() || back.is_full(self.limits.max_batch_size)
    }

    pub(crate) fn within_processing_capacity(&self) -> bool {
        self.processing.load(std::sync::atomic::Ordering::Acquire)
            <= self.limits.max_key_concurrency
    }

    /// Are we currently processing any batches for this key?
    pub(crate) fn is_processing(&self) -> bool {
        self.processing.load(std::sync::atomic::Ordering::Acquire) > 0
    }

    /// Are we currently at maximum total capacity for this key?
    ///
    /// Includes both processing and pre-acquiring batches.
    pub(crate) fn at_max_total_capacity(&self) -> bool {
        self.pre_acquiring
            .load(std::sync::atomic::Ordering::Acquire)
            + self.processing.load(std::sync::atomic::Ordering::Acquire)
            >= self.limits.max_key_concurrency
    }

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

    pub(crate) fn has_batch_ready(&self) -> bool {
        for batch in &self.queue {
            if batch.is_ready() {
                return true;
            }
        }

        false
    }

    pub(crate) fn is_generation_ready(&self, generation: Generation) -> bool {
        for batch in &self.queue {
            if batch.is_generation(generation) {
                return batch.is_ready();
            }
        }

        false
    }

    pub(crate) fn take_next_ready_batch(&mut self) -> Option<Batch<P>> {
        if self.processing.load(Ordering::Acquire) >= self.limits.max_key_concurrency {
            return None;
        }

        for (index, batch) in self.queue.iter().enumerate() {
            if batch.is_ready() {
                let batch = self
                    .queue
                    .remove(index)
                    .expect("Should exist, we just found it");

                if self.queue.is_empty() {
                    self.queue.push_back(batch.new_generation())
                }

                return Some(batch);
            }
        }

        None
    }

    pub(crate) fn take_generation(&mut self, generation: Generation) -> Option<Batch<P>> {
        if self.processing.load(Ordering::Acquire) >= self.limits.max_key_concurrency {
            return None;
        }

        for (index, batch) in self.queue.iter().enumerate() {
            if batch.is_generation(generation) {
                let batch = self
                    .queue
                    .remove(index)
                    .expect("Should exist, we just found it");

                if self.queue.is_empty() {
                    self.queue.push_back(batch.new_generation())
                }

                return Some(batch);
            }
        }

        None
    }

    /// Acquire resources for the last batch, ahead of processing.
    pub(crate) fn pre_acquire_resources(
        &mut self,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let batch = self.queue.back_mut().expect("Should always be non-empty");
        batch.pre_acquire_resources(processor, tx);

        debug_assert!(
            self.pre_acquiring.load(Ordering::Relaxed) <= self.limits.max_key_concurrency,
            "pre-acquiring count should not exceed max key concurrency"
        );
    }

    /// Process the last batch after a delay.
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
            pre_acquiring,
        } = self;
        f.debug_struct("BatchQueue")
            .field("batcher_name", &batcher_name)
            .field("queue", &queue)
            .field(
                "processing",
                &processing.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "pre_acquiring",
                &pre_acquiring.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field("limits", limits)
            .finish()
    }
}
