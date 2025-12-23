use std::{collections::VecDeque, fmt::Debug, time::Duration};

use tokio::sync::mpsc;

use crate::{
    BatchError, Limits,
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

    /// The number of batches with this key that are currently pre-acquiring resources.
    pre_acquiring: usize,

    /// The number of batches with this key that are currently processing.
    processing: usize,
}

impl<P: Processor> BatchQueue<P> {
    pub(crate) fn new(batcher_name: String, key: P::Key, limits: Limits) -> Self {
        let mut queue = VecDeque::with_capacity(limits.max_batch_queue_size);

        let processing = 0;
        let pre_acquiring = 0;
        queue.push_back(Batch::new(batcher_name.clone(), key));

        Self {
            batcher_name,
            queue,
            limits,
            pre_acquiring,
            processing,
        }
    }

    /// Is the next batch – the one at the front of the queue – full?
    pub(crate) fn is_next_batch_full(&self) -> bool {
        let next = self.queue.front().expect("Should always be non-empty");
        next.is_full(self.limits.max_batch_size)
    }

    pub(crate) fn has_last_batch_reached_size(&self, size: usize) -> bool {
        let last = self.queue.back().expect("Should always be non-empty");
        last.len() >= size
    }

    pub(crate) fn is_last_batch_acquiring_resources(&self) -> bool {
        let last = self.queue.back().expect("Should always be non-empty");
        last.has_started_acquiring()
    }

    pub(crate) fn has_next_batch_timeout_expired(&self) -> bool {
        let next = self.queue.front().expect("Should always be non-empty");
        next.has_timeout_expired()
    }

    /// Is this batch queue full?
    pub(crate) fn is_full(&self) -> bool {
        let back = self.queue.back().expect("Should always be non-empty");
        self.queue.len() >= self.limits.max_batch_queue_size
            && back.len() >= self.limits.max_batch_size
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

    /// Are we currently processing any batches for this key?
    pub(crate) fn is_processing(&self) -> bool {
        self.processing > 0
    }

    pub(crate) fn mark_processed(&mut self) {
        debug_assert!(
            self.processing > 0,
            "processing count should never go below zero"
        );
        self.processing = self.processing.saturating_sub(1);
    }

    pub(crate) fn mark_resource_acquisition_finished(&mut self) {
        debug_assert!(
            self.pre_acquiring > 0,
            "pre-acquiring count should never go below zero"
        );
        self.pre_acquiring = self.pre_acquiring.saturating_sub(1);
    }

    /// Are we currently at maximum total capacity for this key?
    ///
    /// Includes both processing and pre-acquiring batches.
    pub(crate) fn at_max_total_processing_capacity(&self) -> bool {
        self.pre_acquiring + self.processing >= self.limits.max_key_concurrency
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

    fn take_next_batch(&mut self) -> Option<Batch<P>> {
        let batch = self.queue.pop_front().expect("Should always be non-empty");

        if self.queue.is_empty() {
            self.queue.push_back(batch.new_generation())
        }

        Some(batch)
    }

    pub(crate) fn take_next_ready_batch(&mut self) -> Option<Batch<P>> {
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

    pub(crate) fn process_next_ready_batch(
        &mut self,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        debug_assert!(
            self.processing < self.limits.max_key_concurrency,
            "Attempting to process next ready batch from batch queue '{}' while at max key concurrency",
            self.batcher_name
        );

        let Some(batch) = self.take_next_ready_batch() else {
            debug_assert!(
                false,
                "No ready batch found in batch queue '{}'",
                self.batcher_name
            );
            return;
        };

        self.processing += 1;

        debug_assert!(
            self.processing <= self.limits.max_key_concurrency,
            "Processing count should not exceed max key concurrency"
        );

        batch.process(processor, on_finished);
    }

    pub(crate) fn process_next_batch(
        &mut self,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        debug_assert!(
            self.processing < self.limits.max_key_concurrency,
            "Attempting to process next batch from batch queue '{}' while at max key concurrency",
            self.batcher_name
        );

        let Some(batch) = self.take_next_batch() else {
            debug_assert!(
                false,
                "No next batch found in batch queue '{}'",
                self.batcher_name
            );
            return;
        };

        self.processing += 1;

        debug_assert!(
            self.processing <= self.limits.max_key_concurrency,
            "Processing count should not exceed max key concurrency"
        );

        batch.process(processor, on_finished);
    }

    fn take_generation(&mut self, generation: Generation) -> Option<Batch<P>> {
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

    pub(crate) fn process_generation(
        &mut self,
        generation: Generation,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        debug_assert!(
            self.processing < self.limits.max_key_concurrency,
            "Attempting to process generation {:?} from batch queue '{}' while at max key concurrency",
            generation,
            self.batcher_name
        );

        let Some(batch) = self.take_generation(generation) else {
            debug_assert!(
                false,
                "No batch found for generation {:?} in batch queue '{}'",
                generation, self.batcher_name
            );
            return;
        };

        self.processing += 1;

        batch.process(processor, tx);
    }

    pub(crate) fn fail_generation(
        &mut self,
        generation: Generation,
        error: BatchError<P::Error>,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let Some(batch) = self.take_generation(generation) else {
            debug_assert!(
                false,
                "No batch found for generation {:?} in batch queue '{}'",
                generation, self.batcher_name
            );
            return;
        };

        batch.fail(error, tx);
    }

    /// Acquire resources for the first batch that hasn't yet acquired resources.
    pub(crate) fn pre_acquire_resources(
        &mut self,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        for batch in self.queue.iter_mut() {
            if !batch.has_started_acquiring() {
                self.pre_acquiring += 1;
                batch.pre_acquire_resources(processor, tx);

                debug_assert!(
                    self.pre_acquiring <= self.limits.max_key_concurrency,
                    "pre-acquiring count should not exceed max key concurrency"
                );

                break;
            }
        }
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
            .field("processing", &processing)
            .field("pre_acquiring", &pre_acquiring)
            .field("limits", limits)
            .finish()
    }
}
