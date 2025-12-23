use std::{
    fmt::{self, Debug, Display},
    time::Duration,
};

use crate::{
    Limits, Processor,
    batch_inner::Generation,
    batch_queue::BatchQueue,
    error::{ConcurrencyStatus, RejectionReason},
};

mod immediate;
mod size;
mod duration;
mod balanced;

#[cfg(test)]
mod test_utils;

/// A policy controlling when batches get processed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BatchingPolicy {
    /// Immediately process the batch if possible.
    ///
    /// When concurrency and resources are available, new items will be processed immediately (with
    /// a batch size of one).
    ///
    /// When resources are not immediately available, then the batch will remain open while
    /// acquiring resources  to allow more items to be added, up to the maximum batch size.
    ///
    /// In this way, we try to prioritise larger batch sizes, while still keeping latency low.
    ///
    /// When concurrency is maximised, new items will added to the next batch (up to the maximum
    /// batch size). As soon as a batch finishes the next batch will start. When concurrency is
    /// limited to 1, it will run batches serially.
    ///
    /// Prioritises low latency.
    Immediate,

    /// Process the batch when it reaches the maximum size.
    ///
    /// Prioritises high batch utilisation.
    Size,

    /// Process the batch a given duration after it was created.
    ///
    /// If using `OnFull::Process`, then process the batch when either the duration elapses or the
    /// batch becomes full, whichever happens first.
    ///
    /// Prioritises regularity.
    Duration(Duration, OnFull),

    /// Balance between resource efficiency and latency based on system load.
    ///
    /// When no batches are processing, the first item processes immediately.
    ///
    /// When batches are already processing:
    /// - If batch size < `min_size_hint`: Wait for either the batch to reach `min_size_hint` or
    ///   any batch to complete
    /// - If batch size >= `min_size_hint`: Start acquiring resources and process immediately
    ///
    /// The `min_size_hint` must be <= `max_batch_size`.
    ///
    /// Prioritises efficient resource usage while maintaining reasonable latency.
    Balanced {
        /// The minimum batch size to prefer before using additional concurrency.
        min_size_hint: usize,
    },
}

/// What to do when a batch becomes full.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum OnFull {
    /// Immediately attempt process the batch. If the maximum concurrency has been reached for the
    /// key, it will reject.
    Process,

    /// Reject any additional items. The batch will be processed when another condition is reached.
    Reject,
}

/// Action to take when adding an item to a batch.
#[derive(Debug)]
pub(crate) enum OnAdd {
    AddAndProcess,
    AddAndAcquireResources,
    AddAndProcessAfter(Duration),
    Reject(RejectionReason),
    Add,
}

/// Action to take when a specific generation times out or acquires resources.
#[derive(Debug)]
pub(crate) enum ProcessGenerationAction {
    Process,
    DoNothing,
}

/// Action to take when a batch finishes processing.
#[derive(Debug)]
pub(crate) enum ProcessNextAction {
    ProcessNext,
    ProcessNextReady,
    DoNothing,
}

impl Display for BatchingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchingPolicy::Immediate => write!(f, "Immediate"),
            BatchingPolicy::Size => write!(f, "Size"),
            BatchingPolicy::Duration(duration, on_full) => {
                write!(f, "Duration({}ms, {:?})", duration.as_millis(), on_full)
            }
            BatchingPolicy::Balanced { min_size_hint } => {
                write!(f, "Balanced(min_size: {})", min_size_hint)
            }
        }
    }
}

impl BatchingPolicy {
    /// Normalise the policy to ensure it's valid for the given limits.
    pub(crate) fn normalise(self, limits: Limits) -> Self {
        match self {
            BatchingPolicy::Balanced { min_size_hint } => BatchingPolicy::Balanced {
                min_size_hint: min_size_hint.min(limits.max_batch_size),
            },
            other => other,
        }
    }

    /// Should be applied _before_ adding the new item to the batch.
    pub(crate) fn on_add<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> OnAdd {
        if let Some(rejection) = self.should_reject(batch_queue) {
            return OnAdd::Reject(rejection);
        }

        self.on_add_inner(batch_queue)
    }

    /// Check if the item should be rejected due to capacity constraints.
    fn should_reject<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> Option<RejectionReason> {
        if batch_queue.is_full() {
            if batch_queue.at_max_total_processing_capacity() {
                Some(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
            } else {
                Some(RejectionReason::BatchQueueFull(
                    ConcurrencyStatus::Available,
                ))
            }
        } else {
            None
        }
    }

    /// Determine the appropriate action based on policy and batch state.
    fn on_add_inner<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> OnAdd {
        match self {
            Self::Immediate => Self::immediate_on_add_inner(batch_queue),
            Self::Size => self.size_on_add_inner(batch_queue),
            Self::Duration(dur, on_full) => self.duration_on_add_inner(*dur, *on_full, batch_queue),
            Self::Balanced { min_size_hint } => {
                Self::balanced_on_add_inner(*min_size_hint, batch_queue)
            }
        }
    }

    /// Decide between Add and AddAndProcess based on processing capacity.
    fn add_or_process<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> OnAdd {
        if batch_queue.at_max_total_processing_capacity() {
            // We can't process the batch yet, so just add to it.
            OnAdd::Add
        } else {
            OnAdd::AddAndProcess
        }
    }

    pub(crate) fn on_timeout<P: Processor>(
        &self,
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> ProcessGenerationAction {
        if batch_queue.at_max_total_processing_capacity() {
            ProcessGenerationAction::DoNothing
        } else {
            Self::process_generation_if_ready(generation, batch_queue)
        }
    }

    pub(crate) fn on_resources_acquired<P: Processor>(
        &self,
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> ProcessGenerationAction {
        if batch_queue.at_max_total_processing_capacity() {
            ProcessGenerationAction::DoNothing
        } else {
            Self::process_generation_if_ready(generation, batch_queue)
        }
    }

    pub(crate) fn on_finish<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> ProcessNextAction {
        if batch_queue.at_max_total_processing_capacity() {
            return ProcessNextAction::DoNothing;
        }
        match self {
            BatchingPolicy::Immediate => Self::immediate_on_finish(batch_queue),
            BatchingPolicy::Size => Self::size_on_finish(batch_queue),
            BatchingPolicy::Duration(_, _) => Self::duration_on_finish(batch_queue),
            BatchingPolicy::Balanced { .. } => Self::balanced_on_finish(batch_queue),
        }
    }

    // Immediate policy functions
    fn immediate_on_add_inner<P: Processor>(batch_queue: &BatchQueue<P>) -> OnAdd {
        if batch_queue.at_max_total_processing_capacity() {
            OnAdd::Add
        } else if batch_queue.adding_to_new_batch() {
            OnAdd::AddAndAcquireResources
        } else {
            OnAdd::Add
        }
    }

    fn immediate_on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
        Self::process_if_any_ready(batch_queue)
    }

    // Size policy functions
    fn size_on_add_inner<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> OnAdd {
        if batch_queue.last_space_in_batch() {
            self.add_or_process(batch_queue)
        } else {
            OnAdd::Add
        }
    }

    fn size_on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
        if batch_queue.is_next_batch_full() {
            ProcessNextAction::ProcessNext
        } else {
            ProcessNextAction::DoNothing
        }
    }

    // Duration policy functions
    fn duration_on_add_inner<P: Processor>(
        &self,
        duration: Duration,
        on_full: OnFull,
        batch_queue: &BatchQueue<P>,
    ) -> OnAdd {
        if batch_queue.last_space_in_batch() {
            if matches!(on_full, OnFull::Process) {
                self.add_or_process(batch_queue)
            } else {
                OnAdd::Add
            }
        } else if batch_queue.adding_to_new_batch() {
            OnAdd::AddAndProcessAfter(duration)
        } else {
            OnAdd::Add
        }
    }

    fn duration_on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
        if batch_queue.has_next_batch_timeout_expired() {
            ProcessNextAction::ProcessNext
        } else {
            Self::size_on_finish(batch_queue)
        }
    }

    // Balanced policy functions
    fn balanced_on_add_inner<P: Processor>(
        min_size_hint: usize,
        batch_queue: &BatchQueue<P>,
    ) -> OnAdd {
        if batch_queue.at_max_total_processing_capacity() {
            OnAdd::Add
        } else if batch_queue.adding_to_new_batch() && !batch_queue.is_processing() {
            // First item, nothing else processing
            OnAdd::AddAndAcquireResources
        } else if batch_queue.has_last_batch_reached_size(min_size_hint.saturating_sub(1))
            && !batch_queue.is_last_batch_acquiring_resources()
        {
            OnAdd::AddAndAcquireResources
        } else {
            OnAdd::Add
        }
    }

    fn balanced_on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
        Self::process_if_any_ready(batch_queue)
    }

    // Common helper functions
    fn process_generation_if_ready<P: Processor>(
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> ProcessGenerationAction {
        if batch_queue.is_generation_ready(generation) {
            ProcessGenerationAction::Process
        } else {
            ProcessGenerationAction::DoNothing
        }
    }

    fn process_if_any_ready<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
        if batch_queue.has_batch_ready() {
            ProcessNextAction::ProcessNextReady
        } else {
            ProcessNextAction::DoNothing
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use tokio::sync::{Mutex, Notify, mpsc};

    use crate::{batch_queue::BatchQueue, worker::Message};

    use super::*;
    use super::test_utils::*;

    #[test]
    fn limits_builder_methods() {
        let limits = Limits::builder()
            .max_batch_size(50)
            .max_key_concurrency(5)
            .build();

        assert_eq!(limits.max_batch_size, 50);
        assert_eq!(limits.max_key_concurrency, 5);
    }

    mod size {
        use super::*;

        #[test]
        fn waits_for_full_batch_when_empty() {
            let limits = Limits::builder()
                .max_batch_size(3)
                .max_key_concurrency(2)
                .build();
            let queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            let policy = BatchingPolicy::Size;
            let result = policy.on_add(&queue);

            assert_matches!(result, OnAdd::Add);
        }

        #[test]
        fn processes_when_batch_becomes_full() {
            let limits = Limits::builder().max_batch_size(2).build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Add one item to make it nearly full
            queue.push(new_item("key".to_string(), "item1".to_string()));

            let policy = BatchingPolicy::Size;
            let result = policy.on_add(&queue);

            // Should process when adding the last item
            assert_matches!(result, OnAdd::AddAndProcess);
        }

        #[tokio::test]
        async fn rejects_when_full_and_at_capacity() {
            let limits = Limits::builder()
                .max_batch_size(1)
                .max_key_concurrency(1)
                .max_batch_queue_size(1)
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Fill the current batch
            queue.push(new_item("key".to_string(), "item1".to_string()));

            // Start processing to reach max processing capacity
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            queue.process_next_ready_batch(TestProcessor, on_finished);

            // Fill the next batch to reach max queueing capacity
            queue.push(new_item("key".to_string(), "item2".to_string()));

            // Now we're full and at capacity - should reject
            let policy = BatchingPolicy::Size;
            let result = policy.on_add(&queue);

            assert_matches!(
                result,
                OnAdd::Reject(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
            );
        }

        #[tokio::test]
        async fn waits_for_full_batch_after_finish() {
            // Scenario: Size policy waits for full batch even after other batches finish

            let processor = ControlledProcessor::default();
            let limits = Limits::builder()
                .max_batch_size(3)
                .max_key_concurrency(2)
                .build();
            let mut queue = BatchQueue::<ControlledProcessor>::new("test".to_string(), (), limits);
            let policy = BatchingPolicy::Size;

            // Fill and process first batch
            let notify1 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));

            let (on_finished, mut rx) = mpsc::channel(1);
            queue.process_next_ready_batch(processor, on_finished);

            // Add partial second batch
            let notify2 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify2).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify2).notified_owned())); // Only 2 items, not full yet

            // First batch finishes
            notify1.notify_waiters(); // Let first batch complete
            let msg = rx.recv().await.unwrap();
            assert_matches!(msg, Message::Finished(_, _));

            let result = policy.on_finish(&queue);
            assert_matches!(result, ProcessNextAction::DoNothing); // Second batch not full yet

            // Add third item to complete second batch
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::AddAndProcess); // Now should process
        }
    }

    mod immediate {
        use super::*;

        #[test]
        fn acquires_resources_when_empty() {
            let limits = Limits::builder()
                .max_batch_size(3)
                .max_key_concurrency(2)
                .build();
            let queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            let policy = BatchingPolicy::Immediate;
            let result = policy.on_add(&queue);

            assert_matches!(result, OnAdd::AddAndAcquireResources);
        }

        #[tokio::test]
        async fn wont_acquire_more_resources_than_capacity() {
            // Arrange
            // - max_concurrency = 2
            // - 1 batch already processing
            // - 1 batch acquiring resources
            let limits = Limits::builder()
                .max_batch_size(2)
                .max_key_concurrency(2)
                .build();

            let processor = ControlledProcessor::default();

            let mut queue = BatchQueue::<ControlledProcessor>::new("test".to_string(), (), limits);
            let policy = BatchingPolicy::Immediate;

            // Add items to fill first batch
            let notify1 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));

            // Start processing first batch
            let (on_finished, _rx) = mpsc::channel(1);
            queue.process_next_ready_batch(processor.clone(), on_finished);

            // Add items to fill second batch (acquiring resources)
            let notify2 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify2).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify2).notified_owned())); // Only 2 items, not full yet

            let (on_finished, _rx) = mpsc::channel(1);
            queue.process_next_ready_batch(processor, on_finished);

            // Act
            // Now add the first item to the third batch
            let result = policy.on_add(&queue);

            // Assert
            assert_matches!(
                result,
                OnAdd::Add,
                "Should not acquire more resources when at max total concurrency"
            );
        }

        #[tokio::test]
        async fn adds_when_at_max_capacity() {
            let limits = Limits::builder()
                .max_batch_size(1)
                .max_key_concurrency(1)
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            queue.push(new_item("key".to_string(), "item1".to_string()));

            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            queue.process_next_ready_batch(TestProcessor, on_finished);

            let policy = BatchingPolicy::Immediate;

            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::Add);
        }

        #[tokio::test]
        async fn processes_after_finish() {
            // Scenario: Immediate policy processes next batch after one finishes

            let processor = ControlledProcessor::default();
            let limits = Limits::builder()
                .max_batch_size(2)
                .max_key_concurrency(1)
                .build();
            let mut queue = BatchQueue::<ControlledProcessor>::new("test".to_string(), (), limits);
            let policy = BatchingPolicy::Immediate;

            // Add items to fill first batch
            let notify1 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));

            // Start processing first batch
            let (on_finished, mut finished_rx) = mpsc::channel(1);
            queue.process_next_ready_batch(processor, on_finished);

            // Add item to second batch
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::Add); // Can't process yet, at capacity
            let notify2 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify2).notified_owned()));

            // First batch finishes
            notify1.notify_waiters(); // Let first batch complete
            let msg = finished_rx.recv().await.unwrap();
            assert_matches!(msg, Message::Finished(_, _));

            queue.mark_processed();

            let result = policy.on_finish(&queue);
            assert_matches!(result, ProcessNextAction::ProcessNextReady); // Should process second batch
        }

        #[tokio::test]
        async fn out_of_order_acquisition() {
            // Scenario: Resources are acquired out of order

            let mut processor = ControlledProcessor::default();
            let limits = Limits::builder()
                .max_batch_size(2)
                .max_key_concurrency(2)
                .build();
            let mut queue = BatchQueue::<ControlledProcessor>::new("test".to_string(), (), limits);
            let policy = BatchingPolicy::Immediate;

            // Add item - should start acquiring resources
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::AddAndAcquireResources);
            queue.push(new_item((), Arc::new(Notify::new()).notified_owned()));

            let acquire_lock1 = Arc::new(Mutex::new(()));
            let lock_guard1 = acquire_lock1.lock().await; // Hold the lock to simulate long acquire
            processor.acquire_locks.push(Arc::clone(&acquire_lock1));
            let (tx, mut acquired1) = mpsc::channel(1);
            queue.pre_acquire_resources(processor.clone(), tx);

            // Add second item while first is acquiring
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::Add);
            queue.push(new_item((), Arc::new(Notify::new()).notified_owned()));

            // First batch is now full

            // Add third item
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::AddAndAcquireResources); // Should also start acquiring
            queue.push(new_item((), Arc::new(Notify::new()).notified_owned()));

            let acquire_lock2 = Arc::new(Mutex::new(()));
            let lock_guard2 = acquire_lock2.lock().await; // Hold the lock to simulate long acquire
            processor.acquire_locks.push(Arc::clone(&acquire_lock2));
            let (tx, mut acquired2) = mpsc::channel(1);
            queue.pre_acquire_resources(processor.clone(), tx);

            // Simulate resources acquired for second batch first
            drop(lock_guard2); // Release second acquire first

            let msg = acquired2.recv().await.unwrap();
            let second_gen = Generation::default().next();
            assert_matches!(msg, Message::ResourcesAcquired(_, generation) => {
                assert_eq!(generation, second_gen);
            });

            queue.mark_resource_acquisition_finished();

            let result = policy.on_resources_acquired(second_gen, &queue);
            assert_matches!(result, ProcessGenerationAction::Process); // Should process now

            // Now release first acquire
            drop(lock_guard1);

            let msg = acquired1.recv().await.unwrap();
            let first_gen = Generation::default();
            assert_matches!(msg, Message::ResourcesAcquired(_, generation) => {
                assert_eq!(generation, first_gen);
            });

            let result = policy.on_resources_acquired(first_gen, &queue);
            assert_matches!(result, ProcessGenerationAction::Process);
        }
    }

    mod duration {
        use super::*;

        #[test]
        fn schedules_timeout_when_empty() {
            let limits = Limits::builder().max_batch_size(2).build();
            let queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            let duration = Duration::from_millis(100);
            let policy = BatchingPolicy::Duration(duration, OnFull::Process);
            let result = policy.on_add(&queue);

            assert_matches!(result, OnAdd::AddAndProcessAfter(d) if d == duration);
        }

        #[test]
        fn onfull_reject_rejects_when_full_but_not_processing() {
            let limits = Limits::builder()
                .max_batch_size(1)
                .max_key_concurrency(1)
                .max_batch_queue_size(1)
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Fill the batch but don't start processing
            queue.push(new_item("key".to_string(), "item1".to_string()));

            // Full but not at processing capacity yet - should still reject as BatchFull
            let policy = BatchingPolicy::Duration(Duration::from_millis(100), OnFull::Reject);
            let result = policy.on_add(&queue);

            assert_matches!(
                result,
                OnAdd::Reject(RejectionReason::BatchQueueFull(
                    ConcurrencyStatus::Available
                ))
            );
        }

        #[tokio::test]
        async fn timeout_while_processing() {
            // Scenario: Duration policy, max_concurrency=1, batch_size=2
            // 1. Add 3 items (2 in first batch, 1 in second)
            // 2. First batch starts processing
            // 3. Second batch times out while first is still processing
            // 4. After first finishes, second batch should be processed

            let processor = ControlledProcessor::default();
            let limits = Limits::builder()
                .max_batch_size(2)
                .max_key_concurrency(1)
                .build();
            let mut queue = BatchQueue::<ControlledProcessor>::new("test".to_string(), (), limits);
            let policy = BatchingPolicy::Duration(Duration::from_millis(100), OnFull::Process);

            // Step 1: Add first 2 items (should fill first batch and start processing)
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::AddAndProcessAfter(_));
            let notify1 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));

            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::AddAndProcess); // Last space, should process
            queue.push(new_item((), Arc::clone(&notify1).notified_owned()));

            // Start processing first batch
            let (on_finished, mut rx) = mpsc::channel(1);
            queue.process_next_ready_batch(processor, on_finished);

            // Step 2: Add third item (goes to second batch)
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::AddAndProcessAfter(_)); // New batch, set timeout
            let notify2 = Arc::new(Notify::new());
            queue.push(new_item((), notify2.notified_owned()));
            let (tx, mut timeout_rx) = mpsc::channel(1);
            queue.process_after(Duration::from_millis(1), tx);

            // Step 3: Second batch times out while first is still processing
            let msg = timeout_rx.recv().await.unwrap(); // Wait for timeout signal
            let second_gen = Generation::default().next();
            assert_matches!(msg, Message::TimedOut(_, generation)=> {
                assert_eq!(generation, second_gen);
            });
            let result = policy.on_timeout(second_gen, &queue);
            assert_matches!(result, ProcessGenerationAction::DoNothing); // Can't process, at max capacity

            // Step 4: First batch finishes
            notify1.notify_waiters(); // Let first batch complete
            let msg = rx.recv().await.unwrap();
            assert_matches!(msg, Message::Finished(_, _));

            queue.mark_processed();

            let result = policy.on_finish(&queue);
            assert_matches!(result, ProcessNextAction::ProcessNext); // Should process second batch
        }
    }

    mod balanced {
        use super::*;

        #[test]
        fn acquires_resources_when_nothing_processing() {
            let limits = Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build();
            let queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
            let result = policy.on_add(&queue);

            assert_matches!(result, OnAdd::AddAndAcquireResources);
        }

        #[tokio::test]
        async fn waits_when_below_hint_and_processing() {
            let limits = Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Add one item and start processing
            queue.push(new_item("key".to_string(), "item1".to_string()));
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            queue.process_next_ready_batch(TestProcessor, on_finished);

            // Now add another item - should wait (size=1 < hint=5, processing=true)
            let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
            let result = policy.on_add(&queue);

            assert_matches!(result, OnAdd::Add);
        }

        #[tokio::test]
        async fn acquires_when_reached_hint() {
            let limits = Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(2)
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Add one item and start processing
            queue.push(new_item("key".to_string(), "item1".to_string()));
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            queue.process_next_ready_batch(TestProcessor, on_finished);

            // Add 4 more items (total=5, reaching hint)
            for i in 2..=5 {
                queue.push(new_item("key".to_string(), format!("item{}", i)));
            }

            // Next item should trigger acquisition (size=5 >= hint=5)
            let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
            let result = policy.on_add(&queue);

            assert_matches!(result, OnAdd::AddAndAcquireResources);
        }

        #[tokio::test]
        async fn rejects_at_max_capacity() {
            let limits = Limits::builder()
                .max_batch_size(5)
                .max_key_concurrency(1)
                .max_batch_queue_size(1)
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Fill and process first batch
            for i in 1..=5 {
                queue.push(new_item("key".to_string(), format!("item{}", i)));
            }
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            queue.process_next_ready_batch(TestProcessor, on_finished);

            // Fill second batch
            for i in 6..=10 {
                queue.push(new_item("key".to_string(), format!("item{}", i)));
            }

            // At max capacity - should reject
            let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
            let result = policy.on_add(&queue);

            assert_matches!(
                result,
                OnAdd::Reject(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
            );
        }

        #[test]
        fn processes_on_finish_when_batch_ready() {
            let limits = Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Add items to next batch (below hint, waiting)
            for i in 1..=3 {
                queue.push(new_item("key".to_string(), format!("item{}", i)));
            }

            // When a batch finishes, should process the waiting batch
            let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
            let result = policy.on_finish(&queue);

            assert_matches!(result, ProcessNextAction::ProcessNextReady);
        }

        #[test]
        fn does_not_process_on_finish_when_no_batch_ready() {
            let limits = Limits::builder().max_batch_size(10).build();
            let queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Empty queue - nothing to process
            let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
            let result = policy.on_finish(&queue);

            assert_matches!(result, ProcessNextAction::DoNothing);
        }
    }
}
