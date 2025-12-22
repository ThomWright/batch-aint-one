use std::{
    fmt::{self, Debug, Display},
    time::Duration,
};

use bon::bon;

use crate::{
    Processor,
    batch_inner::Generation,
    batch_queue::BatchQueue,
    error::{ConcurrencyStatus, RejectionReason},
};

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

/// A policy controlling limits on batch sizes and concurrency.
///
/// New items will be rejected when both the limits have been reached.
///
/// `max_key_concurrency * max_batch_size` is both:
///
/// - The number of items that can be processed concurrently.
/// - By default, the number of items that can be queued.
///
/// So when using the default `max_batch_queue_size` the total number of items in the system for a
/// given key can be up to `2 * max_key_concurrency
/// * max_batch_size`.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Limits {
    pub(crate) max_batch_size: usize,
    pub(crate) max_key_concurrency: usize,
    pub(crate) max_batch_queue_size: usize,
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

#[derive(Debug)]
pub(crate) enum OnAdd {
    AddAndProcess,
    AddAndAcquireResources,
    AddAndProcessAfter(Duration),
    Reject(RejectionReason),
    Add,
}

#[derive(Debug)]
pub(crate) enum ProcessAction {
    Process,
    DoNothing,
}

#[bon]
impl Limits {
    #[allow(missing_docs)]
    #[builder]
    pub fn new(
        /// Limits the maximum size of a batch.
        #[builder(default = 100)]
        max_batch_size: usize,
        /// Limits the maximum number of batches that can be processed concurrently for a key.
        #[builder(default = 10)]
        max_key_concurrency: usize,
        /// Limits the maximum number of batches that can be queued for processing.
        max_batch_queue_size: Option<usize>,
    ) -> Self {
        Self {
            max_batch_size,
            max_key_concurrency,
            max_batch_queue_size: max_batch_queue_size
                .unwrap_or(max_key_concurrency * max_batch_size),
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        let max_batch_size = 100;
        let max_key_concurrency = 10;
        let max_batch_queue_size = max_key_concurrency;
        Self {
            max_batch_size,
            max_key_concurrency,
            max_batch_queue_size,
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
            if batch_queue.at_max_total_capacity() {
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
            Self::Size if batch_queue.last_space_in_batch() => self.add_or_process(batch_queue),

            Self::Duration(_dur, on_full) if batch_queue.last_space_in_batch() => {
                if matches!(on_full, OnFull::Process) {
                    self.add_or_process(batch_queue)
                } else {
                    OnAdd::Add
                }
            }

            Self::Duration(dur, _on_full) if batch_queue.adding_to_new_batch() => {
                OnAdd::AddAndProcessAfter(*dur)
            }

            Self::Immediate => {
                if batch_queue.at_max_total_capacity() {
                    OnAdd::Add
                } else if batch_queue.adding_to_new_batch() {
                    OnAdd::AddAndAcquireResources
                } else {
                    OnAdd::Add
                }
            }

            Self::Balanced { min_size_hint } => {
                if batch_queue.at_max_total_capacity() {
                    OnAdd::Add
                } else if batch_queue.adding_to_new_batch() && !batch_queue.is_processing() {
                    // First item, nothing else processing
                    OnAdd::AddAndAcquireResources
                } else if batch_queue.has_next_batch_reached_size(min_size_hint.saturating_sub(1))
                    && !batch_queue.is_next_batch_acquiring_resources()
                {
                    OnAdd::AddAndAcquireResources
                } else {
                    OnAdd::Add
                }
            }

            BatchingPolicy::Size | BatchingPolicy::Duration(_, _) => OnAdd::Add,
        }
    }

    /// Decide between Add and AddAndProcess based on processing capacity.
    fn add_or_process<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> OnAdd {
        if batch_queue.at_max_total_capacity() {
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
    ) -> ProcessAction {
        if batch_queue.at_max_total_capacity() {
            ProcessAction::DoNothing
        } else {
            Self::process_generation_if_ready(generation, batch_queue)
        }
    }

    pub(crate) fn on_resources_acquired<P: Processor>(
        &self,
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> ProcessAction {
        if batch_queue.at_max_total_capacity() {
            ProcessAction::DoNothing
        } else {
            Self::process_generation_if_ready(generation, batch_queue)
        }
    }

    pub(crate) fn on_finish<P: Processor>(&self, batch_queue: &BatchQueue<P>) -> ProcessAction {
        if batch_queue.at_max_total_capacity() {
            return ProcessAction::DoNothing;
        }
        match self {
            BatchingPolicy::Immediate => Self::process_if_any_ready(batch_queue),

            BatchingPolicy::Balanced { .. } => Self::process_if_any_ready(batch_queue),

            BatchingPolicy::Duration(_, _) if batch_queue.has_next_batch_timeout_expired() => {
                ProcessAction::Process
            }

            BatchingPolicy::Duration(_, _) | BatchingPolicy::Size => {
                if batch_queue.is_next_batch_full() {
                    ProcessAction::Process
                } else {
                    ProcessAction::DoNothing
                }
            }
        }
    }

    fn process_generation_if_ready<P: Processor>(
        generation: Generation,
        batch_queue: &BatchQueue<P>,
    ) -> ProcessAction {
        if batch_queue.is_generation_ready(generation) {
            ProcessAction::Process
        } else {
            ProcessAction::DoNothing
        }
    }

    fn process_if_any_ready<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessAction {
        if batch_queue.has_batch_ready() {
            ProcessAction::Process
        } else {
            ProcessAction::DoNothing
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicUsize};

    use assert_matches::assert_matches;
    use tokio::sync::{Mutex, Notify, futures::OwnedNotified, mpsc};
    use tracing::Span;

    use crate::{Processor, batch::BatchItem, batch_queue::BatchQueue, worker::Message};

    use super::*;

    #[derive(Clone)]
    struct TestProcessor;

    impl Processor for TestProcessor {
        type Key = String;
        type Input = String;
        type Output = String;
        type Error = String;
        type Resources = ();

        async fn acquire_resources(&self, _key: String) -> Result<(), String> {
            Ok(())
        }

        async fn process(
            &self,
            _key: String,
            inputs: impl Iterator<Item = String> + Send,
            _resources: (),
        ) -> Result<Vec<String>, String> {
            Ok(inputs.collect())
        }
    }

    #[derive(Default, Clone)]
    struct ControlledProcessor {
        // We control when acquire_resources completes by holding locks.
        acquire_locks: Vec<Arc<Mutex<()>>>,
        acquire_counter: Arc<AtomicUsize>,
    }

    impl Processor for ControlledProcessor {
        type Key = ();
        type Input = OwnedNotified;
        type Output = ();
        type Error = String;
        type Resources = ();

        async fn acquire_resources(&self, _key: ()) -> Result<(), String> {
            let n = self
                .acquire_counter
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            if let Some(lock) = self.acquire_locks.get(n) {
                let _guard = lock.lock().await;
            }
            Ok(())
        }

        async fn process(
            &self,
            _key: (),
            inputs: impl Iterator<Item = OwnedNotified> + Send,
            _resources: (),
        ) -> Result<Vec<()>, String> {
            let mut outputs = vec![];
            for item in inputs {
                item.await;
                outputs.push(());
            }
            Ok(outputs)
        }
    }

    fn new_item<P: Processor>(key: P::Key, input: P::Input) -> BatchItem<P> {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        BatchItem {
            key,
            input,
            submitted_at: tokio::time::Instant::now(),
            tx,
            requesting_span: Span::none(),
        }
    }

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
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Fill the current batch
            queue.push(new_item("key".to_string(), "item1".to_string()));

            // Start processing to reach max processing capacity
            let batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            batch.process(TestProcessor, on_finished);

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

            let first_batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, mut rx) = mpsc::channel(1);
            first_batch.process(processor, on_finished);

            // Add partial second batch
            let notify2 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify2).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify2).notified_owned())); // Only 2 items, not full yet

            // First batch finishes
            notify1.notify_waiters(); // Let first batch complete
            let msg = rx.recv().await.unwrap();
            assert_matches!(msg, Message::Finished(_));

            let result = policy.on_finish(&queue);
            assert_matches!(result, ProcessAction::DoNothing); // Second batch not full yet

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
            let first_batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, _rx) = mpsc::channel(1);
            first_batch.process(processor.clone(), on_finished);

            // Add items to fill second batch (acquiring resources)
            let notify2 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify2).notified_owned()));
            queue.push(new_item((), Arc::clone(&notify2).notified_owned())); // Only 2 items, not full yet

            let mut second_batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, _rx) = mpsc::channel(1);
            second_batch.pre_acquire_resources(processor, on_finished);

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

            let batch = queue.take_next_ready_batch().unwrap();

            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            batch.process(TestProcessor, on_finished);

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
            let first_batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, mut rx) = mpsc::channel(1);
            first_batch.process(processor, on_finished);

            // Add item to second batch
            let result = policy.on_add(&queue);
            assert_matches!(result, OnAdd::Add); // Can't process yet, at capacity
            let notify2 = Arc::new(Notify::new());
            queue.push(new_item((), Arc::clone(&notify2).notified_owned()));

            // First batch finishes
            notify1.notify_waiters(); // Let first batch complete
            let msg = rx.recv().await.unwrap();
            assert_matches!(msg, Message::Finished(_));

            let result = policy.on_finish(&queue);
            assert_matches!(result, ProcessAction::Process); // Should process second batch
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

            let result = policy.on_resources_acquired(second_gen, &queue);
            assert_matches!(result, ProcessAction::Process); // Should process now

            // Now release first acquire
            drop(lock_guard1);

            let msg = acquired1.recv().await.unwrap();
            let first_gen = Generation::default();
            assert_matches!(msg, Message::ResourcesAcquired(_, generation) => {
                assert_eq!(generation, first_gen);
            });

            let result = policy.on_resources_acquired(first_gen, &queue);
            assert_matches!(result, ProcessAction::Process);
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
            let first_batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, mut rx) = mpsc::channel(1);
            first_batch.process(processor, on_finished);

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
            assert_matches!(result, ProcessAction::DoNothing); // Can't process, at max capacity

            // Step 4: First batch finishes
            notify1.notify_waiters(); // Let first batch complete
            let msg = rx.recv().await.unwrap();
            assert_matches!(msg, Message::Finished(_));

            let result = policy.on_finish(&queue);
            assert_matches!(result, ProcessAction::Process); // Should process second batch
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
            let batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            batch.process(TestProcessor, on_finished);

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
            let batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            batch.process(TestProcessor, on_finished);

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
                .build();
            let mut queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Fill and process first batch
            for i in 1..=5 {
                queue.push(new_item("key".to_string(), format!("item{}", i)));
            }
            let batch = queue.take_next_ready_batch().unwrap();
            let (on_finished, _rx) = tokio::sync::mpsc::channel(1);
            batch.process(TestProcessor, on_finished);

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

            assert_matches!(result, ProcessAction::Process);
        }

        #[test]
        fn does_not_process_on_finish_when_no_batch_ready() {
            let limits = Limits::builder().max_batch_size(10).build();
            let queue =
                BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

            // Empty queue - nothing to process
            let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
            let result = policy.on_finish(&queue);

            assert_matches!(result, ProcessAction::DoNothing);
        }
    }
}
