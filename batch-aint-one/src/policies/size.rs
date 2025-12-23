//! Size policy: Process batches when they reach the maximum size.
//!
//! Prioritises high batch utilisation.

use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, OnFinish};

pub(super) fn on_add<P: Processor>(
    batch_queue: &BatchQueue<P>,
    add_or_process: impl FnOnce(&BatchQueue<P>) -> OnAdd,
) -> OnAdd {
    if batch_queue.last_space_in_batch() {
        add_or_process(batch_queue)
    } else {
        OnAdd::Add
    }
}

pub(super) fn on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> OnFinish {
    if batch_queue.is_next_batch_full() {
        OnFinish::ProcessNext
    } else {
        OnFinish::DoNothing
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use tokio::sync::{Notify, mpsc};

    use crate::{
        Limits,
        batch_queue::BatchQueue,
        error::{ConcurrencyStatus, RejectionReason},
        policies::BatchingPolicy,
        worker::Message,
    };

    use super::super::test_utils::*;
    use super::*;

    #[test]
    fn waits_for_full_batch_when_empty() {
        let limits = Limits::builder()
            .max_batch_size(3)
            .max_key_concurrency(2)
            .build();
        let queue = BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

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
        assert_matches!(result, OnFinish::DoNothing); // Second batch not full yet

        // Add third item to complete second batch
        let result = policy.on_add(&queue);
        assert_matches!(result, OnAdd::AddAndProcess); // Now should process
    }
}
