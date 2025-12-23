//! Duration policy: Process batches after a given duration or when full.
//!
//! Prioritises regularity.

use std::time::Duration;

use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, OnFull, ProcessNextAction};

pub(super) fn on_add<P: Processor>(
    duration: Duration,
    on_full: OnFull,
    batch_queue: &BatchQueue<P>,
    add_or_process: impl FnOnce(&BatchQueue<P>) -> OnAdd,
) -> OnAdd {
    if batch_queue.last_space_in_batch() {
        if matches!(on_full, OnFull::Process) {
            add_or_process(batch_queue)
        } else {
            OnAdd::Add
        }
    } else if batch_queue.adding_to_new_batch() {
        OnAdd::AddAndProcessAfter(duration)
    } else {
        OnAdd::Add
    }
}

pub(super) fn on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
    if batch_queue.has_next_batch_timeout_expired() {
        ProcessNextAction::ProcessNext
    } else if batch_queue.is_next_batch_full() {
        ProcessNextAction::ProcessNext
    } else {
        ProcessNextAction::DoNothing
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use tokio::sync::{Notify, mpsc};

    use crate::{
        Limits,
        batch_inner::Generation,
        batch_queue::BatchQueue,
        error::{ConcurrencyStatus, RejectionReason},
        policies::BatchingPolicy,
        worker::Message,
    };

    use super::super::test_utils::*;
    use super::*;

    #[test]
    fn schedules_timeout_when_empty() {
        let limits = Limits::builder().max_batch_size(2).build();
        let queue = BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

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
        assert_matches!(result, super::super::ProcessGenerationAction::DoNothing); // Can't process, at max capacity

        // Step 4: First batch finishes
        notify1.notify_waiters(); // Let first batch complete
        let msg = rx.recv().await.unwrap();
        assert_matches!(msg, Message::Finished(_, _));

        queue.mark_processed();

        let result = policy.on_finish(&queue);
        assert_matches!(result, ProcessNextAction::ProcessNext); // Should process second batch
    }
}
