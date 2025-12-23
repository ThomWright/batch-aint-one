//! Balanced policy: Balance between resource efficiency and latency based on system load.
//!
//! Prioritises efficient resource usage while maintaining reasonable latency.

use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, ProcessNextAction};

pub(super) fn on_add<P: Processor>(min_size_hint: usize, batch_queue: &BatchQueue<P>) -> OnAdd {
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

pub(super) fn on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> ProcessNextAction {
    if batch_queue.has_batch_ready() {
        ProcessNextAction::ProcessNextReady
    } else {
        ProcessNextAction::DoNothing
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use crate::{
        Limits,
        batch_queue::BatchQueue,
        error::{ConcurrencyStatus, RejectionReason},
        policies::BatchingPolicy,
    };

    use super::super::test_utils::*;
    use super::*;

    #[test]
    fn acquires_resources_when_nothing_processing() {
        let limits = Limits::builder()
            .max_batch_size(10)
            .max_key_concurrency(1)
            .build();
        let queue = BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

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
        let queue = BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

        // Empty queue - nothing to process
        let policy = BatchingPolicy::Balanced { min_size_hint: 5 };
        let result = policy.on_finish(&queue);

        assert_matches!(result, ProcessNextAction::DoNothing);
    }
}
