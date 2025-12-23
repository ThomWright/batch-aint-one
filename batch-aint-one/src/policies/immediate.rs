//! Immediate policy: Process batches as soon as resources are available.
//!
//! Prioritises low latency by processing items immediately when possible.

use crate::{Processor, batch_queue::BatchQueue};

use super::{OnAdd, OnFinish};

pub(super) fn on_add<P: Processor>(batch_queue: &BatchQueue<P>) -> OnAdd {
    if batch_queue.at_max_total_processing_capacity() {
        OnAdd::Add
    } else if batch_queue.adding_to_new_batch() {
        OnAdd::AddAndAcquireResources
    } else {
        OnAdd::Add
    }
}

pub(super) fn on_finish<P: Processor>(batch_queue: &BatchQueue<P>) -> OnFinish {
    if batch_queue.has_batch_ready() {
        OnFinish::ProcessNextReady
    } else {
        OnFinish::DoNothing
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use tokio::sync::{Mutex, Notify, mpsc};

    use crate::{
        Limits, batch_inner::Generation, batch_queue::BatchQueue, policies::BatchingPolicy,
        worker::Message,
    };

    use super::super::test_utils::*;
    use super::*;

    #[test]
    fn acquires_resources_when_empty() {
        let limits = Limits::builder()
            .max_batch_size(3)
            .max_key_concurrency(2)
            .build();
        let queue = BatchQueue::<TestProcessor>::new("test".to_string(), "key".to_string(), limits);

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
        queue.push(new_item((), Arc::clone(&notify2).notified_owned()));

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
        assert_matches!(result, OnFinish::ProcessNextReady); // Should process second batch
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
        assert_matches!(result, super::super::OnGenerationEvent::Process); // Should process now

        // Now release first acquire
        drop(lock_guard1);

        let msg = acquired1.recv().await.unwrap();
        let first_gen = Generation::default();
        assert_matches!(msg, Message::ResourcesAcquired(_, generation) => {
            assert_eq!(generation, first_gen);
        });

        let result = policy.on_resources_acquired(first_gen, &queue);
        assert_matches!(result, super::super::OnGenerationEvent::Process);
    }
}
