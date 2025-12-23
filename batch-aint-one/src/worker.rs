use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, info};

use crate::{
    BatchError,
    batch::BatchItem,
    batch_inner::Generation,
    batch_queue::BatchQueue,
    policies::{BatchingPolicy, OnAdd, OnGenerationEvent, OnFinish},
    limits::Limits,
    processor::Processor,
};

pub(crate) struct Worker<P: Processor> {
    batcher_name: String,

    /// Used to receive new batch items.
    item_rx: mpsc::Receiver<BatchItem<P>>,
    /// The callback to process a batch of inputs.
    processor: P,

    /// Used to signal that a batch for key `K` should be processed.
    msg_tx: mpsc::Sender<Message<P::Key, P::Error>>,
    /// Receives signals to process a batch for key `K`.
    msg_rx: mpsc::Receiver<Message<P::Key, P::Error>>,

    /// Used to send messages to the worker related to shutdown.
    shutdown_notifier_rx: mpsc::Receiver<ShutdownMessage>,

    /// Used to signal to listeners that the worker has shut down.
    shutdown_notifiers: Vec<oneshot::Sender<()>>,

    shutting_down: bool,

    limits: Limits,
    /// Controls when to start processing a batch.
    batching_policy: BatchingPolicy,

    /// Unprocessed batches, grouped by key `K`.
    batch_queues: HashMap<P::Key, BatchQueue<P>>,
}

#[derive(Debug)]
pub(crate) enum Message<K, E: Display + Debug> {
    TimedOut(K, Generation),
    ResourcesAcquired(K, Generation),
    ResourceAcquisitionFailed(K, Generation, BatchError<E>),
    Finished(K, BatchTerminalState),
}

#[derive(Debug)]
pub(crate) enum BatchTerminalState {
    Processed,
    FailedAcquiring,
}

pub(crate) enum ShutdownMessage {
    Register(ShutdownNotifier),
    ShutDown,
}

pub(crate) struct ShutdownNotifier(oneshot::Sender<()>);

/// A handle to the worker task.
///
/// Used for shutting down the worker and waiting for it to finish.
#[derive(Debug, Clone)]
pub struct WorkerHandle {
    shutdown_tx: mpsc::Sender<ShutdownMessage>,
}

/// Aborts the worker task when dropped.
#[derive(Debug)]
pub(crate) struct WorkerDropGuard {
    handle: JoinHandle<()>,
}

impl<P: Processor> Worker<P> {
    pub fn spawn(
        batcher_name: String,
        processor: P,
        limits: Limits,
        batching_policy: BatchingPolicy,
    ) -> (WorkerHandle, WorkerDropGuard, mpsc::Sender<BatchItem<P>>) {
        // These channel sizes are somewhat arbitrary - they just need to be big enough to avoid
        // backpressure in normal operation.
        let (item_tx, item_rx) = mpsc::channel(limits.max_items_in_system_per_key());
        let (msg_tx, msg_rx) = mpsc::channel(limits.max_items_in_system_per_key());

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let mut worker = Worker {
            batcher_name,

            item_rx,
            processor,

            msg_tx,
            msg_rx,

            shutdown_notifier_rx: shutdown_rx,
            shutdown_notifiers: Vec::new(),

            shutting_down: false,

            limits,
            batching_policy,

            batch_queues: HashMap::new(),
        };

        let handle = tokio::spawn(async move {
            worker.run().await;
        });

        (
            WorkerHandle { shutdown_tx },
            WorkerDropGuard { handle },
            item_tx,
        )
    }

    /// Add an item to the batch.
    fn add(&mut self, item: BatchItem<P>) {
        let key = item.key.clone();

        let batch_queue = self.batch_queues.entry(key.clone()).or_insert_with(|| {
            BatchQueue::new(self.batcher_name.clone(), key.clone(), self.limits)
        });

        match self.batching_policy.on_add(batch_queue) {
            OnAdd::AddAndProcess => {
                batch_queue.push(item);

                self.process_next_batch(&key);
            }
            OnAdd::AddAndAcquireResources => {
                batch_queue.push(item);

                batch_queue.pre_acquire_resources(self.processor.clone(), self.msg_tx.clone());
            }
            OnAdd::AddAndProcessAfter(duration) => {
                batch_queue.push(item);

                batch_queue.process_after(duration, self.msg_tx.clone());
            }
            OnAdd::Add => {
                batch_queue.push(item);
            }
            OnAdd::Reject(reason) => {
                if item
                    .tx
                    .send((Err(BatchError::Rejected(reason)), None))
                    .is_err()
                {
                    // Whatever was waiting for the output must have shut down. Presumably it
                    // doesn't care anymore, but we log here anyway. There's not much else we can do.
                    debug!(
                        "Unable to send output over oneshot channel. Receiver deallocated. Batcher: {}",
                        self.batcher_name
                    );
                }
            }
        }
    }

    fn process_generation(&mut self, key: P::Key, generation: Generation) {
        let batch_queue = self.batch_queues.get_mut(&key).expect("batch should exist");

        batch_queue.process_generation(generation, self.processor.clone(), self.msg_tx.clone());
    }

    fn process_next_ready_batch(&mut self, key: &P::Key) {
        let batch_queue = self
            .batch_queues
            .get_mut(key)
            .expect("batch queue should exist");

        batch_queue.process_next_ready_batch(self.processor.clone(), self.msg_tx.clone());
    }

    fn process_next_batch(&mut self, key: &P::Key) {
        let batch_queue = self
            .batch_queues
            .get_mut(key)
            .expect("batch queue should exist");

        batch_queue.process_next_batch(self.processor.clone(), self.msg_tx.clone());
    }

    fn on_timeout(&mut self, key: P::Key, generation: Generation) {
        let batch_queue = self
            .batch_queues
            .get_mut(&key)
            .expect("batch queue should exist");

        match self.batching_policy.on_timeout(generation, batch_queue) {
            OnGenerationEvent::Process => {
                self.process_generation(key, generation);
            }
            OnGenerationEvent::DoNothing => {}
        }
    }

    fn on_resource_acquired(&mut self, key: P::Key, generation: Generation) {
        let batch_queue = self
            .batch_queues
            .get_mut(&key)
            .expect("batch queue should exist");

        batch_queue.mark_resource_acquisition_finished();

        match self
            .batching_policy
            .on_resources_acquired(generation, batch_queue)
        {
            OnGenerationEvent::Process => {
                self.process_generation(key, generation);
            }
            OnGenerationEvent::DoNothing => {}
        }
    }

    fn on_batch_finished(&mut self, key: &P::Key, terminal_state: BatchTerminalState) {
        let batch_queue = self
            .batch_queues
            .get_mut(key)
            .expect("batch queue should exist");

        match terminal_state {
            BatchTerminalState::Processed => {
                batch_queue.mark_processed();
            }
            BatchTerminalState::FailedAcquiring => {
                batch_queue.mark_resource_acquisition_finished();
            }
        }

        match self.batching_policy.on_finish(batch_queue) {
            OnFinish::ProcessNextReady => {
                self.process_next_ready_batch(key);
            }
            OnFinish::ProcessNext => {
                self.process_next_batch(key);
            }
            OnFinish::DoNothing => {}
        }
    }

    fn on_resource_acquisition_failed(
        &mut self,
        key: P::Key,
        generation: Generation,
        err: BatchError<P::Error>,
    ) {
        let batch_queue = self
            .batch_queues
            .get_mut(&key)
            .expect("batch queue should exist");

        batch_queue.fail_generation(generation, err.clone(), self.msg_tx.clone());
    }

    fn ready_to_shut_down(&self) -> bool {
        self.shutting_down
            && self.batch_queues.values().all(|q| q.is_empty())
            && !self.batch_queues.values().any(|q| q.is_processing())
    }

    /// Start running the worker event loop.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.shutdown_notifier_rx.recv() => {
                    match msg {
                        ShutdownMessage::Register(notifier) => {
                           self.shutdown_notifiers.push(notifier.0);
                        }
                        ShutdownMessage::ShutDown => {
                            self.shutting_down = true;
                        }
                    }
                }

                Some(item) = self.item_rx.recv() => {
                    self.add(item);
                }

                Some(msg) = self.msg_rx.recv() => {
                    match msg {
                        Message::ResourcesAcquired(key, generation) => {
                            self.on_resource_acquired(key, generation);
                        }
                        Message::ResourceAcquisitionFailed(key, generation, err) => {
                            self.on_resource_acquisition_failed(key, generation, err);
                        }
                        Message::TimedOut(key, generation) => {
                            self.on_timeout(key, generation);
                        }
                        Message::Finished(key, terminal_state) => {
                            self.on_batch_finished(&key, terminal_state);
                        }
                    }
                }
            }

            if self.ready_to_shut_down() {
                info!("Batch worker '{}' is shutting down", &self.batcher_name);
                return;
            }
        }
    }
}

impl WorkerHandle {
    /// Signal the worker to shut down after processing any in-flight batches.
    ///
    /// Note that when using the Size policy this may wait indefinitely if no new items are added.
    pub async fn shut_down(&self) {
        // We ignore errors here - if the receiver has gone away, the worker is already shut down.
        let _ = self.shutdown_tx.send(ShutdownMessage::ShutDown).await;
    }

    /// Wait for the worker to finish.
    pub async fn wait_for_shutdown(&self) {
        // We ignore errors here - if the receiver has gone away, the worker is already shut down.
        let (notifier_tx, notifier_rx) = oneshot::channel();
        let _ = self
            .shutdown_tx
            .send(ShutdownMessage::Register(ShutdownNotifier(notifier_tx)))
            .await;
        // Wait for the notifier to be dropped.
        let _ = notifier_rx.await;
    }
}

impl Drop for WorkerDropGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod test {
    use tokio::sync::oneshot;
    use tracing::Span;

    use super::*;

    #[derive(Debug, Clone)]
    struct SimpleBatchProcessor;

    impl Processor for SimpleBatchProcessor {
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
            Ok(inputs.map(|s| s + " processed").collect())
        }
    }

    #[tokio::test]
    async fn simple_test_over_channel() {
        let (_worker_handle, _worker_guard, item_tx) = Worker::<SimpleBatchProcessor>::spawn(
            "test".to_string(),
            SimpleBatchProcessor,
            Limits::builder().max_batch_size(2).build(),
            BatchingPolicy::Size,
        );

        let rx1 = {
            let (tx, rx) = oneshot::channel();
            item_tx
                .send(BatchItem {
                    key: "K1".to_string(),
                    input: "I1".to_string(),
                    submitted_at: tokio::time::Instant::now(),
                    tx,
                    requesting_span: Span::none(),
                })
                .await
                .unwrap();

            rx
        };

        let rx2 = {
            let (tx, rx) = oneshot::channel();
            item_tx
                .send(BatchItem {
                    key: "K1".to_string(),
                    input: "I2".to_string(),
                    submitted_at: tokio::time::Instant::now(),
                    tx,
                    requesting_span: Span::none(),
                })
                .await
                .unwrap();

            rx
        };

        let o1 = rx1.await.unwrap().0.unwrap();
        let o2 = rx2.await.unwrap().0.unwrap();

        assert_eq!(o1, "I1 processed".to_string());
        assert_eq!(o2, "I2 processed".to_string());
    }
}
