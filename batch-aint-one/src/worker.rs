use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{Span, debug, info};

use crate::{
    BatchError,
    batch::BatchItem,
    batch_inner::Generation,
    batch_queue::BatchQueue,
    limits::Limits,
    metrics::{BatchStats, MetricsRecorder},
    policies::{BatchingPolicy, OnAdd, OnFinish, OnGenerationEvent},
    processor::Processor,
};

pub(crate) struct Worker<P: Processor> {
    batcher_name: String,

    /// Used to receive new batch items.
    item_rx: mpsc::Receiver<BatchItem<P>>,
    /// The callback to process a batch of inputs.
    processor: P,

    /// Used to signal that a batch for key `K` should be processed.
    msg_tx: mpsc::Sender<Message<P>>,
    /// Receives signals to process a batch for key `K`.
    msg_rx: mpsc::Receiver<Message<P>>,

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

    metrics_recorder: Arc<dyn MetricsRecorder>,
}

/// Events which drive the worker.
///
/// Spawned tasks (resource acquisition, timeouts) report their outcomes to the worker by
/// sending a message, and the worker performs the resulting batch state transitions when
/// handling them, so it observes all events in message order.
pub(crate) enum Message<P: Processor> {
    TimedOut(P::Key, Generation),
    ResourcesAcquired {
        key: P::Key,
        generation: Generation,
        resources: P::Resources,
        span: Span,
        acquisition_duration: Duration,
    },
    ResourceAcquisitionFailed {
        key: P::Key,
        generation: Generation,
        err: BatchError<P::Error>,
        acquisition_duration: Duration,
    },
    Finished {
        key: P::Key,
        metrics: BatchStats,
    },
}

impl<P: Processor> Debug for Message<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::TimedOut(key, generation) => f
                .debug_tuple("TimedOut")
                .field(key)
                .field(generation)
                .finish(),
            Message::ResourcesAcquired {
                key,
                generation,
                resources: _,
                span: _,
                acquisition_duration: _,
            } => f
                .debug_tuple("ResourcesAcquired")
                .field(key)
                .field(generation)
                .field(&"<Resources>")
                .finish(),
            Message::ResourceAcquisitionFailed {
                key,
                generation,
                err,
                acquisition_duration: _,
            } => f
                .debug_tuple("ResourceAcquisitionFailed")
                .field(key)
                .field(generation)
                .field(err)
                .finish(),
            Message::Finished { key, metrics: _ } => f.debug_tuple("Finished").field(key).finish(),
        }
    }
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
        metrics_recorder: Arc<dyn MetricsRecorder>,
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

            metrics_recorder,
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
    fn add(&mut self, mut item: BatchItem<P>) {
        self.metrics_recorder
            .item_received(item.submitted_at.elapsed());
        item.received_at = Some(tokio::time::Instant::now());

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
                self.metrics_recorder.item_rejected();

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

        self.report_gauges();
    }

    /// Get the batch queue for the given key, which should always exist when handling an event
    /// for that key.
    fn queue_mut<'q>(
        batch_queues: &'q mut HashMap<P::Key, BatchQueue<P>>,
        key: &P::Key,
    ) -> &'q mut BatchQueue<P> {
        batch_queues.get_mut(key).expect("batch queue should exist")
    }

    fn process_generation(&mut self, key: P::Key, generation: Generation) {
        let batch_queue = Self::queue_mut(&mut self.batch_queues, &key);

        batch_queue.process_generation(generation, self.processor.clone(), self.msg_tx.clone());
    }

    fn process_next_ready_batch(&mut self, key: &P::Key) {
        let batch_queue = Self::queue_mut(&mut self.batch_queues, key);

        batch_queue.process_next_ready_batch(self.processor.clone(), self.msg_tx.clone());
    }

    fn process_next_batch(&mut self, key: &P::Key) {
        let batch_queue = Self::queue_mut(&mut self.batch_queues, key);

        batch_queue.process_next_batch(self.processor.clone(), self.msg_tx.clone());
    }

    fn on_timeout(&mut self, key: P::Key, generation: Generation) {
        // Unlike the other message handlers, the queue may have been removed: timers are not
        // tracked by the in-flight counters, so a TimedOut message can outlive its queue.
        let Some(batch_queue) = self.batch_queues.get_mut(&key) else {
            debug!("Timeout for a batch queue which no longer exists. Ignoring.");
            return;
        };

        match self.batching_policy.on_timeout(generation, batch_queue) {
            OnGenerationEvent::Process => {
                self.process_generation(key, generation);
            }
            OnGenerationEvent::DoNothing => {}
        }
    }

    fn on_resource_acquired(
        &mut self,
        key: P::Key,
        generation: Generation,
        resources: P::Resources,
        span: Span,
        acquisition_duration: Duration,
    ) {
        self.metrics_recorder
            .resource_acquisition_completed(acquisition_duration, true);

        let batch_queue = Self::queue_mut(&mut self.batch_queues, &key);

        batch_queue.resources_acquired(generation, resources, span);

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

    fn on_resource_acquisition_failed(
        &mut self,
        key: P::Key,
        generation: Generation,
        err: BatchError<P::Error>,
        acquisition_duration: Duration,
    ) {
        self.metrics_recorder
            .resource_acquisition_completed(acquisition_duration, false);

        let batch_queue = Self::queue_mut(&mut self.batch_queues, &key);

        batch_queue.fail_generation(generation, err);

        self.process_next_and_clean_up(&key);
        self.report_gauges();
    }

    fn on_batch_finished(&mut self, key: &P::Key, metrics: BatchStats) {
        self.metrics_recorder.batch_completed(&metrics);

        let batch_queue = Self::queue_mut(&mut self.batch_queues, key);

        batch_queue.mark_processed();

        self.process_next_and_clean_up(key);
        self.report_gauges();
    }

    fn report_gauges(&self) {
        self.metrics_recorder
            .active_keys_changed(self.batch_queues.len());

        let (total_processing, max_processing, total_queued, max_queued) = self
            .batch_queues
            .values()
            .fold((0, 0, 0, 0), |(tp, mp, tq, mq), bq| {
                let p = bq.processing();
                let q = bq.queued();
                (tp + p, mp.max(p), tq + q, mq.max(q))
            });

        self.metrics_recorder
            .processing_concurrency_changed(total_processing, max_processing);
        self.metrics_recorder
            .queue_depth_changed(total_queued, max_queued);
    }

    /// After a batch has left the queue (either processed or having failed to acquire resources),
    /// apply the batching policy's finish action and drop the queue if the key is now idle.
    fn process_next_and_clean_up(&mut self, key: &P::Key) {
        let batch_queue = Self::queue_mut(&mut self.batch_queues, key);

        match self.batching_policy.on_finish(batch_queue) {
            OnFinish::ProcessNextReady => {
                self.process_next_ready_batch(key);
            }
            OnFinish::ProcessNext => {
                self.process_next_batch(key);
            }
            OnFinish::DoNothing => {}
        }

        // Remove the queue for idle keys, otherwise the map grows unboundedly as new keys are
        // seen. A key can only become idle once a batch leaves the queue, so these handlers are
        // the only place we need to do this.
        if Self::queue_mut(&mut self.batch_queues, key).is_idle() {
            self.batch_queues.remove(key);
        }
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
                        Message::ResourcesAcquired { key, generation, resources, span, acquisition_duration } => {
                            self.on_resource_acquired(key, generation, resources, span, acquisition_duration);
                        }
                        Message::ResourceAcquisitionFailed { key, generation, err, acquisition_duration } => {
                            self.on_resource_acquisition_failed(key, generation, err, acquisition_duration);
                        }
                        Message::TimedOut(key, generation) => {
                            self.on_timeout(key, generation);
                        }
                        Message::Finished { key, metrics } => {
                            self.on_batch_finished(&key, metrics);
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
    /// New items are still accepted while shutting down, and the worker only shuts down once all
    /// keys are idle. This means shutdown may never complete if:
    ///
    /// - new items keep being added, or
    /// - a batch never meets its policy's processing condition, e.g. when using the
    ///   [`Size`](crate::BatchingPolicy::Size) policy, a final partial batch may wait
    ///   indefinitely for more items.
    ///
    /// Stopping the flow of new items is expected to be handled by the caller, e.g. by shutting
    /// down the message handlers which add items before shutting down the batcher.
    pub async fn shut_down(&self) {
        info!("Sending shut down signal to batch worker");
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
        info!("Aborting batch worker");
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

    /// Construct a worker directly, without spawning the run loop, so tests can drive it
    /// manually and inspect its state.
    fn new_worker() -> Worker<SimpleBatchProcessor> {
        let (_item_tx, item_rx) = mpsc::channel(1);
        let (msg_tx, msg_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Worker {
            batcher_name: "test".to_string(),
            item_rx,
            processor: SimpleBatchProcessor,
            msg_tx,
            msg_rx,
            shutdown_notifier_rx: shutdown_rx,
            shutdown_notifiers: Vec::new(),
            shutting_down: false,
            limits: Limits::builder().max_batch_size(1).build(),
            batching_policy: BatchingPolicy::Size,
            batch_queues: HashMap::new(),
            metrics_recorder: Arc::new(crate::metrics::NoopMetricsRecorder),
        }
    }

    #[tokio::test]
    async fn removes_batch_queue_when_key_becomes_idle() {
        let mut worker = new_worker();

        let (tx, rx) = oneshot::channel();
        worker.add(BatchItem {
            key: "K1".to_string(),
            input: "I1".to_string(),
            submitted_at: tokio::time::Instant::now(),
            received_at: None,
            tx,
            requesting_span: Span::none(),
        });

        // max_batch_size is 1, so the batch processes immediately.
        let output = rx.await.unwrap().0.unwrap();
        assert_eq!(output, "I1 processed");

        // Handle the Finished message, as the run loop would.
        let msg = worker.msg_rx.recv().await.unwrap();
        let Message::Finished { key, metrics } = msg else {
            panic!("expected Finished message, got {:?}", msg);
        };
        worker.on_batch_finished(&key, metrics);

        assert!(
            worker.batch_queues.is_empty(),
            "the batch queue for an idle key should be removed"
        );
    }

    #[tokio::test]
    async fn ignores_timeout_for_removed_batch_queue() {
        // A timer can fire and enqueue a TimedOut message, after which the batch is processed
        // anyway (e.g. it filled up) and the queue is removed once the key is idle. The stale
        // TimedOut message must be ignored, not panic the worker.
        let mut worker = new_worker();

        worker.on_timeout("K1".to_string(), Generation::default());
    }

    #[tokio::test]
    async fn simple_test_over_channel() {
        let (_worker_handle, _worker_guard, item_tx) = Worker::<SimpleBatchProcessor>::spawn(
            "test".to_string(),
            SimpleBatchProcessor,
            Limits::builder().max_batch_size(2).build(),
            BatchingPolicy::Size,
            Arc::new(crate::metrics::NoopMetricsRecorder),
        );

        let rx1 = {
            let (tx, rx) = oneshot::channel();
            item_tx
                .send(BatchItem {
                    key: "K1".to_string(),
                    input: "I1".to_string(),
                    submitted_at: tokio::time::Instant::now(),
            received_at: None,
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
            received_at: None,
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
