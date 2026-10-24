//! The worker: one task that owns all batch state, driven by an event loop.
//!
//! The worker only spawns tasks to run user code ([`Processor::acquire_resources`] and
//! [`Processor::process`]), because running it concurrently is the point of batching. Everything
//! else, including waiting for batch deadlines, happens in the loop itself. That keeps a single
//! owner for all state, so there are no races between the worker and its own helpers.
//!
//! The worker never cancels user code. It only exits once every task it spawned has reported
//! back, so when it has exited, no user code it started is still running.

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, Interval, MissedTickBehavior},
};
use tracing::{Span, debug, info};

use crate::{
    BatchError,
    batch::BatchItem,
    batch_inner::Generation,
    batch_queue::BatchQueue,
    limits::Limits,
    metrics::{BatchStats, GaugeRecording, MetricsRecorder},
    policies::{BatchingPolicy, OnAdd, OnDeadline, OnFinish, OnGenerationEvent},
    processor::Processor,
};

pub(crate) struct Worker<P: Processor> {
    batcher_name: String,

    /// Used to receive new batch items.
    item_rx: mpsc::Receiver<BatchItem<P>>,
    /// Set once `item_rx` has no more items to give: it's been closed, or every sender dropped, and
    /// everything sent on it has been received.
    items_drained: bool,
    /// The callback to process a batch of inputs.
    processor: P,

    /// Used to signal that a batch for key `K` should be processed.
    msg_tx: mpsc::UnboundedSender<Message<P>>,
    /// Receives signals to process a batch for key `K`.
    msg_rx: mpsc::UnboundedReceiver<Message<P>>,

    /// Receives requests from [`WorkerHandle`].
    control_rx: mpsc::Receiver<ControlMessage>,

    /// Used to signal to listeners that the worker has shut down.
    shutdown_notifiers: Vec<oneshot::Sender<()>>,

    shutting_down: bool,

    limits: Limits,
    /// Controls when to start processing a batch.
    batching_policy: BatchingPolicy,

    /// Unprocessed batches, grouped by key `K`.
    batch_queues: HashMap<P::Key, BatchQueue<P>>,
    /// When to check keys for batches whose deadlines have passed.
    ///
    /// The deadlines themselves live on the batches. An entry is only a reminder to check its key,
    /// so it can outlive the batch it was made for: the batch may have been processed early, or
    /// its queue removed. Checking then finds nothing ready and does nothing.
    wake_ups: BTreeMap<Instant, Vec<P::Key>>,

    metrics_recorder: Box<dyn MetricsRecorder>,
    gauge_recording: GaugeRecording,
    /// Set when `gauge_recording` is `Interval`.
    gauge_interval: Option<Interval>,
}

/// Events which drive the worker.
///
/// Spawned tasks (resource acquisition and processing) report their outcomes to the worker by
/// sending a message, and the worker performs the resulting batch state transitions when
/// handling them, so it observes all events in message order.
pub(crate) enum Message<P: Processor> {
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

pub(crate) enum ControlMessage {
    Register(ShutdownNotifier),
    ShutDown,
    RecordGauges(oneshot::Sender<()>),
}

pub(crate) struct ShutdownNotifier(oneshot::Sender<()>);

/// A handle to the worker task.
///
/// Used for shutting down the worker and waiting for it to finish.
#[derive(Debug, Clone)]
pub struct WorkerHandle {
    control_tx: mpsc::Sender<ControlMessage>,
}

impl<P: Processor> Worker<P> {
    pub fn spawn(
        batcher_name: String,
        processor: P,
        limits: Limits,
        batching_policy: BatchingPolicy,
        metrics_recorder: Box<dyn MetricsRecorder>,
    ) -> (WorkerHandle, mpsc::Sender<BatchItem<P>>) {
        let (item_tx, item_rx) = mpsc::channel(limits.max_items_awaiting_worker);
        // Unbounded so spawned tasks never wait to report back. Only internal tasks send on this
        // channel, at most one message each per resource acquisition or processing task, so
        // in-flight work bounds its size.
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();

        let (control_tx, control_rx) = mpsc::channel(1);

        let gauge_recording = match metrics_recorder.gauge_recording() {
            GaugeRecording::Interval(period) if period.is_zero() => GaugeRecording::OnChange,
            gauge_recording => gauge_recording,
        };
        let gauge_interval = match gauge_recording {
            GaugeRecording::Interval(period) => {
                let mut interval = tokio::time::interval(period);
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                Some(interval)
            }
            GaugeRecording::OnChange | GaugeRecording::OnDemand => None,
        };

        let mut worker = Worker {
            batcher_name,

            item_rx,
            items_drained: false,
            processor,

            msg_tx,
            msg_rx,

            control_rx,
            shutdown_notifiers: Vec::new(),

            shutting_down: false,

            limits,
            batching_policy,

            batch_queues: HashMap::new(),
            wake_ups: BTreeMap::new(),

            metrics_recorder,
            gauge_recording,
            gauge_interval,
        };

        tokio::spawn(async move {
            worker.run().await;
        });

        (WorkerHandle { control_tx }, item_tx)
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

                let deadline = Instant::now() + duration;
                batch_queue.set_deadline(deadline);
                self.wake_ups.entry(deadline).or_default().push(key);
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

    /// Check every key whose wake-up time has passed.
    fn on_wake_up(&mut self) {
        let now = Instant::now();
        while let Some(entry) = self.wake_ups.first_entry() {
            let wake_up_time = *entry.key();
            if wake_up_time > now {
                break;
            }
            let batch_keys = entry.remove();
            for key in batch_keys {
                self.on_deadline(key);
            }
        }
    }

    /// Process the key's next ready batch, if the batching policy allows it.
    fn on_deadline(&mut self, key: P::Key) {
        let Some(batch_queue) = self.batch_queues.get_mut(&key) else {
            debug!("Deadline for a batch queue which no longer exists. Ignoring.");
            return;
        };

        match self.batching_policy.on_deadline(batch_queue) {
            OnDeadline::ProcessNextReady => {
                self.process_next_ready_batch(&key);
            }
            OnDeadline::DoNothing => {}
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
    }

    fn on_batch_finished(&mut self, key: &P::Key, metrics: BatchStats) {
        self.metrics_recorder.batch_completed(&metrics);

        let batch_queue = Self::queue_mut(&mut self.batch_queues, key);

        batch_queue.mark_processed();

        self.process_next_and_clean_up(key);
    }

    fn record_gauges(&self) {
        self.metrics_recorder
            .active_keys_changed(self.batch_queues.len());

        let (total_processing, max_processing, total_queued, max_queued, total_items, max_items) =
            self.batch_queues
                .values()
                .fold((0, 0, 0, 0, 0, 0), |(tp, mp, tq, mq, ti, mi), bq| {
                    let p = bq.processing();
                    let q = bq.queued();
                    let i = bq.queued_items();
                    (tp + p, mp.max(p), tq + q, mq.max(q), ti + i, mi.max(i))
                });

        self.metrics_recorder
            .processing_concurrency_changed(total_processing, max_processing);
        self.metrics_recorder
            .queue_depth_changed(total_queued, max_queued);
        self.metrics_recorder
            .queue_items_changed(total_items, max_items);
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

    /// Process every batch that can be processed now, within the concurrency limits, regardless
    /// of the batching policy.
    fn flush(&mut self) {
        for batch_queue in self.batch_queues.values_mut() {
            while !batch_queue.at_max_total_processing_capacity()
                && batch_queue.has_batch_to_flush()
            {
                batch_queue.flush_next_batch(self.processor.clone(), self.msg_tx.clone());
            }
        }
    }

    /// Wait for every in-flight resource acquisition and processing task to finish, after the
    /// last [`Batcher`](crate::Batcher) has been dropped.
    ///
    /// Nothing can be waiting for an output, so batches which haven't started are dropped, and so
    /// are resources acquired for them. Only control requests and reports from spawned tasks are
    /// handled here, so no new work can start.
    async fn finish_in_flight(&mut self) {
        let mut in_flight: usize = self.batch_queues.values().map(BatchQueue::in_flight).sum();
        self.batch_queues.clear();
        self.wake_ups.clear();
        info!(
            "Batch worker '{}' is shutting down, waiting for {} in-flight batches",
            &self.batcher_name, in_flight
        );
        if self.gauge_recording == GaugeRecording::OnChange {
            self.record_gauges();
        }

        while in_flight > 0 {
            tokio::select! {
                Some(msg) = self.control_rx.recv() => {
                    match msg {
                        ControlMessage::Register(notifier) => {
                            self.shutdown_notifiers.push(notifier.0);
                        }
                        // Already shutting down.
                        ControlMessage::ShutDown => {}
                        ControlMessage::RecordGauges(done) => {
                            self.record_gauges();
                            // The caller may have stopped waiting, which is fine.
                            let _ = done.send(());
                        }
                    }
                }

                Some(msg) = self.msg_rx.recv() => {
                    match msg {
                        Message::ResourcesAcquired { acquisition_duration, .. } => {
                            self.metrics_recorder
                                .resource_acquisition_completed(acquisition_duration, true);
                        }
                        Message::ResourceAcquisitionFailed { acquisition_duration, .. } => {
                            self.metrics_recorder
                                .resource_acquisition_completed(acquisition_duration, false);
                        }
                        Message::Finished { metrics, .. } => {
                            self.metrics_recorder.batch_completed(&metrics);
                        }
                    }
                    in_flight -= 1;
                }
            }
        }

        self.record_gauges();
    }

    fn all_keys_idle(&self) -> bool {
        self.batch_queues.values().all(|q| q.is_idle())
    }

    /// Start running the worker event loop.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.control_rx.recv() => {
                    match msg {
                        ControlMessage::Register(notifier) => {
                           self.shutdown_notifiers.push(notifier.0);
                        }
                        ControlMessage::ShutDown => {
                            self.shutting_down = true;
                        }
                        ControlMessage::RecordGauges(done) => {
                            self.record_gauges();
                            // The caller may have stopped waiting, which is fine.
                            let _ = done.send(());
                        }
                    }
                }

                _ = tick(&mut self.gauge_interval) => {
                    self.record_gauges();
                }

                _ = sleep_until_first(&self.wake_ups) => {
                    self.on_wake_up();
                }

                item = self.item_rx.recv(), if !self.items_drained => {
                    match item {
                        Some(item) => self.add(item),
                        // While shutting down, the channel ends because the worker closed it, or
                        // because every `Batcher` was dropped. Either way, shutdown carries on
                        // processing every batch, as requested.
                        None if self.shutting_down => self.items_drained = true,
                        // Every `Batcher` has been dropped.
                        None => {
                            self.finish_in_flight().await;
                            return;
                        }
                    }
                }

                Some(msg) = self.msg_rx.recv() => {
                    match msg {
                        Message::ResourcesAcquired { key, generation, resources, span, acquisition_duration } => {
                            self.on_resource_acquired(key, generation, resources, span, acquisition_duration);
                        }
                        Message::ResourceAcquisitionFailed { key, generation, err, acquisition_duration } => {
                            self.on_resource_acquisition_failed(key, generation, err, acquisition_duration);
                        }
                        Message::Finished { key, metrics } => {
                            self.on_batch_finished(&key, metrics);
                        }
                    }
                }
            }

            // Waiting for a batch to fill up or reach its deadline could hold up shutdown, maybe
            // forever. Flushing after every event, rather than once, also catches batches which
            // become processable later: when capacity frees up, resources are acquired, or items
            // are added.
            if self.shutting_down {
                self.flush();
            }

            if self.gauge_recording == GaugeRecording::OnChange {
                self.record_gauges();
            }

            if self.shutting_down && self.all_keys_idle() {
                if self.items_drained {
                    info!("Batch worker '{}' is shutting down", &self.batcher_name);
                    self.record_gauges();
                    return;
                }
                // Items may still be in the channel, or being sent, so close it rather than
                // exiting. The loop keeps receiving until it's drained, and items it receives are
                // processed as normal.
                self.item_rx.close();
            }
        }
    }
}

/// Waits for the next tick, or forever if there is no interval.
async fn tick(interval: &mut Option<Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// Waits until the earliest wake-up time, or forever if there are none.
async fn sleep_until_first<K>(wake_ups: &BTreeMap<Instant, Vec<K>>) {
    match wake_ups.keys().next() {
        Some(&wake_up) => tokio::time::sleep_until(wake_up).await,
        None => std::future::pending().await,
    }
}

impl WorkerHandle {
    /// Signal the worker to shut down after processing any in-flight batches.
    ///
    /// New items are still accepted and processed while shutting down. Once every key is idle,
    /// the worker stops accepting items, processes any already sent, and exits. After that,
    /// [`Batcher::add`](crate::Batcher::add) returns [`BatchError::Tx`].
    ///
    /// While shutting down, batches are processed without waiting for the batching policy, e.g. a
    /// partial batch doesn't wait to fill up and a [`Duration`](crate::BatchingPolicy::Duration)
    /// batch doesn't wait for its deadline. The concurrency limits still apply.
    ///
    /// Shutdown may never complete if new items keep being added, so stop whatever adds them
    /// first. For example, in an HTTP service: stop the server and let in-flight requests finish,
    /// then call `shut_down()` and [`wait_for_shutdown()`](Self::wait_for_shutdown).
    pub async fn shut_down(&self) {
        info!("Sending shut down signal to batch worker");
        // We ignore errors here - if the receiver has gone away, the worker is already shut down.
        let _ = self.control_tx.send(ControlMessage::ShutDown).await;
    }

    /// Record the current gauges with the batcher's [`MetricsRecorder`], and wait until that's
    /// done.
    ///
    /// Works with any [`GaugeRecording`] mode, but is mainly for
    /// [`OnDemand`](GaugeRecording::OnDemand), e.g. calling it from a Prometheus scrape handler
    /// before gathering metrics. Returns straight away if the worker has shut down.
    pub async fn record_gauges(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        // We ignore errors here - if the receiver has gone away, the worker is already shut down.
        let _ = self
            .control_tx
            .send(ControlMessage::RecordGauges(done_tx))
            .await;
        let _ = done_rx.await;
    }

    /// Wait for the worker to exit, either after [`shut_down()`](Self::shut_down) or once the last
    /// [`Batcher`](crate::Batcher) has been dropped.
    ///
    /// Once this returns, every [`Processor::acquire_resources`] and [`Processor::process`] call
    /// the worker started has returned. So if any of them never returns, neither does this.
    pub async fn wait_for_shutdown(&self) {
        // We ignore errors here - if the receiver has gone away, the worker is already shut down.
        let (notifier_tx, notifier_rx) = oneshot::channel();
        let _ = self
            .control_tx
            .send(ControlMessage::Register(ShutdownNotifier(notifier_tx)))
            .await;
        // Wait for the notifier to be dropped.
        let _ = notifier_rx.await;
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use tokio::sync::oneshot;
    use tracing::Span;

    use super::*;
    use crate::{OnFull, error::BatchResult};

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
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        let (_control_tx, control_rx) = mpsc::channel(1);

        Worker {
            batcher_name: "test".to_string(),
            item_rx,
            items_drained: false,
            processor: SimpleBatchProcessor,
            msg_tx,
            msg_rx,
            control_rx,
            shutdown_notifiers: Vec::new(),
            shutting_down: false,
            limits: Limits::builder().max_batch_size(1).build(),
            batching_policy: BatchingPolicy::Size,
            batch_queues: HashMap::new(),
            wake_ups: BTreeMap::new(),
            metrics_recorder: Box::new(crate::metrics::NoopMetricsRecorder),
            gauge_recording: GaugeRecording::OnDemand,
            gauge_interval: None,
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

    type OutputReceiver = oneshot::Receiver<(BatchResult<String, String>, Option<Span>)>;

    fn new_item(key: &str, input: &str) -> (BatchItem<SimpleBatchProcessor>, OutputReceiver) {
        let (tx, rx) = oneshot::channel();
        let item = BatchItem {
            key: key.to_string(),
            input: input.to_string(),
            submitted_at: tokio::time::Instant::now(),
            received_at: None,
            tx,
            requesting_span: Span::none(),
        };
        (item, rx)
    }

    #[tokio::test(start_paused = true)]
    async fn wake_up_for_an_earlier_batch_does_not_process_a_later_one() {
        let mut worker = new_worker();
        worker.limits = Limits::builder().max_batch_size(2).build();
        worker.batching_policy = BatchingPolicy::Duration(Duration::from_secs(10), OnFull::Process);

        // The first batch fills up and is processed before its deadline, and the key's queue is
        // removed once it's idle. Its wake-up is still pending.
        let (item, _rx) = new_item("K1", "I1");
        worker.add(item);
        let (item, rx) = new_item("K1", "I2");
        worker.add(item);
        rx.await.unwrap().0.unwrap();
        let Some(Message::Finished { key, metrics }) = worker.msg_rx.recv().await else {
            panic!("expected Finished message");
        };
        worker.on_batch_finished(&key, metrics);
        assert!(worker.batch_queues.is_empty());

        tokio::time::advance(Duration::from_secs(5)).await;
        let (item, _rx) = new_item("K1", "I3");
        worker.add(item);

        // The first batch's wake-up: the new batch's deadline hasn't passed yet.
        tokio::time::advance(Duration::from_secs(5)).await;
        worker.on_wake_up();
        assert_eq!(worker.batch_queues["K1"].processing(), 0);

        // The new batch's own wake-up.
        tokio::time::advance(Duration::from_secs(5)).await;
        worker.on_wake_up();
        assert_eq!(worker.batch_queues["K1"].processing(), 1);
    }

    #[tokio::test]
    async fn ignores_deadline_for_removed_batch_queue() {
        let mut worker = new_worker();

        worker.on_deadline("K1".to_string());
    }

    #[tokio::test]
    async fn item_channel_is_sized_by_max_items_awaiting_worker() {
        let (_worker_handle, item_tx) = Worker::<SimpleBatchProcessor>::spawn(
            "test".to_string(),
            SimpleBatchProcessor,
            Limits::builder().max_items_awaiting_worker(5).build(),
            BatchingPolicy::Size,
            Box::new(crate::metrics::NoopMetricsRecorder),
        );

        assert_eq!(item_tx.max_capacity(), 5);
    }

    #[tokio::test]
    async fn simple_test_over_channel() {
        let (_worker_handle, item_tx) = Worker::<SimpleBatchProcessor>::spawn(
            "test".to_string(),
            SimpleBatchProcessor,
            Limits::builder().max_batch_size(2).build(),
            BatchingPolicy::Size,
            Box::new(crate::metrics::NoopMetricsRecorder),
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

    #[derive(Debug, Clone)]
    struct RecordingMetrics(Arc<Mutex<(usize, usize)>>);

    impl MetricsRecorder for RecordingMetrics {
        fn queue_items_changed(&self, total: usize, max_per_key: usize) {
            *self.0.lock().unwrap() = (total, max_per_key);
        }
    }

    fn dummy_item(key: &str) -> BatchItem<SimpleBatchProcessor> {
        let (tx, _rx) = oneshot::channel();
        BatchItem {
            key: key.to_string(),
            input: "I".to_string(),
            submitted_at: tokio::time::Instant::now(),
            received_at: None,
            tx,
            requesting_span: Span::none(),
        }
    }

    #[tokio::test]
    async fn queue_items_changed_sums_and_maxes_across_keys() {
        let mut worker = new_worker();
        worker.limits = Limits::builder().max_batch_size(2).build();

        let gauges = Arc::new(Mutex::new((0, 0)));
        worker.metrics_recorder = Box::new(RecordingMetrics(gauges.clone()));

        let limits = worker.limits;

        // Key "A": 3 items split across 2 batches (one full, one with a single item).
        let queue_a = worker
            .batch_queues
            .entry("A".to_string())
            .or_insert_with(|| BatchQueue::new("test".to_string(), "A".to_string(), limits));
        for _ in 0..3 {
            queue_a.push(dummy_item("A"));
        }

        // Key "B": 1 item in a single batch.
        let queue_b = worker
            .batch_queues
            .entry("B".to_string())
            .or_insert_with(|| BatchQueue::new("test".to_string(), "B".to_string(), limits));
        queue_b.push(dummy_item("B"));

        worker.record_gauges();

        let (total, max_per_key) = *gauges.lock().unwrap();
        assert_eq!(total, 4, "should sum items across both keys");
        assert_eq!(
            max_per_key, 3,
            "should report the highest per-key item count, not batch count"
        );
    }

    #[tokio::test]
    async fn shutdown_processes_items_sent_as_the_worker_becomes_idle() {
        let mut worker = new_worker();
        let (item_tx, item_rx) = mpsc::channel(1);
        worker.item_rx = item_rx;
        let (control_tx, control_rx) = mpsc::channel(1);
        worker.control_rx = control_rx;
        worker.shutting_down = true;

        // A send is in progress while the worker handles an event with every key idle.
        let permit = item_tx.reserve().await.unwrap();
        let (done_tx, done_rx) = oneshot::channel();
        control_tx
            .send(ControlMessage::RecordGauges(done_tx))
            .await
            .unwrap();
        let run = tokio::spawn(async move { worker.run().await });
        done_rx.await.unwrap();

        let (item, rx) = new_item("K1", "I1");
        permit.send(item);

        let output = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("the item should be processed before the worker exits")
            .unwrap()
            .0
            .unwrap();
        assert_eq!(output, "I1 processed");
        run.await.unwrap();
    }
}
