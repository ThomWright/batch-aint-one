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
    batch_queue::BatchQueue,
    limits::Limits,
    metrics::{BatchStats, GaugeRecording, MetricsRecorder},
    policies::{BatchingPolicy, Event},
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

    /// Queued items and batches in flight, grouped by key `K`.
    batch_queues: HashMap<P::Key, BatchQueue<P>>,
    /// When to ask the batching policy again whether to start acquiring resources for a key.
    ///
    /// An entry is only a reminder to check its key, so it can be stale: the items it was for may
    /// have been processed already, or the queue removed. Checking then starts nothing.
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
        resources: P::Resources,
        span: Span,
        acquisition_duration: Duration,
    },
    ResourceAcquisitionFailed {
        key: P::Key,
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
                resources: _,
                span: _,
                acquisition_duration: _,
            } => f
                .debug_tuple("ResourcesAcquired")
                .field(key)
                .field(&"<Resources>")
                .finish(),
            Message::ResourceAcquisitionFailed {
                key,
                err,
                acquisition_duration: _,
            } => f
                .debug_tuple("ResourceAcquisitionFailed")
                .field(key)
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

    /// Add an item to its key's queue, unless the queue is full or the item has been cancelled.
    fn add(&mut self, mut item: BatchItem<P>) {
        self.metrics_recorder
            .item_received(item.submitted_at.elapsed());
        item.received_at = Some(tokio::time::Instant::now());

        if item.is_cancelled() {
            Self::record_cancelled(&*self.metrics_recorder, 1);
            return;
        }

        let key = item.key.clone();

        let batch_queue = self.batch_queues.entry(key.clone()).or_insert_with(|| {
            BatchQueue::new(self.batcher_name.clone(), key.clone(), self.limits)
        });

        // Cancelled items don't count against the queue limit. Removing them can only make room,
        // so it's only worth doing when the queue looks full.
        let mut rejection = self.batching_policy.should_reject(&batch_queue.state());
        if rejection.is_some() && Self::remove_cancelled(batch_queue, &*self.metrics_recorder) {
            rejection = self.batching_policy.should_reject(&batch_queue.state());
        }

        if let Some(reason) = rejection {
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
            return;
        }

        batch_queue.push(item);
        self.start_acquisitions(&key, Event::ItemAdded);
    }

    /// Get the batch queue for the given key, which should always exist when handling an event
    /// for that key.
    fn queue_mut<'q>(
        batch_queues: &'q mut HashMap<P::Key, BatchQueue<P>>,
        key: &P::Key,
    ) -> &'q mut BatchQueue<P> {
        batch_queues.get_mut(key).expect("batch queue should exist")
    }

    /// Remove the queue's cancelled items, and record how many there were. Returns whether there
    /// were any.
    fn remove_cancelled(
        batch_queue: &mut BatchQueue<P>,
        metrics_recorder: &dyn MetricsRecorder,
    ) -> bool {
        let num_cancelled = batch_queue.remove_cancelled();
        Self::record_cancelled(metrics_recorder, num_cancelled);
        num_cancelled > 0
    }

    fn record_cancelled(metrics_recorder: &dyn MetricsRecorder, num_cancelled: usize) {
        if num_cancelled > 0 {
            metrics_recorder.items_cancelled(num_cancelled);
        }
    }

    /// Start as many acquisitions for the key as the batching policy allows after `event`, then
    /// schedule the policy's next wake-up, if it wants one.
    fn start_acquisitions(&mut self, key: &P::Key, mut event: Event) {
        let batch_queue = Self::queue_mut(&mut self.batch_queues, key);
        let now = Instant::now();
        let mut removed_cancelled = false;

        loop {
            if !self
                .batching_policy
                .should_start_acquiring(event, &batch_queue.state(), now)
            {
                break;
            }
            // The policy decided on counts which may include cancelled items. Fewer items never
            // make a policy start acquiring when it otherwise wouldn't, so it only needs asking
            // again if some were removed. Once is enough: an item cancelled since then is no
            // different to one cancelled during acquisition.
            if !removed_cancelled {
                removed_cancelled = true;
                if Self::remove_cancelled(batch_queue, &*self.metrics_recorder)
                    && !self.batching_policy.should_start_acquiring(
                        event,
                        &batch_queue.state(),
                        now,
                    )
                {
                    break;
                }
            }
            batch_queue.start_acquiring(self.processor.clone(), self.msg_tx.clone());
            event = Event::AcquisitionStarted;
        }

        // A wake-up that's already due would fire straight away and find nothing more to start.
        // The policy is asked again when a batch finishes instead.
        if let Some(wake_up) = self
            .batching_policy
            .wake_up_at(&batch_queue.state())
            .filter(|&wake_up| wake_up > now)
        {
            let keys = self.wake_ups.entry(wake_up).or_default();
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
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
                self.on_wake_up_for(key);
            }
        }
    }

    fn on_wake_up_for(&mut self, key: P::Key) {
        if !self.batch_queues.contains_key(&key) {
            debug!("Wake-up for a batch queue which no longer exists. Ignoring.");
            return;
        }

        self.start_acquisitions(&key, Event::WakeUp);
    }

    fn handle_message(&mut self, msg: Message<P>) {
        match msg {
            Message::ResourcesAcquired {
                key,
                resources,
                span,
                acquisition_duration,
            } => {
                self.on_resources_acquired(key, resources, span, acquisition_duration);
            }
            Message::ResourceAcquisitionFailed {
                key,
                err,
                acquisition_duration,
            } => {
                self.on_resource_acquisition_failed(key, err, acquisition_duration);
            }
            Message::Finished { key, metrics } => {
                self.on_batch_finished(&key, metrics);
            }
        }
    }

    fn on_resources_acquired(
        &mut self,
        key: P::Key,
        resources: P::Resources,
        span: Span,
        acquisition_duration: Duration,
    ) {
        self.metrics_recorder
            .resource_acquisition_completed(acquisition_duration, true);

        let batch_queue = Self::queue_mut(&mut self.batch_queues, &key);
        let num_cancelled =
            batch_queue.process(resources, span, self.processor.clone(), self.msg_tx.clone());
        Self::record_cancelled(&*self.metrics_recorder, num_cancelled);

        self.start_acquisitions(&key, Event::ResourcesAcquired);
        self.remove_if_idle(&key);
    }

    fn on_resource_acquisition_failed(
        &mut self,
        key: P::Key,
        err: BatchError<P::Error>,
        acquisition_duration: Duration,
    ) {
        self.metrics_recorder
            .resource_acquisition_completed(acquisition_duration, false);

        let batch_queue = Self::queue_mut(&mut self.batch_queues, &key);
        let num_cancelled = batch_queue.fail(err);
        Self::record_cancelled(&*self.metrics_recorder, num_cancelled);

        self.start_acquisitions(&key, Event::BatchFinished);
        self.remove_if_idle(&key);
    }

    fn on_batch_finished(&mut self, key: &P::Key, metrics: BatchStats) {
        self.metrics_recorder.batch_completed(&metrics);

        let batch_queue = Self::queue_mut(&mut self.batch_queues, key);
        batch_queue.mark_processed();

        self.start_acquisitions(key, Event::BatchFinished);
        self.remove_if_idle(key);
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
        #[allow(deprecated)]
        self.metrics_recorder
            .queue_depth_changed(total_queued, max_queued);
        self.metrics_recorder
            .queue_items_changed(total_items, max_items);
    }

    /// Remove the queue if the key is now idle, otherwise the map grows unboundedly as new keys
    /// are seen. A key can only become idle once a batch leaves flight, so the handlers for
    /// spawned tasks' messages are the only place this is needed.
    fn remove_if_idle(&mut self, key: &P::Key) {
        if Self::queue_mut(&mut self.batch_queues, key).is_idle() {
            self.batch_queues.remove(key);
        }
    }

    /// Start acquiring resources for every batch that can be processed now, within the
    /// concurrency limits, regardless of the batching policy.
    fn flush(&mut self) {
        for batch_queue in self.batch_queues.values_mut() {
            Self::remove_cancelled(batch_queue, &*self.metrics_recorder);
            while batch_queue.state().can_start_acquiring() {
                batch_queue.start_acquiring(self.processor.clone(), self.msg_tx.clone());
            }
        }
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
                        // Every `Batcher` has been dropped. No special handling is needed: each
                        // `add` borrows a `Batcher`, so every queued item has been cancelled, and
                        // a normal shutdown drops them rather than processing them. It still
                        // waits for work in progress.
                        None => {
                            info!(
                                "Every Batcher for '{}' has been dropped, shutting down",
                                &self.batcher_name
                            );
                            self.shutting_down = true;
                            self.items_drained = true;
                        }
                    }
                }

                Some(msg) = self.msg_rx.recv() => {
                    self.handle_message(msg);
                }
            }

            // Waiting for a batch to fill up or reach its deadline could hold up shutdown, maybe
            // forever. Flushing after every event, rather than once, also catches batches which
            // can start later: when capacity frees up, or items are added.
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

        // max_batch_size is 1, so acquisition starts immediately. Handle the acquisition and
        // Finished messages, as the run loop would.
        handle_next_message(&mut worker).await;
        let output = rx.await.unwrap().0.unwrap();
        assert_eq!(output, "I1 processed");
        handle_next_message(&mut worker).await;

        assert!(
            worker.batch_queues.is_empty(),
            "the batch queue for an idle key should be removed"
        );
    }

    async fn handle_next_message(worker: &mut Worker<SimpleBatchProcessor>) {
        let msg = worker.msg_rx.recv().await.unwrap();
        worker.handle_message(msg);
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
        handle_next_message(&mut worker).await;
        rx.await.unwrap().0.unwrap();
        handle_next_message(&mut worker).await;
        assert!(worker.batch_queues.is_empty());

        tokio::time::advance(Duration::from_secs(5)).await;
        let (item, _rx) = new_item("K1", "I3");
        worker.add(item);

        // The first batch's wake-up: the new item hasn't waited long enough yet.
        tokio::time::advance(Duration::from_secs(5)).await;
        worker.on_wake_up();
        assert_eq!(worker.batch_queues["K1"].in_flight(), 0);

        // The new item's own wake-up.
        tokio::time::advance(Duration::from_secs(5)).await;
        worker.on_wake_up();
        assert_eq!(worker.batch_queues["K1"].in_flight(), 1);
    }

    #[tokio::test]
    async fn drops_items_cancelled_before_they_are_received() {
        let mut worker = new_worker();
        let num_cancelled = Arc::new(Mutex::new(0));
        worker.metrics_recorder = Box::new(CancellationMetrics(Arc::clone(&num_cancelled)));

        let (item, rx) = new_item("K1", "I1");
        drop(rx);
        worker.add(item);

        assert!(
            worker.batch_queues.is_empty(),
            "should not queue the item, or create a queue for its key"
        );
        assert_eq!(*num_cancelled.lock().unwrap(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn counts_items_queued_when_every_batcher_is_dropped_as_cancelled() {
        let mut worker = new_worker();
        worker.limits = Limits::builder().max_batch_size(2).build();
        let num_cancelled = Arc::new(Mutex::new(0));
        worker.metrics_recorder = Box::new(CancellationMetrics(Arc::clone(&num_cancelled)));

        let (item, rx) = new_item("K1", "I1");
        worker.add(item);
        // Every caller is gone once every `Batcher` is, since `add` borrows one.
        drop(rx);

        // The worker's item channel has no senders, as if every `Batcher` had been dropped.
        tokio::time::timeout(Duration::from_secs(1), worker.run())
            .await
            .expect("worker should exit");

        assert_eq!(*num_cancelled.lock().unwrap(), 1);
    }

    #[derive(Debug)]
    struct CancellationMetrics(Arc<Mutex<usize>>);

    impl MetricsRecorder for CancellationMetrics {
        fn items_cancelled(&self, count: usize) {
            *self.0.lock().unwrap() += count;
        }
    }

    #[tokio::test]
    async fn ignores_wake_up_for_removed_batch_queue() {
        let mut worker = new_worker();

        worker.on_wake_up_for("K1".to_string());
    }

    #[tokio::test(start_paused = true)]
    async fn schedules_one_wake_up_per_key_however_many_items_are_added() {
        let mut worker = new_worker();
        worker.limits = Limits::builder().max_batch_size(10).build();
        worker.batching_policy = BatchingPolicy::Duration(Duration::from_secs(10), OnFull::Process);

        for input in ["I1", "I2", "I3"] {
            let (item, _rx) = new_item("K1", input);
            worker.add(item);
        }

        let keys: Vec<_> = worker.wake_ups.values().flatten().collect();
        assert_eq!(keys, ["K1"]);
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

        // Key "A": 3 items, more than one batch's worth.
        let queue_a = worker
            .batch_queues
            .entry("A".to_string())
            .or_insert_with(|| BatchQueue::new("test".to_string(), "A".to_string(), limits));
        for _ in 0..3 {
            queue_a.push(dummy_item("A"));
        }

        // Key "B": 1 item.
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
