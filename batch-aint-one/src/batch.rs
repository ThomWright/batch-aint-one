use std::{
    fmt::Debug,
    iter, mem,
    ops::Deref,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{self, AtomicUsize},
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{Level, Span, debug, instrument::WithSubscriber, span};

use crate::{
    BatchError,
    batch_inner::{BatchInner, Generation},
    error::BatchResult,
    processor::Processor,
    timeout::TimeoutHandle,
    worker::Message,
};

#[derive(Debug)]
pub(crate) struct BatchItem<P: Processor> {
    pub key: P::Key,
    pub input: P::Input,
    pub submitted_at: tokio::time::Instant,
    /// Used to send the output back.
    pub tx: SendOutput<P::Output, P::Error>,
    /// This item was added to the batch as part of this span.
    pub requesting_span: Span,
}

type SendOutput<O, E> = oneshot::Sender<(BatchResult<O, E>, Option<Span>)>;

/// A batch of items to process.
///
/// State management is handled here, including timeouts, concurrency tracking, and resource
/// acquisition.
pub(crate) struct Batch<P: Processor> {
    inner: Arc<BatchInner<P>>,

    items: Vec<BatchItem<P>>,

    timeout: TimeoutHandle<P>,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,

    /// The number of batches with this key that are currently pre-acquiring resources.
    pre_acquiring: Arc<AtomicUsize>,

    resources_state: Arc<Mutex<ResourcesState<P::Resources>>>,
}

impl<P: Processor> Debug for Batch<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Batch {
            inner,
            items,
            timeout: _,
            processing,
            pre_acquiring,
            resources_state,
        } = self;
        f.debug_struct("Batch")
            .field("name", &inner.name())
            .field("key", &inner.key())
            .field("generation", &inner.generation())
            .field("items_len", &items.len())
            .field("processing", &processing.load(atomic::Ordering::Relaxed))
            .field(
                "pre_acquiring",
                &pre_acquiring.load(atomic::Ordering::Relaxed),
            )
            .field(
                "resources_state",
                resources_state
                    .lock()
                    .expect("Resources mutex should not be poisoned")
                    .deref(),
            )
            .finish()
    }
}

enum ResourcesState<R> {
    NotStartedAcquiring,
    StartedAcquiring,
    Acquiring(JoinHandle<()>),
    Acquired {
        resources: R,
        /// The span in which the resources were acquired.
        span: Span,
    },
    AcquiredAndTaken,
    Failed,
}

impl<R> Debug for ResourcesState<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourcesState::NotStartedAcquiring => {
                write!(f, "NotStartedAcquiring")
            }
            ResourcesState::StartedAcquiring => {
                write!(f, "StartedAcquiring")
            }
            ResourcesState::Acquiring(_) => {
                write!(f, "Acquiring(<JoinHandle>)")
            }
            ResourcesState::Acquired { .. } => {
                write!(f, "Acquired(<Resources>)")
            }
            ResourcesState::AcquiredAndTaken => {
                write!(f, "AcquiredAndTaken")
            }
            ResourcesState::Failed => {
                write!(f, "Failed")
            }
        }
    }
}

pub(crate) struct ConcurrencyCountGuard(Arc<AtomicUsize>);
impl ConcurrencyCountGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, atomic::Ordering::AcqRel);
        Self(counter)
    }
}
impl Drop for ConcurrencyCountGuard {
    fn drop(&mut self) {
        let prev = self.0.fetch_sub(1, atomic::Ordering::AcqRel);
        debug_assert!(prev > 0, "counter should not wrap below zero");
    }
}

impl<P: Processor> Batch<P> {
    pub(crate) fn new(
        batcher_name: String,
        key: P::Key,
        processing: Arc<AtomicUsize>,
        pre_acquiring: Arc<AtomicUsize>,
    ) -> Self {
        let state = Arc::new(BatchInner::new(batcher_name.clone(), key.clone()));

        let timeout = TimeoutHandle::new(key.clone(), state.generation());

        Self {
            inner: state,
            items: Vec::new(),
            timeout,
            processing,
            pre_acquiring,
            resources_state: Arc::new(Mutex::new(ResourcesState::NotStartedAcquiring)),
        }
    }

    pub(crate) fn new_generation(&self) -> Self {
        let state = self.inner.new_generation();
        let timeout = TimeoutHandle::new(self.inner.key().clone(), state.generation());
        Self {
            inner: Arc::new(state),
            items: Vec::new(),
            timeout,
            processing: self.processing.clone(),
            pre_acquiring: self.pre_acquiring.clone(),
            resources_state: Arc::new(Mutex::new(ResourcesState::NotStartedAcquiring)),
        }
    }

    /// The size of the batch.
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_new_batch(&self) -> bool {
        self.is_empty()
    }

    pub(crate) fn is_full(&self, max: usize) -> bool {
        self.len() >= max
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn has_single_space(&self, max: usize) -> bool {
        self.len() == max - 1
    }

    pub(crate) fn is_generation(&self, generation: Generation) -> bool {
        self.inner.generation() == generation
    }

    pub(crate) fn is_ready(&self) -> bool {
        // To be ready, we must have some items to process...
        !self.is_empty()
            // ... and if there is a timeout deadline, it must be in the past
            && self.timeout.is_ready_for_processing()
            // ... and we must not be acquiring resources
            && !self.is_acquiring()
    }

    pub(crate) fn push(&mut self, item: BatchItem<P>) {
        self.items.push(item);
    }

    pub(crate) fn has_timeout_expired(&self) -> bool {
        self.timeout.is_expired()
    }

    fn cancel_timeout(&mut self) {
        self.timeout.cancel();
    }

    pub(crate) fn has_started_acquiring(&self) -> bool {
        let state = self.resources_state();
        state.has_started()
    }

    fn is_acquiring(&self) -> bool {
        let state = self.resources_state();
        state.is_acquiring()
    }

    fn has_acquisition_failed(&self) -> bool {
        let state = self.resources_state();
        matches!(*state, ResourcesState::Failed)
    }

    fn started_pre_acquiring(&self) -> ConcurrencyCountGuard {
        let mut state = self.resources_state();
        state.start();
        ConcurrencyCountGuard::new(self.pre_acquiring.clone())
    }

    fn set_acquiring_handle(&self, handle: JoinHandle<()>) {
        let mut state = self.resources_state();
        state.acquiring(handle);
    }

    fn started_processing(&self) -> ConcurrencyCountGuard {
        ConcurrencyCountGuard::new(self.processing.clone())
    }

    /// Acquire resources for this batch, if we are not doing so already.
    ///
    /// Once acquired, a message will be sent to process the batch.
    pub(crate) fn pre_acquire_resources(
        &mut self,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        debug_assert!(
            !self.has_started_acquiring(),
            "should not try to acquire resources if already started acquiring"
        );
        if !self.has_started_acquiring() {
            let name = self.inner.name().to_string();
            let key = self.inner.key().clone();
            let generation = self.inner.generation();

            let acquiring_count_guard = self.started_pre_acquiring();

            let inner_state = Arc::clone(&self.inner);

            let resources_state = Arc::clone(&self.resources_state);

            // Spawn a new task so we can process multiple batches concurrently, without blocking the
            // run loop.
            let handle = tokio::spawn(async move {
                let result = inner_state
                    .pre_acquire(processor)
                    .with_current_subscriber()
                    .await;

                let (new_state, msg) = match result {
                    Ok((resources, span)) => (
                        ResourcesState::Acquired { resources, span },
                        Message::ResourcesAcquired(key, generation),
                    ),
                    Err(err) => (
                        ResourcesState::Failed,
                        Message::ResourceAcquisitionFailed(key, generation, err),
                    ),
                };

                {
                    let mut state = resources_state
                        .lock()
                        .expect("Resources mutex should not be poisoned");
                    *state = new_state;
                }

                drop(acquiring_count_guard);

                if tx.send(msg).await.is_err() {
                    debug!(
                        "Tried to signal resources acquisition failure but the worker has shut down. Batcher: {}",
                        name
                    );
                }
            });

            self.set_acquiring_handle(handle);
        }
    }

    pub(crate) fn process(
        mut self,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        debug_assert!(
            !self.is_acquiring(),
            "should not try to acquire resources if already acquiring"
        );
        debug_assert!(
            !self.has_acquisition_failed(),
            "should not try to process if resource acquisition failed"
        );

        let processing_count_guard = self.started_processing();

        self.cancel_timeout();

        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(
            self.process_inner(processing_count_guard, processor, on_finished)
                .with_current_subscriber(),
        );
    }

    async fn process_inner(
        mut self,
        processing_count_guard: ConcurrencyCountGuard,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let batch_size = self.items.len();
        let name = self.inner.name();
        let key = self.inner.key();

        let delay_since_first_submission = self.first_submission()
            .map(|input| {
                input.elapsed().as_secs()
            })
            .unwrap_or(0);

        let outer_span = span!(Level::INFO, "process batch",
            batch.name = &name,
            batch.key = ?key,
            // Convert to u64 so tracing will treat this as an integer instead of a string.
            batch.size = batch_size as u64,
            batch.first_item_wait_time_secs = delay_since_first_submission,
        );

        // Extract the inputs and response channels from the batch items.
        let (inputs, response_channels): (Vec<_>, Vec<_>) = mem::take(&mut self.items)
            .into_iter()
            .map(|item| {
                // Link the shared batch processing span to the span for each batch item. We
                // don't use a parent relationship because that's 1:many (parent:child), and
                // this is many:1.
                outer_span.follows_from(&item.requesting_span);
                (item.input, item.tx)
            })
            .unzip();

        let resources = self.resources_state().take();

        // Process the batch.
        let outputs = Arc::clone(&self.inner)
            .process(processor, inputs, resources, outer_span.clone())
            .await;

        // Send the outputs back to the requesters.
        Self::send_outputs(response_channels, outputs, Some(outer_span));

        drop(processing_count_guard);

        // Signal that we're finished with this batch.
        Self::finalise(key.clone(), on_finished).await;
    }

    pub(crate) fn fail(
        mut self,
        err: BatchError<P::Error>,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let response_channels: Vec<_> = mem::take(&mut self.items)
            .into_iter()
            .map(|item| item.tx)
            .collect();
        let outputs = iter::repeat_n(err, response_channels.len())
            .map(Err)
            .collect();

        tokio::spawn(async move {
            Self::send_outputs(response_channels, outputs, None);
            Self::finalise(self.inner.key().clone(), on_finished).await;
        });
    }

    fn send_outputs(
        response_channels: Vec<SendOutput<P::Output, P::Error>>,
        outputs: Vec<Result<P::Output, BatchError<P::Error>>>,
        process_span: Option<Span>,
    ) {
        for (tx, output) in response_channels.into_iter().zip(outputs) {
            if tx.send((output, process_span.clone())).is_err() {
                // Whatever was waiting for the output must have shut down. Presumably it
                // doesn't care anymore, but we log here anyway. There's not much else we can do
                // here.
                debug!("Unable to send output over oneshot channel. Receiver deallocated.");
            }
        }
    }

    async fn finalise(key: P::Key, on_finished: mpsc::Sender<Message<P::Key, P::Error>>) {
        if on_finished.send(Message::Finished(key)).await.is_err() {
            // The worker must have shut down. In this case, we don't want to process any more
            // batches anyway.
            debug!("Tried to signal a batch had finished but the worker has shut down");
        }
    }

    pub(crate) fn process_after(
        &mut self,
        duration: Duration,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        self.timeout.set_timeout(duration, tx);
    }

    fn resources_state(&self) -> MutexGuard<'_, ResourcesState<P::Resources>> {
        self.resources_state
            .lock()
            .expect("Resources mutex should not be poisoned")
    }

    fn first_submission(&self) -> Option<tokio::time::Instant> {
        self.items.first().map(|item| item.submitted_at)
    }
}

impl<P: Processor> Drop for Batch<P> {
    fn drop(&mut self) {
        // Cancel any ongoing resource acquisition
        self.resources_state().cancel_acquisition();
    }
}

impl<R> ResourcesState<R> {
    fn take(&mut self) -> Option<(R, Span)> {
        if matches!(*self, ResourcesState::Acquired { .. }) {
            match mem::replace(self, ResourcesState::AcquiredAndTaken) {
                ResourcesState::Acquired { resources, span } => Some((resources, span)),
                _ => None,
            }
        } else {
            None
        }
    }

    fn has_started(&self) -> bool {
        !matches!(*self, ResourcesState::NotStartedAcquiring)
    }

    fn start(&mut self) {
        *self = ResourcesState::StartedAcquiring;
    }

    fn acquiring(&mut self, handle: JoinHandle<()>) {
        if matches!(*self, ResourcesState::StartedAcquiring) {
            *self = ResourcesState::Acquiring(handle);
        }
    }

    fn is_acquiring(&self) -> bool {
        matches!(
            *self,
            ResourcesState::StartedAcquiring | ResourcesState::Acquiring(_)
        )
    }

    fn cancel_acquisition(&mut self) {
        if let ResourcesState::Acquiring(handle) =
            mem::replace(self, ResourcesState::NotStartedAcquiring)
        {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{
        sync::{mpsc, oneshot},
        time,
    };
    use tracing::Span;

    use super::{Batch, BatchItem};

    #[derive(Clone)]
    struct DummyProcessor;
    impl crate::Processor for DummyProcessor {
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
            _inputs: impl Iterator<Item = String> + Send,
            _resources: (),
        ) -> Result<Vec<String>, String> {
            Ok(vec![])
        }
    }

    #[tokio::test(start_paused = true)]
    async fn is_processable_timeout() {
        let mut batch: Batch<DummyProcessor> = Batch::new(
            "test batcher".to_string(),
            "key".to_string(),
            Arc::default(),
            Arc::default(),
        );

        let (tx, _rx) = oneshot::channel();

        batch.push(BatchItem {
            key: "key".to_string(),
            input: "item".to_string(),
            submitted_at: tokio::time::Instant::now(),
            tx,
            requesting_span: Span::none(),
        });

        let (tx, _rx) = mpsc::channel(1);
        batch.process_after(Duration::from_millis(50), tx);

        assert!(!batch.is_ready(), "should not be processable initially");

        time::advance(Duration::from_millis(49)).await;

        assert!(!batch.is_ready(), "should not be processable after 49ms",);

        time::advance(Duration::from_millis(1)).await;

        assert!(batch.is_ready(), "should be processable after 50ms");
    }

    #[tokio::test(start_paused = true)]
    async fn first_submission() {
        let mut batch: Batch<DummyProcessor> = Batch::new(
            "test batcher".to_string(),
            "key".to_string(),
            Arc::default(),
            Arc::default(),
        );

        let (tx, _rx) = oneshot::channel();
        batch.push(BatchItem {
            key: "key".to_string(),
            input: "item".to_string(),
            submitted_at: tokio::time::Instant::now(),
            tx,
            requesting_span: Span::none(),
        });

        time::advance(Duration::from_millis(50)).await;

        let (tx, _rx) = oneshot::channel();
        batch.push(BatchItem {
            key: "key".to_string(),
            input: "item".to_string(),
            submitted_at: tokio::time::Instant::now(),
            tx,
            requesting_span: Span::none(),
        });

        assert!(batch.len() == 2, "batch should have 2 items");
        assert!(
            batch.first_submission().is_some(),
            "first submission should be set"
        );
        assert_eq!(
            batch.first_submission().unwrap().elapsed().as_millis(),
            50,
            "first submission elapsed time should be 50 ms ago"
        );
    }
}
