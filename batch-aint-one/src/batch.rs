use std::{
    fmt::Debug,
    iter, mem,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{Level, Span, info, instrument::WithSubscriber, span, warn};

use crate::{
    BatchError,
    batch_inner::{BatchInner, Generation},
    error::BatchResult,
    processor::Processor,
    timeout::TimeoutHandle,
    worker::{BatchTerminalState, Message},
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
    state: Arc<Mutex<BatchState<P>>>,
}

enum BatchState<P: Processor> {
    New,
    StartedAcquiring,
    Acquiring(JoinHandle<()>),
    FailedToAcquireResources,
    ReadyForProcessing {
        resources: P::Resources,
        /// The span in which the resources were acquired.
        span: Span,
    },
    Processing,
    Processed,
}

impl<P: Processor> Debug for Batch<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Batch {
            inner,
            items,
            timeout,
            state,
        } = self;
        f.debug_struct("Batch")
            .field("name", &inner.name())
            .field("key", &inner.key())
            .field("generation", &inner.generation())
            .field("items_len", &items.len())
            .field("timeout", &timeout)
            .field("state", &state)
            .finish()
    }
}

impl<P: Processor> Debug for BatchState<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchState::New => {
                write!(f, "New")
            }
            BatchState::StartedAcquiring => {
                write!(f, "StartedAcquiring")
            }
            BatchState::Acquiring(_) => {
                write!(f, "Acquiring(<JoinHandle>)")
            }
            BatchState::FailedToAcquireResources => {
                write!(f, "FailedToAcquireResources")
            }
            BatchState::ReadyForProcessing { .. } => {
                write!(f, "ReadyForProcessing(<Resources>)")
            }
            BatchState::Processing => {
                write!(f, "Processing")
            }
            BatchState::Processed => {
                write!(f, "Processed")
            }
        }
    }
}

impl<P: Processor> Batch<P> {
    pub(crate) fn new(batcher_name: String, key: P::Key) -> Self {
        let state = Arc::new(BatchInner::new(batcher_name.clone(), key.clone()));

        let timeout = TimeoutHandle::new(key.clone(), state.generation());

        Self {
            inner: state,
            items: Vec::new(),
            timeout,
            state: Arc::new(Mutex::new(BatchState::New)),
        }
    }

    pub(crate) fn new_generation(&self) -> Self {
        let state = self.inner.new_generation();
        let timeout = TimeoutHandle::new(self.inner.key().clone(), state.generation());
        Self {
            inner: Arc::new(state),
            items: Vec::new(),
            timeout,
            state: Arc::new(Mutex::new(BatchState::New)),
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
        self.state.has_started()
    }

    fn is_acquiring(&self) -> bool {
        self.state.is_acquiring()
    }

    /// Acquire resources for this batch.
    ///
    /// Once acquired, a message will be sent back to the worker.
    pub(crate) fn pre_acquire_resources(
        &mut self,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        if self.has_started_acquiring() {
            warn!("should not try to acquire resources if already started acquiring");
        }
        debug_assert!(
            !self.has_started_acquiring(),
            "should not try to acquire resources if already started acquiring"
        );
        if !self.has_started_acquiring() {
            self.state.started_pre_acquiring();

            let name = self.inner.name().to_string();
            let key = self.inner.key().clone();
            let generation = self.inner.generation();

            let inner_state = Arc::clone(&self.inner);

            let state = Arc::clone(&self.state);

            // Spawn a new task so we can process multiple batches concurrently, without blocking the
            // run loop.
            let handle = tokio::spawn(async move {
                let result = inner_state
                    .pre_acquire(processor)
                    .with_current_subscriber()
                    .await;

                let (new_state, msg) = match result {
                    Ok((resources, span)) => (
                        BatchState::ReadyForProcessing { resources, span },
                        Message::ResourcesAcquired(key, generation),
                    ),
                    Err(err) => (
                        BatchState::FailedToAcquireResources,
                        Message::ResourceAcquisitionFailed(key, generation, err),
                    ),
                };

                {
                    let mut state = state
                        .lock()
                        .expect("Resources mutex should not be poisoned");
                    *state = new_state;
                }

                if tx.send(msg).await.is_err() {
                    info!(
                        "Tried to signal resources acquisition failure but the worker has shut down. Batcher: {}",
                        name
                    );
                }
            });

            self.state.set_acquiring(handle);
        }
    }

    pub(crate) fn process(
        mut self,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        if !self.state.is_processable() {
            warn!(
                "should not try to process a batch that is in state {:?}",
                self.state
            );
        }
        debug_assert!(
            self.state.is_processable(),
            "should not try to process a batch that is in state {:?}",
            self.state
        );

        self.cancel_timeout();

        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(
            self.process_inner(processor, on_finished)
                .with_current_subscriber(),
        );
    }

    async fn process_inner(
        mut self,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let batch_size = self.items.len();
        let name = self.inner.name();
        let key = self.inner.key();

        let delay_since_first_submission = self
            .first_submission()
            .map(|input| input.elapsed().as_secs())
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

        // TODO: Always go through resource acquisition before here?
        let resources = self.state.take_resources();

        // Process the batch.
        let outputs = Arc::clone(&self.inner)
            .process(processor, inputs, resources, outer_span.clone())
            .await;

        // Send the outputs back to the requesters.
        Self::send_outputs(response_channels, outputs, Some(outer_span));

        self.state.processed();

        // Signal that we're finished with this batch.
        Self::finalise(key.clone(), on_finished, BatchTerminalState::Processed).await;
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
            Self::finalise(
                self.inner.key().clone(),
                on_finished,
                BatchTerminalState::FailedAcquiring,
            )
            .await;
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
                info!("Unable to send output over oneshot channel. Receiver deallocated.");
            }
        }
    }

    async fn finalise(
        key: P::Key,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
        terminal_state: BatchTerminalState,
    ) {
        if on_finished
            .send(Message::Finished(key, terminal_state))
            .await
            .is_err()
        {
            // The worker must have shut down. In this case, we don't want to process any more
            // batches anyway.
            info!("Tried to signal a batch had finished but the worker has shut down");
        }
    }

    pub(crate) fn process_after(
        &mut self,
        duration: Duration,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        self.timeout.set_timeout(duration, tx);
    }

    fn first_submission(&self) -> Option<tokio::time::Instant> {
        self.items.first().map(|item| item.submitted_at)
    }
}

impl<P: Processor> Drop for Batch<P> {
    fn drop(&mut self) {
        // Cancel any ongoing resource acquisition
        self.state.cancel_acquisition();
    }
}

trait LockedBatchState<P: Processor> {
    fn started_pre_acquiring(&self);
    fn has_started(&self) -> bool;
    fn set_acquiring(&self, handle: JoinHandle<()>);
    fn is_acquiring(&self) -> bool;
    fn is_processable(&self) -> bool;
    fn take_resources(&self) -> Option<(P::Resources, Span)>;
    fn processed(&self);
    fn cancel_acquisition(&self);
}

impl<P: Processor> LockedBatchState<P> for Mutex<BatchState<P>> {
    fn started_pre_acquiring(&self) {
        let mut state = self.lock().expect("Resources mutex should not be poisoned");

        *state = BatchState::StartedAcquiring;
    }

    fn take_resources(&self) -> Option<(<P as Processor>::Resources, Span)> {
        let mut state = self.lock().expect("Resources mutex should not be poisoned");

        if matches!(*state, BatchState::ReadyForProcessing { .. }) {
            match mem::replace(&mut *state, BatchState::Processing) {
                BatchState::ReadyForProcessing { resources, span } => Some((resources, span)),
                _ => None,
            }
        } else {
            None
        }
    }

    fn has_started(&self) -> bool {
        let state = self.lock().expect("Resources mutex should not be poisoned");

        !matches!(*state, BatchState::New)
    }

    fn set_acquiring(&self, handle: JoinHandle<()>) {
        let mut state = self.lock().expect("Resources mutex should not be poisoned");

        if matches!(*state, BatchState::StartedAcquiring) {
            *state = BatchState::Acquiring(handle);
        }
    }

    fn is_acquiring(&self) -> bool {
        let state = self.lock().expect("Resources mutex should not be poisoned");

        matches!(
            *state,
            BatchState::StartedAcquiring | BatchState::Acquiring(_)
        )
    }

    fn is_processable(&self) -> bool {
        let state = self.lock().expect("Resources mutex should not be poisoned");

        matches!(
            *state,
            BatchState::ReadyForProcessing { .. } | BatchState::New
        )
    }

    fn processed(&self) {
        let mut state = self.lock().expect("Resources mutex should not be poisoned");

        *state = BatchState::Processed;
    }

    fn cancel_acquisition(&self) {
        let mut state = self.lock().expect("Resources mutex should not be poisoned");

        if let BatchState::Acquiring(handle) =
            mem::replace(&mut *state, BatchState::FailedToAcquireResources)
        {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        let mut batch: Batch<DummyProcessor> =
            Batch::new("test batcher".to_string(), "key".to_string());

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
        let mut batch: Batch<DummyProcessor> =
            Batch::new("test batcher".to_string(), "key".to_string());

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
