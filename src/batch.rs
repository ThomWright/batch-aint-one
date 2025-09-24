use std::{
    fmt::{Debug, Display},
    mem,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{self, AtomicUsize},
    },
    time::Duration,
};

use futures::FutureExt;
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinHandle},
};
use tracing::{Instrument, Level, Span, debug, span};

use crate::{
    BatchError, error::BatchResult, processor::Processor, timeout::TimeoutHandle, worker::Message,
};

#[derive(Debug)]
pub(crate) struct BatchItem<P: Processor> {
    pub key: P::Key,
    pub input: P::Input,
    /// Used to send the output back.
    pub tx: SendOutput<P::Output, P::Error>,
    /// This item was added to the batch as part of this span.
    pub requesting_span: Span,
}

type SendOutput<O, E> = oneshot::Sender<(BatchResult<O, E>, Option<Span>)>;

/// A batch of items to process.
pub(crate) struct Batch<P: Processor> {
    batcher_name: String,

    key: P::Key,
    generation: Generation,
    items: Vec<BatchItem<P>>,

    timeout: TimeoutHandle<P>,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,

    /// The number of batches with this key that are currently pre-acquiring resources.
    pre_acquiring: Arc<AtomicUsize>,

    resources_state: Arc<Mutex<ResourcesState<P::Resources>>>,
}

struct ConcurrencyCountGuard(Arc<AtomicUsize>);
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

#[derive(Debug)]
enum ResourcesState<R> {
    NotAcquired,
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

    fn is_acquiring(&self) -> bool {
        matches!(
            *self,
            ResourcesState::StartedAcquiring | ResourcesState::Acquiring(_)
        )
    }

    fn cancel_acquisition(&mut self) {
        if let ResourcesState::Acquiring(handle) = mem::replace(self, ResourcesState::NotAcquired) {
            handle.abort();
        }
    }
}

/// Generations are used to handle the case where a timer goes off after the associated batch has
/// already been processed, and a new batch has already been created with the same key.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub(crate) struct Generation(u32);

impl Generation {
    fn next(&self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl<P: Processor> Batch<P> {
    pub(crate) fn new(
        batcher_name: String,
        key: P::Key,
        processing: Arc<AtomicUsize>,
        pre_acquiring: Arc<AtomicUsize>,
    ) -> Self {
        let generation = Generation::default();
        let timeout = TimeoutHandle::new(key.clone(), generation);
        Self {
            batcher_name,

            key,
            generation,
            items: Vec::new(),

            timeout,

            processing,
            pre_acquiring,

            resources_state: Arc::new(Mutex::new(ResourcesState::NotAcquired)),
        }
    }

    pub(crate) fn new_generation(&self) -> Self {
        let generation = self.generation.next();
        Self {
            batcher_name: self.batcher_name.clone(),

            key: self.key.clone(),
            generation,
            items: Vec::new(),

            timeout: TimeoutHandle::new(self.key.clone(), generation),

            processing: Arc::clone(&self.processing),
            pre_acquiring: Arc::clone(&self.pre_acquiring),

            resources_state: Arc::new(Mutex::new(ResourcesState::NotAcquired)),
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

    pub(crate) fn generation(&self) -> Generation {
        self.generation
    }

    pub(crate) fn is_generation(&self, generation: Generation) -> bool {
        self.generation == generation
    }

    pub(crate) fn is_processable(&self) -> bool {
        // To be processable, we must have some items to process...
        !self.is_empty()
            // ... and if there is a timeout deadline, it must be in the past.
            && self.timeout.is_ready_for_processing()
            && !self.is_acquiring_resources()
    }

    pub(crate) fn has_timeout_expired(&self) -> bool {
        self.timeout.is_expired()
    }

    pub(crate) fn push(&mut self, item: BatchItem<P>) {
        self.items.push(item);
    }

    fn cancel_timeout(&mut self) {
        self.timeout.cancel();
    }

    /// Get the key for this batch.
    pub fn key(&self) -> P::Key {
        self.key.clone()
    }

    /// Acquire resources for this batch, if we are not doing so already.
    ///
    /// Once acquired, a message will be sent to process the batch.
    pub(crate) fn pre_acquire_resources(
        &mut self,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let mut resources = self.resources_state();
        if matches!(*resources, ResourcesState::NotAcquired) {
            let acquiring_count_guard = ConcurrencyCountGuard::new(Arc::clone(&self.pre_acquiring));

            let key = self.key();
            let name = self.batcher_name.clone();
            let generation = self.generation();

            *resources = ResourcesState::StartedAcquiring;

            let resource_state = Arc::clone(&self.resources_state);
            let handle = tokio::spawn(async move {
                let acquiring_count_guard = acquiring_count_guard;

                let span = acquire_resources_span(&name, &key);

                let result = Self::pre_acquire_inner(key.clone(), processor, span.clone()).await;

                match result {
                    Ok(resources) => {
                        {
                            let mut state = resource_state
                                .lock()
                                .expect("Resources mutex should not be poisoned");
                            *state = ResourcesState::Acquired { resources, span };
                        }

                        drop(acquiring_count_guard);

                        if tx.send(Message::Process(key, generation)).await.is_err() {
                            debug!(
                                "Tried to signal resources had been acquired but the worker has shut down. Batcher: {}",
                                name
                            );
                        }
                    }
                    Err(err) => {
                        {
                            let mut state = resource_state
                                .lock()
                                .expect("Resources mutex should not be poisoned");
                            *state = ResourcesState::Failed;
                        }

                        drop(acquiring_count_guard);

                        // Failed to acquire resources. Fail the batch.
                        if tx.send(Message::Fail(key, generation, err)).await.is_err() {
                            debug!(
                                "Tried to signal resources acquisition failure but the worker has shut down. Batcher: {}",
                                name
                            );
                        }
                    }
                }
            });

            *resources = ResourcesState::Acquiring(handle);
        }
    }

    async fn pre_acquire_inner(
        key: P::Key,
        processor: P,
        span: Span,
    ) -> Result<P::Resources, BatchError<P::Error>> {
        // Wrap resource acquisition in a spawn to catch panics
        tokio::spawn(async move {
            processor
                .acquire_resources(key.clone())
                .instrument(span.clone())
                .await
        })
        .await
        .map_err(join_error_to_batch_error)
        .and_then(|r| r.map_err(BatchError::ResourceAcquisitionFailed))
    }

    pub(crate) fn is_acquiring_resources(&self) -> bool {
        self.resources_state().is_acquiring()
    }

    pub(crate) fn process(
        mut self,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let processing_count_guard = ConcurrencyCountGuard::new(Arc::clone(&self.processing));

        self.cancel_timeout();

        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(async move {
            let processing_count_guard = processing_count_guard;

            let batch_size = self.items.len();
            let outer_span = span!(Level::INFO, "process batch",
                batch.name = &self.batcher_name,
                batch.key = ?self.key(),
                // Convert to u64 so tracing will treat this as an integer instead of a string.
                batch.size = batch_size as u64
            );

            let key = self.key.clone();

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

            // Process the batch.
            let outputs = Self::process_inner(
                self.batcher_name.clone(),
                key.clone(),
                processor,
                inputs,
                Arc::clone(&self.resources_state),
            )
            .instrument(outer_span.clone())
            .await;

            // Send the outputs back to the requesters.
            Self::send_outputs(response_channels, outputs, Some(outer_span)).await;

            drop(processing_count_guard);

            // Signal that we're finished with this batch.
            Self::finalise(key, on_finished).await;
        });
    }

    async fn process_inner(
        name: String,
        key: P::Key,
        processor: P,
        inputs: Vec<P::Input>,
        resources_state: Arc<Mutex<ResourcesState<P::Resources>>>,
    ) -> Vec<Result<P::Output, BatchError<P::Error>>> {
        let batch_size = inputs.len();

        // Spawn a task so we can catch panics.
        let result = tokio::spawn(async move {
            // Acquire resources (if we don't have them already).
            let resources = Self::acquire_resources(
                resources_state,
                processor.clone(),
                &name,
                key.clone(),
            )
            .await;

            // Early exit if we failed to acquire resources.
            let resources = match resources {
                Ok(resources) => resources,
                Err(err) => {
                    return Err(BatchError::ResourceAcquisitionFailed(err));
                }
            };

            // Now process the batch.
            let inner_span =
                span!(Level::INFO, "process()", batch.name = &name, batch.key = ?key, batch.size = batch_size as u64);
            processor
                .process(key, inputs.into_iter(), resources)
                .map(|r| r.map_err(BatchError::BatchFailed))
                .instrument(inner_span.clone())
                .await
        })
        .await
        .map_err(join_error_to_batch_error)
        .and_then(|r| r);

        // Collect the outputs
        let outputs: Vec<Result<P::Output, BatchError<P::Error>>> = match result {
            Ok(outputs) => outputs.into_iter().map(Ok).collect(),

            // If we failed to process the batch, return the error for each item in the batch.
            Err(err) => std::iter::repeat_n(err, batch_size).map(Err).collect(),
        };

        outputs
    }

    /// Acquire resources for processing, either from pre-acquisition or on-demand.
    async fn acquire_resources(
        resources_state: Arc<Mutex<ResourcesState<P::Resources>>>,
        processor: P,
        name: &str,
        key: P::Key,
    ) -> Result<P::Resources, P::Error> {
        // Try to take pre-acquired resources first
        let pre_acquired = {
            let mut resources = resources_state
                .lock()
                .expect("Resources mutex should not be poisoned");
            resources.take()
        };

        match pre_acquired {
            Some((resources, acquire_span)) => {
                // We've pre-acquired resources
                Span::current().follows_from(acquire_span);
                Ok(resources)
            }
            None => {
                // Not pre-acquired, so acquire now
                let acquire_span = acquire_resources_span(name, &key);
                processor
                    .acquire_resources(key.clone())
                    .instrument(acquire_span.clone())
                    .await
            }
        }
    }

    pub fn fail(
        mut self,
        err: BatchError<P::Error>,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let response_channels: Vec<_> = mem::take(&mut self.items)
            .into_iter()
            .map(|item| item.tx)
            .collect();
        let outputs = std::iter::repeat_n(err, response_channels.len())
            .map(Err)
            .collect();

        tokio::spawn(async move {
            Self::send_outputs(response_channels, outputs, None).await;
            Self::finalise(self.key.clone(), on_finished).await;
        });
    }

    async fn send_outputs(
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

    fn resources_state(&'_ self) -> MutexGuard<'_, ResourcesState<P::Resources>> {
        self.resources_state
            .lock()
            .expect("Resources mutex should not be poisoned")
    }
}

impl<P: Processor> Drop for Batch<P> {
    fn drop(&mut self) {
        // Cancel any ongoing resource acquisition
        if let Ok(mut resources) = self.resources_state.try_lock() {
            resources.cancel_acquisition();
        }
    }
}

fn join_error_to_batch_error<E: Display>(join_err: JoinError) -> BatchError<E> {
    if join_err.is_cancelled() {
        BatchError::Cancelled
    } else {
        BatchError::Panic
    }
}

fn acquire_resources_span(name: &str, key: &impl Debug) -> Span {
    span!(Level::INFO, "acquire resources", batch.name = name, batch.key = ?key)
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

    #[tokio::test]
    async fn is_processable_timeout() {
        time::pause();

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
            tx,
            requesting_span: Span::none(),
        });

        let (tx, _rx) = mpsc::channel(1);
        batch.process_after(Duration::from_millis(50), tx);

        assert!(
            !batch.is_processable(),
            "should not be processable initially"
        );

        time::advance(Duration::from_millis(49)).await;

        assert!(
            !batch.is_processable(),
            "should not be processable after 49ms",
        );

        time::advance(Duration::from_millis(1)).await;

        assert!(batch.is_processable(), "should be processable after 50ms");
    }
}
