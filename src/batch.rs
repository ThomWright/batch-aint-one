use std::{
    cmp,
    fmt::Debug,
    mem,
    sync::{
        atomic::{self, AtomicUsize},
        Arc, Mutex,
    },
    time::Duration,
};

use futures::FutureExt;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tracing::{debug, span, Instrument, Level, Span};

use crate::{error::BatchResult, processor::Processor, worker::Message, BatchError};

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
    key: P::Key,
    generation: Generation,
    items: Vec<BatchItem<P>>,

    timeout_deadline: Option<Instant>,
    timeout_handle: Option<JoinHandle<()>>,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,

    resources_state: Arc<Mutex<ResourcesState<P::Resources>>>,
}

#[derive(Debug)]
enum ResourcesState<R> {
    NotAcquired,
    Acquiring(JoinHandle<()>),
    Acquired {
        resources: R,
        /// The span in which the resources were acquired.
        span: Span,
    },
}

impl<R> ResourcesState<R> {
    fn take(&mut self) -> Option<(R, Span)> {
        match mem::replace(self, ResourcesState::NotAcquired) {
            ResourcesState::Acquired { resources, span } => Some((resources, span)),
            ResourcesState::NotAcquired | ResourcesState::Acquiring(_) => None,
        }
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
    pub(crate) fn new(key: P::Key, processing: Arc<AtomicUsize>) -> Self {
        Self {
            key,
            generation: Generation::default(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            processing,

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
            && self
                .timeout_deadline
                .is_none_or(|deadline| match deadline.cmp(&Instant::now()){
                    cmp::Ordering::Less => true,
                    cmp::Ordering::Equal => true,
                    cmp::Ordering::Greater => false,
                })
    }

    pub(crate) fn push(&mut self, item: BatchItem<P>) {
        self.items.push(item);
    }

    pub(crate) fn cancel_timeout(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }

    /// Get the key for this batch.
    pub fn key(&self) -> P::Key {
        self.key.clone()
    }

    pub(crate) fn new_generation(&self) -> Self {
        Self {
            key: self.key.clone(),
            generation: self.generation.next(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            processing: self.processing.clone(),

            resources_state: Arc::new(Mutex::new(ResourcesState::NotAcquired)),
        }
    }

    /// Acquire resources for this batch, if we are not doing so already.
    ///
    /// Once acquired, a message will be sent to process the batch.
    pub(crate) fn pre_acquire_resources(
        &mut self,
        processor: P,
        tx: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        let mut resources = self
            .resources_state
            .lock()
            .expect("Resources mutex should not be poisoned");
        if matches!(*resources, ResourcesState::NotAcquired) {
            let key = self.key();
            let generation = self.generation();

            let resource_state = Arc::clone(&self.resources_state);
            let handle = tokio::spawn(async move {
                let span = span!(Level::INFO, "acquire resources", batch.key = ?key);

                // Wrap resource acquisition in a spawn to catch panics
                let acquisition_result = {
                    let key = key.clone();
                    let span = span.clone();
                    tokio::spawn(async move {
                        processor
                            .acquire_resources(key.clone())
                            .instrument(span.clone())
                            .await
                    })
                    .await
                };

                let resources = match acquisition_result {
                    Ok(Ok(r)) => Ok(r),
                    Ok(Err(err)) => Err(BatchError::ResourceAcquisitionFailed(err)),
                    Err(join_err) => {
                        let batch_error = if join_err.is_cancelled() {
                            BatchError::Cancelled
                        } else {
                            BatchError::Panic
                        };
                        Err(batch_error)
                    }
                };
                let resources = match resources {
                    Ok(r) => r,
                    Err(err) => {
                        // Failed to acquire resources. Fail the batch.
                        if tx.send(Message::Fail(key, generation, err)).await.is_err() {
                            debug!("Tried to signal resources acquisition failure but the worker has shut down");
                        }
                        return;
                    }
                };

                {
                    let mut state = resource_state
                        .lock()
                        .expect("Resources mutex should not be poisoned");
                    *state = ResourcesState::Acquired { resources, span };
                }

                if tx.send(Message::Process(key, generation)).await.is_err() {
                    debug!(
                        "Tried to signal resources had been acquired but the worker has shut down"
                    );
                }
            });

            *resources = ResourcesState::Acquiring(handle);
        }
    }

    pub(crate) fn process(
        mut self,
        processor: P,
        on_finished: mpsc::Sender<Message<P::Key, P::Error>>,
    ) {
        self.processing.fetch_add(1, atomic::Ordering::AcqRel);

        self.cancel_timeout();

        let batch_size = self.items.len();
        // Convert to u64 so tracing will treat this as an integer instead of a string.
        let outer_span = span!(Level::INFO, "process batch", batch.key = ?self.key(), batch.size = batch_size as u64);

        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(async move {
            let key = self.key.clone();
            let processing = Arc::clone(&self.processing);

            self.process_inner(processor, batch_size)
                .instrument(outer_span)
                .await;

            let prev = processing.fetch_sub(1, atomic::Ordering::AcqRel);
            debug_assert!(prev > 0, "processing count should not go negative");

            Self::finalise(key, on_finished).await;
        });
    }

    async fn process_inner(mut self, processor: P, batch_size: usize) {
        let outer_span = Span::current();

        let key = self.key.clone();

        // Replace with a placeholder to keep the Drop impl working.
        let items = mem::take(&mut self.items);
        let (inputs, txs): (Vec<P::Input>, Vec<SendOutput<P::Output, P::Error>>) = items
            .into_iter()
            .map(|item| {
                // Link the shared batch processing span to the span for each batch item. We
                // don't use a parent relationship because that's 1:many (parent:child), and
                // this is many:1.
                outer_span.follows_from(&item.requesting_span);
                (item.input, item.tx)
            })
            .unzip();

        let resources_state = Arc::clone(&self.resources_state);

        // Process the batch.
        let result = match tokio::spawn(async move {
            // Acquire resources (if we don't have them already).
            let resources = match Self::acquire_resources_for_processing(
                resources_state,
                processor.clone(),
                key.clone(),
            )
            .await
            {
                Ok(resources) => resources,
                Err(err) => {
                    return Err(BatchError::ResourceAcquisitionFailed(err));
                }
            };

            let inner_span =
                span!(Level::DEBUG, "process", batch.key = ?key, batch.size = batch_size as u64);
            processor
                .process(key, inputs.into_iter(), resources)
                .map(|r| r.map_err(BatchError::BatchFailed))
                .instrument(inner_span.clone())
                .await
        })
        .await
        {
            Ok(r) => r,
            Err(join_err) => {
                if join_err.is_cancelled() {
                    Err(BatchError::Cancelled)
                } else {
                    Err(BatchError::Panic)
                }
            }
        };

        // Collect the outputs and send them back.
        let outputs: Vec<Result<P::Output, BatchError<P::Error>>> = match result {
            Ok(outputs) => outputs.into_iter().map(|o| Ok(o)).collect(),
            Err(err) => std::iter::repeat_n(err, batch_size).map(Err).collect(),
        };
        self.send_results(txs, outputs, Some(outer_span)).await;
    }

    /// Acquire resources for processing, either from pre-acquisition or on-demand.
    async fn acquire_resources_for_processing(
        resources_state: Arc<Mutex<ResourcesState<P::Resources>>>,
        processor: P,
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
                Span::current().follows_from(acquire_span);
                Ok(resources)
            }
            None => {
                // Need to acquire resources now
                let acquire_span = span!(Level::INFO, "acquire resources", batch.key = ?key);
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
        let txs: Vec<_> = mem::take(&mut self.items)
            .into_iter()
            .map(|item| item.tx)
            .collect();
        let outputs = std::iter::repeat_n(err, txs.len()).map(Err).collect();

        tokio::spawn(async move {
            let key = self.key.clone();
            self.send_results(txs, outputs, None).await;
            Self::finalise(key, on_finished).await;
        });
    }

    async fn send_results(
        self,
        txs: Vec<SendOutput<P::Output, P::Error>>,
        outputs: Vec<Result<P::Output, BatchError<P::Error>>>,
        span: Option<Span>,
    ) {
        for (tx, output) in txs.into_iter().zip(outputs) {
            if tx.send((output, span.clone())).is_err() {
                // Whatever was waiting for the output must have shut down. Presumably it
                // doesn't care anymore, but we log here anyway. There's not much else we can do
                // here.
                debug!("Unable to send output over oneshot channel. Receiver deallocated.");
            }
        }
    }

    async fn finalise(key: P::Key, on_finished: mpsc::Sender<Message<P::Key, P::Error>>) {
        // We're finished with this batch
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
        self.cancel_timeout();

        let new_deadline = Instant::now() + duration;
        self.timeout_deadline = Some(new_deadline);

        let key = self.key();
        let generation = self.generation();

        let new_handle = tokio::spawn(async move {
            tokio::time::sleep_until(new_deadline).await;

            if tx.send(Message::Process(key, generation)).await.is_err() {
                // The worker must have shut down. In this case, we don't want to process any more
                // batches anyway.
                debug!("A batch reached a timeout but the worker has shut down");
            }
        });

        self.timeout_handle = Some(new_handle);
    }
}

impl<P: Processor> Drop for Batch<P> {
    fn drop(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }

        // Cancel any ongoing resource acquisition
        if let Ok(mut resources) = self.resources_state.try_lock() {
            resources.cancel_acquisition();
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

        let mut batch: Batch<DummyProcessor> = Batch::new("key".to_string(), Arc::default());

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
