use std::{
    cmp,
    fmt::{Debug, Display},
    mem,
    sync::{
        atomic::{self, AtomicUsize},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tracing::{debug, span, Instrument, Level, Span};

use crate::{error::BatchResult, processor::Processor, worker::Message, BatchError};

#[derive(Debug)]
pub(crate) struct BatchItem<K, I, O, E: Display> {
    pub key: K,
    pub input: I,
    /// Used to send the output back.
    pub tx: SendOutput<O, E>,
    /// This item was added to the batch as part of this span.
    pub requesting_span: Span,
}

type SendOutput<O, E> = oneshot::Sender<(BatchResult<O, E>, Option<Span>)>;

/// A batch of items to process.
#[derive(Debug)]
pub(crate) struct Batch<K, I, O, E: Display, R = ()> {
    key: K,
    generation: Generation,
    items: Vec<BatchItem<K, I, O, E>>,

    timeout_deadline: Option<Instant>,
    timeout_handle: Option<JoinHandle<()>>,

    /// The number of batches with this key that are currently processing.
    processing: Arc<AtomicUsize>,

    resources: Arc<Mutex<Resources<R>>>,
}

#[derive(Debug)]
enum Resources<R> {
    NotAcquired,
    Acquiring,
    Acquired {
        resources: R,
        /// The span in which the resources were acquired.
        span: Span,
    },
}

impl<R> Resources<R> {
    fn take(&mut self) -> Option<(R, Span)> {
        match mem::replace(self, Resources::NotAcquired) {
            Resources::Acquired { resources, span } => Some((resources, span)),
            Resources::NotAcquired | Resources::Acquiring => None,
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

impl<K, I, O, E: Display, R> Batch<K, I, O, E, R> {
    pub(crate) fn new(key: K, processing: Arc<AtomicUsize>) -> Self {
        Self {
            key,
            generation: Generation::default(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            processing,

            resources: Arc::new(Mutex::new(Resources::NotAcquired)),
        }
    }

    /// The size of the batch.
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_new_batch(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn is_full(&self, max: usize) -> bool {
        self.len() >= max
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
        self.len() > 0
            // ... and if there is a timeout deadline, it must be in the past.
            && self
                .timeout_deadline
                .is_none_or(|deadline| match deadline.cmp(&Instant::now()){
                    cmp::Ordering::Less => true,
                    cmp::Ordering::Equal => true,
                    cmp::Ordering::Greater => false,
                })
    }

    pub(crate) fn push(&mut self, item: BatchItem<K, I, O, E>) {
        self.items.push(item);
    }

    pub(crate) fn cancel_timeout(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }
}

impl<K, I, O, E: Display, R> Batch<K, I, O, E, R>
where
    K: Clone,
{
    /// Get the key for this batch.
    pub fn key(&self) -> K {
        self.key.clone()
    }
}

impl<K, I, O, E: Display, R> Batch<K, I, O, E, R>
where
    K: 'static + Send + Clone + Debug,
    I: 'static + Send,
    O: 'static + Send,
    E: 'static + Send + Clone + Display,
    R: 'static + Send,
{
    pub(crate) fn new_generation(&self) -> Self {
        Self {
            key: self.key.clone(),
            generation: self.generation.next(),
            items: Vec::default(),

            timeout_deadline: None,
            timeout_handle: None,

            processing: self.processing.clone(),

            resources: Arc::new(Mutex::new(Resources::NotAcquired)),
        }
    }

    /// Acquire resources for this batch, if we are not doing so already.
    ///
    /// Once acquired, a message will be sent to process the batch.
    pub(crate) fn pre_acquire_resources<F>(&mut self, processor: F, tx: mpsc::Sender<Message<K, E>>)
    where
        F: 'static + Send + Processor<K, I, O, E, R>,
    {
        let mut resources = self
            .resources
            .lock()
            .expect("Resources mutex should not be poisoned");
        if matches!(*resources, Resources::NotAcquired) {
            *resources = Resources::Acquiring;
            drop(resources);

            let key = self.key();
            let generation = self.generation();

            let resource_state = Arc::clone(&self.resources);
            tokio::spawn(async move {
                let span = span!(Level::INFO, "acquire resources", batch.key = ?key);

                let resources = processor
                    .acquire_resources(key.clone())
                    .instrument(span.clone())
                    .await;

                let resources = match resources {
                    Ok(r) => r,
                    Err(err) => {
                        if tx.send(Message::Fail(key, generation, err)).await.is_err() {
                            // The worker must have shut down. In this case, we don't want to process any more
                            // batches anyway.
                            debug!("Tried to signal resources acquisition failed but the worker has shut down");
                        }
                        return;
                    }
                };

                {
                    let mut state = resource_state
                        .lock()
                        .expect("Resources mutex should not be poisoned");
                    *state = Resources::Acquired { resources, span };
                    drop(state);
                }

                if tx.send(Message::Process(key, generation)).await.is_err() {
                    // The worker must have shut down. In this case, we don't want to process any more
                    // batches anyway.
                    debug!(
                        "Tried to signal resources had been acquired but the worker has shut down"
                    );
                }
            });
        }
    }

    pub(crate) fn process<F>(mut self, processor: F, on_finished: mpsc::Sender<Message<K, E>>)
    where
        F: 'static + Send + Processor<K, I, O, E, R>,
    {
        self.processing.fetch_add(1, atomic::Ordering::AcqRel);

        self.cancel_timeout();

        let batch_size = self.items.len();
        // Convert to u64 so tracing will treat this as an integer instead of a string.
        let outer_span = span!(Level::INFO, "process batch", batch.key = ?self.key(), batch.size = batch_size as u64);

        // Spawn a new task so we can process multiple batches concurrently, without blocking the
        // run loop.
        tokio::spawn(
            async move {
                let outer_span = Span::current();

                // Replace with a placeholder to keep the Drop impl working.
                let items = mem::take(&mut self.items);

                let (inputs, txs): (Vec<I>, Vec<SendOutput<O, E>>) = items
                    .into_iter()
                    .map(|item| {
                        // Link the shared batch processing span to the span for each batch item. We
                        // don't use a parent relationship because that's 1:many (parent:child), and
                        // this is many:1.
                        outer_span.follows_from(&item.requesting_span);
                        (item.input, item.tx)
                    })
                    .unzip();

                // Acquire resources (if we don't have them already).
                let resources = {
                    let mut resources = self
                        .resources
                        .lock()
                        .expect("Resources mutex should not be poisoned");
                    resources.take()
                };
                let resources = match resources {
                    Some((r, acquire_span)) => {
                        outer_span.follows_from(acquire_span);
                        r
                    }
                    None => {
                        let acquire_span =
                            span!(Level::INFO, "acquire resources", batch.key = ?self.key());
                        let resources = processor
                            .acquire_resources(self.key.clone())
                            .instrument(acquire_span.clone())
                            .await;
                        match resources {
                            Ok(r) => r,
                            Err(err) => {
                                let outputs: Vec<_> = std::iter::repeat_n(err, batch_size)
                                    .map(|e| Err(BatchError::ResourceAcquisitionFailed(e)))
                                    .collect();
                                self.finalise(txs, outputs, Some(outer_span), on_finished).await;
                                return;
                            }
                        }
                    }
                };

                let inner_span = span!(Level::DEBUG, "process", batch.key = ?self.key(), batch.size = batch_size as u64);

                let result = processor
                    .process(self.key.clone(), inputs.into_iter(), resources)
                    .instrument(inner_span.clone())
                    .await;

                let outputs: Vec<_> = match result {
                    Ok(outputs) => outputs.into_iter().map(|o| Ok(o)).collect(),
                    Err(err) => std::iter::repeat_n(err, batch_size)
                        .map(|e| Err(BatchError::BatchFailed(e)))
                        .collect(),
                };

                self.finalise(txs, outputs, Some(outer_span), on_finished).await;
            }
            .instrument(outer_span),
        );
    }

    pub fn fail(mut self, err: E, on_finished: mpsc::Sender<Message<K, E>>) {
        let txs: Vec<_> = mem::take(&mut self.items)
            .into_iter()
            .map(|item| item.tx)
            .collect();
        let outputs = std::iter::repeat_n(err, txs.len())
            .map(|e| Err(BatchError::ResourceAcquisitionFailed(e)))
            .collect();

        tokio::spawn(self.finalise(txs, outputs, None, on_finished));
    }

    /// Send outputs and clean up.
    async fn finalise(
        self,
        txs: Vec<SendOutput<O, E>>,
        outputs: Vec<Result<O, BatchError<E>>>,
        span: Option<Span>,
        on_finished: mpsc::Sender<Message<K, E>>,
    ) {
        for (tx, output) in txs.into_iter().zip(outputs) {
            if tx.send((output, span.clone())).is_err() {
                // Whatever was waiting for the output must have shut down. Presumably it
                // doesn't care anymore, but we log here anyway. There's not much else we can do
                // here.
                debug!("Unable to send output over oneshot channel. Receiver deallocated.");
            }
        }

        self.processing.fetch_sub(1, atomic::Ordering::AcqRel);

        // We're finished with this batch
        if on_finished
            .send(Message::Finished(self.key.clone()))
            .await
            .is_err()
        {
            // The worker must have shut down. In this case, we don't want to process any more
            // batches anyway.
            debug!("Tried to signal a batch had finished but the worker has shut down");
        }
    }
}

impl<K, I, O, E, R> Batch<K, I, O, E, R>
where
    K: 'static + Send + Clone + Debug,
    E: 'static + Send + Display,
{
    pub(crate) fn process_after(&mut self, duration: Duration, tx: mpsc::Sender<Message<K, E>>) {
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

impl<K, I, O, E: Display, R> Drop for Batch<K, I, O, E, R> {
    fn drop(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
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

    #[tokio::test]
    async fn is_processable_timeout() {
        time::pause();

        let mut batch: Batch<String, String, String, String> =
            Batch::new("key".to_string(), Arc::default());

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
