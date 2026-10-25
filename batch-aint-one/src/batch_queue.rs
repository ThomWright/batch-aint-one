use std::{collections::VecDeque, fmt::Debug, sync::Arc};

use tokio::sync::mpsc;
use tracing::{Span, debug, info, instrument::WithSubscriber};

use crate::{
    BatchError, Limits,
    batch::{Batch, BatchItem},
    policies::KeyState,
    processor::Processor,
    user_code,
    worker::Message,
};

/// A key's queued items, and how many of its batches are in flight.
///
/// Items wait in the order they were received. A batch is only formed once resources have been
/// acquired for it, from the oldest items, so whichever acquisition completes first takes them.
///
/// Queued items may have been [cancelled](BatchItem::is_cancelled) since they were added, so the
/// counts in [`state()`](Self::state) are upper bounds until
/// [`remove_cancelled()`](Self::remove_cancelled) is called. Taking a batch skips cancelled items,
/// so they're never processed.
pub(crate) struct BatchQueue<P: Processor> {
    batcher_name: Arc<str>,
    key: P::Key,
    items: VecDeque<BatchItem<P>>,
    limits: Limits,
    /// The number of batches with this key that are acquiring resources.
    acquiring: usize,
    /// The number of batches with this key that are processing.
    processing: usize,
}

impl<P: Processor> BatchQueue<P> {
    pub(crate) fn new(batcher_name: String, key: P::Key, limits: Limits) -> Self {
        Self {
            batcher_name: Arc::from(batcher_name),
            key,
            items: VecDeque::new(),
            limits,
            acquiring: 0,
            processing: 0,
        }
    }

    pub(crate) fn state(&self) -> KeyState {
        let oldest_uncovered = self.items.get(self.acquiring * self.limits.max_batch_size);
        KeyState {
            queued: self.items.len(),
            acquiring: self.acquiring,
            processing: self.processing,
            oldest_uncovered_received_at: oldest_uncovered.and_then(|item| item.received_at),
            limits: self.limits,
        }
    }

    pub(crate) fn push(&mut self, item: BatchItem<P>) {
        self.items.push_back(item);
    }

    /// Remove every cancelled item, returning how many were removed.
    pub(crate) fn remove_cancelled(&mut self) -> usize {
        let len = self.items.len();
        self.items.retain(|item| !item.is_cancelled());
        len - self.items.len()
    }

    /// Is this key idle, i.e. no queued items and no batches in flight?
    pub(crate) fn is_idle(&self) -> bool {
        self.items.is_empty() && self.acquiring == 0 && self.processing == 0
    }

    /// How many batches are acquiring resources or processing.
    pub(crate) fn in_flight(&self) -> usize {
        self.acquiring + self.processing
    }

    pub(crate) fn processing(&self) -> usize {
        self.processing
    }

    /// How many full batches the queued items would make, counting a partial one.
    pub(crate) fn queued(&self) -> usize {
        self.items.len().div_ceil(self.limits.max_batch_size)
    }

    pub(crate) fn queued_items(&self) -> usize {
        self.items.len()
    }

    /// Start acquiring resources for a new batch.
    ///
    /// Once acquired, or once acquisition fails, a message will be sent back to the worker.
    pub(crate) fn start_acquiring(&mut self, processor: P, tx: mpsc::UnboundedSender<Message<P>>) {
        self.acquiring += 1;
        soft_assert!(
            self.in_flight() <= self.limits.max_key_concurrency,
            "batches in flight should not exceed max key concurrency"
        );

        let batcher_name = Arc::clone(&self.batcher_name);
        let key = self.key.clone();
        // Spawn a new task so we can acquire resources for multiple batches concurrently, without
        // blocking the run loop.
        tokio::spawn(
            async move {
                let start = tokio::time::Instant::now();
                let result =
                    user_code::acquire_resources(processor, &batcher_name, key.clone()).await;
                let acquisition_duration = start.elapsed();

                let msg = match result {
                    Ok((resources, span)) => Message::ResourcesAcquired {
                        key,
                        resources,
                        span,
                        acquisition_duration,
                    },
                    Err(err) => Message::ResourceAcquisitionFailed {
                        key,
                        err,
                        acquisition_duration,
                    },
                };
                if tx.send(msg).is_err() {
                    info!(
                        "Tried to signal resource acquisition had finished but the worker has shut down. Batcher: {}",
                        batcher_name
                    );
                }
            }
            .with_current_subscriber(),
        );
    }

    /// Process a batch of the oldest items with resources acquired for it.
    ///
    /// Returns how many cancelled items were skipped.
    pub(crate) fn process(
        &mut self,
        resources: P::Resources,
        span: Span,
        processor: P,
        on_finished: mpsc::UnboundedSender<Message<P>>,
    ) -> usize {
        self.mark_acquisition_finished();

        let (batch, num_cancelled) = self.take_batch();
        if batch.is_empty() {
            debug!(
                "Resources acquired with no items to process. Dropping them. Batcher: {}",
                self.batcher_name
            );
            return num_cancelled;
        }

        self.processing += 1;
        batch.process(processor, resources, span, on_finished);
        num_cancelled
    }

    /// Fail a batch of the oldest items, since resources couldn't be acquired for it.
    ///
    /// Returns how many cancelled items were skipped.
    pub(crate) fn fail(&mut self, err: BatchError<P::Error>) -> usize {
        self.mark_acquisition_finished();
        let (batch, num_cancelled) = self.take_batch();
        batch.fail(err);
        num_cancelled
    }

    pub(crate) fn mark_processed(&mut self) {
        soft_assert!(
            self.processing > 0,
            "processing count should never go below zero"
        );
        self.processing = self.processing.saturating_sub(1);
    }

    fn mark_acquisition_finished(&mut self) {
        soft_assert!(
            self.acquiring > 0,
            "acquiring count should never go below zero"
        );
        self.acquiring = self.acquiring.saturating_sub(1);
    }

    /// Take up to `max_batch_size` of the oldest items which haven't been cancelled, also
    /// removing any cancelled items before the last one taken. Returns the batch, and how many
    /// cancelled items were removed.
    fn take_batch(&mut self) -> (Batch<P>, usize) {
        let max = self.limits.max_batch_size;
        let mut items = Vec::with_capacity(max.min(self.items.len()));
        let mut num_cancelled = 0;
        while items.len() < max {
            let Some(item) = self.items.pop_front() else {
                break;
            };
            if item.is_cancelled() {
                num_cancelled += 1;
            } else {
                items.push(item);
            }
        }

        let batch = Batch::new(Arc::clone(&self.batcher_name), self.key.clone(), items);
        (batch, num_cancelled)
    }
}

impl<P: Processor> Debug for BatchQueue<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            batcher_name,
            key,
            items,
            limits,
            acquiring,
            processing,
        } = self;
        f.debug_struct("BatchQueue")
            .field("batcher_name", batcher_name)
            .field("key", key)
            .field("items_len", &items.len())
            .field("acquiring", &acquiring)
            .field("processing", &processing)
            .field("limits", limits)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        sync::oneshot,
        time::{self, Instant},
    };
    use tracing::Span;

    use super::*;
    use crate::error::BatchResult;

    #[derive(Clone)]
    struct EchoProcessor;

    impl Processor for EchoProcessor {
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
            Ok(inputs.collect())
        }
    }

    type OutputReceiver = oneshot::Receiver<(BatchResult<String, String>, Option<Span>)>;

    fn item(input: &str) -> (BatchItem<EchoProcessor>, OutputReceiver) {
        let (tx, rx) = oneshot::channel();
        let item = BatchItem {
            key: "key".to_string(),
            input: input.to_string(),
            submitted_at: Instant::now(),
            received_at: Some(Instant::now()),
            tx,
            requesting_span: Span::none(),
        };
        (item, rx)
    }

    fn queue(max_batch_size: usize) -> BatchQueue<EchoProcessor> {
        let limits = Limits::builder()
            .max_batch_size(max_batch_size)
            .max_key_concurrency(2)
            .build();
        BatchQueue::new("test".to_string(), "key".to_string(), limits)
    }

    #[tokio::test]
    async fn counts_queued_items_and_batches_worth() {
        let mut queue = queue(2);
        for input in ["1", "2", "3"] {
            queue.push(item(input).0);
        }

        assert_eq!(queue.queued_items(), 3);
        assert_eq!(queue.queued(), 2, "a full batch and a partial one");
    }

    #[tokio::test(start_paused = true)]
    async fn oldest_uncovered_item_is_after_those_acquiring_will_take() {
        let mut queue = queue(2);
        let (tx, _rx) = mpsc::unbounded_channel();
        queue.push(item("1").0);
        queue.push(item("2").0);
        queue.start_acquiring(EchoProcessor, tx);
        time::advance(Duration::from_millis(10)).await;
        let third_received_at = Instant::now();
        queue.push(item("3").0);

        assert_eq!(
            queue.state().oldest_uncovered_received_at,
            Some(third_received_at)
        );
    }

    #[tokio::test]
    async fn whichever_acquisition_completes_first_takes_the_oldest_items() {
        let mut queue = queue(2);
        let (tx, _rx) = mpsc::unbounded_channel();
        let receivers: Vec<_> = ["1", "2", "3"]
            .into_iter()
            .map(|input| {
                let (item, rx) = item(input);
                queue.push(item);
                rx
            })
            .collect();
        queue.start_acquiring(EchoProcessor, tx.clone());
        queue.start_acquiring(EchoProcessor, tx.clone());

        // The results stand in for both acquisitions, in either order: neither is tied to items.
        queue.process((), Span::none(), EchoProcessor, tx.clone());
        assert_eq!(queue.queued_items(), 1, "should take a full batch");
        queue.process((), Span::none(), EchoProcessor, tx);

        let mut outputs = Vec::new();
        for rx in receivers {
            outputs.push(rx.await.unwrap().0.unwrap());
        }
        assert_eq!(outputs, ["1", "2", "3"]);
        assert!(queue.queued_items() == 0 && queue.processing() == 2);
    }

    #[tokio::test]
    async fn drops_resources_acquired_with_no_items_to_process() {
        let mut queue = queue(2);
        let (tx, _rx) = mpsc::unbounded_channel();
        let (item, _output) = item("1");
        queue.push(item);
        queue.start_acquiring(EchoProcessor, tx.clone());
        queue.start_acquiring(EchoProcessor, tx.clone());

        queue.process((), Span::none(), EchoProcessor, tx.clone());
        queue.process((), Span::none(), EchoProcessor, tx);

        assert_eq!(queue.processing(), 1, "only one batch had items");
        assert_eq!(queue.in_flight(), 1);
    }

    #[tokio::test]
    async fn batches_skip_cancelled_items() {
        let mut queue = queue(2);
        let (tx, _rx) = mpsc::unbounded_channel();
        let (cancelled, _) = item("1");
        queue.push(cancelled);
        let receivers: Vec<_> = ["2", "3", "4"]
            .into_iter()
            .map(|input| {
                let (item, rx) = item(input);
                queue.push(item);
                rx
            })
            .collect();
        queue.start_acquiring(EchoProcessor, tx.clone());

        let skipped = queue.process((), Span::none(), EchoProcessor, tx);

        assert_eq!(skipped, 1);
        assert_eq!(
            queue.queued_items(),
            1,
            "should take a full batch of live items"
        );
        let mut receivers = receivers.into_iter();
        for expected in ["2", "3"] {
            let output = receivers.next().unwrap().await.unwrap().0.unwrap();
            assert_eq!(output, expected);
        }
    }

    #[tokio::test]
    async fn removes_cancelled_items_from_anywhere_in_the_queue() {
        let mut queue = queue(2);
        let (first, _first_output) = item("1");
        let (second, _) = item("2");
        let (third, _third_output) = item("3");
        let (fourth, _) = item("4");
        for item in [first, second, third, fourth] {
            queue.push(item);
        }

        assert_eq!(queue.remove_cancelled(), 2);
        assert_eq!(queue.queued_items(), 2);
    }

    #[tokio::test]
    async fn failed_acquisition_fails_a_batch_of_the_oldest_items() {
        let mut queue = queue(2);
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut receivers: Vec<_> = ["1", "2", "3"]
            .into_iter()
            .map(|input| {
                let (item, rx) = item(input);
                queue.push(item);
                rx
            })
            .collect();
        queue.start_acquiring(EchoProcessor, tx);

        queue.fail(BatchError::ResourceAcquisitionFailed("nope".to_string()));

        for rx in receivers.drain(..2) {
            let (result, _) = rx.await.unwrap();
            assert!(matches!(
                result,
                Err(BatchError::ResourceAcquisitionFailed(_))
            ));
        }
        assert_eq!(
            queue.queued_items(),
            1,
            "the newest item should still be queued"
        );
        assert_eq!(queue.in_flight(), 0);
    }
}
