use std::{collections::HashMap, fmt::Display, hash::Hash};

use tokio::{sync::mpsc, task::JoinHandle};
use tracing::debug;

use crate::{
    batch::{BatchItem, Generation},
    batch_queue::BatchQueue,
    policies::{BatchingPolicy, Limits, PostFinish, PreAdd},
    processor::Processor,
    BatchError,
};

pub(crate) struct Worker<K, I, O, F, E: Display, R = ()> {
    /// Used to receive new batch items.
    item_rx: mpsc::Receiver<BatchItem<K, I, O, E>>,
    /// The callback to process a batch of inputs.
    processor: F,

    /// Used to signal that a batch for key `K` should be processed.
    msg_tx: mpsc::Sender<Message<K>>,
    /// Receives signals to process a batch for key `K`.
    msg_rx: mpsc::Receiver<Message<K>>,

    limits: Limits,
    /// Controls when to start processing a batch.
    batching_policy: BatchingPolicy,

    /// Unprocessed batches, grouped by key `K`.
    batch_queues: HashMap<K, BatchQueue<K, I, O, E, R>>,
}

pub(crate) enum Message<K> {
    Process(K, Generation),
    Finished(K),
}

#[derive(Debug)]
pub(crate) struct WorkerHandle {
    handle: JoinHandle<()>,
}

impl<K, I, O, F, E, R> Worker<K, I, O, F, E, R>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    F: 'static + Send + Clone + Processor<K, I, O, E, R>,
    E: 'static + Send + Clone + Display,
    R: 'static + Send,
{
    pub fn spawn(
        processor: F,
        limits: Limits,
        batching_policy: BatchingPolicy,
    ) -> (WorkerHandle, mpsc::Sender<BatchItem<K, I, O, E>>) {
        let (item_tx, item_rx) = mpsc::channel(10);

        let (timeout_tx, timeout_rx) = mpsc::channel(10);

        let mut worker = Worker {
            item_rx,
            processor,

            msg_tx: timeout_tx,
            msg_rx: timeout_rx,

            limits,
            batching_policy,

            batch_queues: HashMap::new(),
        };

        let handle = tokio::spawn(async move {
            worker.run().await;
        });

        (WorkerHandle { handle }, item_tx)
    }

    /// Add an item to the batch.
    fn add(&mut self, item: BatchItem<K, I, O, E>) {
        let key = item.key.clone();

        let batch_queue = self
            .batch_queues
            .entry(key.clone())
            .or_insert_with(|| BatchQueue::new(key.clone(), self.limits));

        match self.batching_policy.pre_add(batch_queue) {
            PreAdd::AddAndProcess => {
                batch_queue.push(item);

                self.process_next_batch(&key);
            }
            PreAdd::AddAndAcquireResources => {
                batch_queue.push(item);

                batch_queue.pre_acquire_resources(self.processor.clone(), self.msg_tx.clone());
            }
            PreAdd::AddAndProcessAfter(duration) => {
                batch_queue.push(item);

                batch_queue.process_after(duration, self.msg_tx.clone());
            }
            PreAdd::Add => {
                batch_queue.push(item);
            }
            PreAdd::Reject(reason) => {
                if item
                    .tx
                    .send((Err(BatchError::Rejected(reason)), None))
                    .is_err()
                {
                    // Whatever was waiting for the output must have shut down. Presumably it
                    // doesn't care anymore, but we log here anyway. There's not much else we can do
                    // here.
                    debug!("Unable to send output over oneshot channel. Receiver deallocated.");
                }
            }
        }
    }

    fn process_generation(&mut self, key: K, generation: Generation) {
        let batch_queue = self.batch_queues.get_mut(&key).expect("batch should exist");

        if let Some(batch) = batch_queue.take_generation(generation) {
            let on_finished = self.msg_tx.clone();

            batch.process(self.processor.clone(), on_finished);
        }
    }

    fn process_next_batch(&mut self, key: &K) {
        let batch_queue = self
            .batch_queues
            .get_mut(key)
            .expect("Batch queue should exist for key");

        if let Some(batch) = batch_queue.take_next_batch() {
            let on_finished = self.msg_tx.clone();

            batch.process(self.processor.clone(), on_finished);
        }
    }

    fn on_batch_finished(&mut self, key: &K) {
        let batch_queue = self.batch_queues.get_mut(key).expect("batch should exist");

        match self.batching_policy.post_finish(batch_queue) {
            PostFinish::Process => {
                self.process_next_batch(key);
            }
            PostFinish::DoNothing => {}
        }
    }

    /// Start running the worker event loop.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(item) = self.item_rx.recv() => {
                    self.add(item);
                }

                Some(msg) = self.msg_rx.recv() => {
                    match msg {
                        Message::Process(key, generation) => {
                            self.process_generation(key, generation);
                        }
                        Message::Finished(key) => {
                            self.on_batch_finished(&key);
                        }
                    }
                }
            }
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        // TODO: this is too late really.
        // Ideally we'd signal to the worker to stop, wait for it,
        // then drop the Batcher with the tx side of the channel.
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

    impl Processor<String, String, String> for SimpleBatchProcessor {
        async fn acquire_resources(&self, _key: String) -> () {
            ()
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
        let (_worker_handle, item_tx) = Worker::<String, _, _, _, _>::spawn(
            SimpleBatchProcessor,
            Limits::default().max_batch_size(2),
            BatchingPolicy::Size,
        );

        let rx1 = {
            let (tx, rx) = oneshot::channel();
            item_tx
                .send(BatchItem {
                    key: "K1".to_string(),
                    input: "I1".to_string(),
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
