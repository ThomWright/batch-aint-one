use std::{collections::HashMap, fmt::Display, hash::Hash};

use tokio::{sync::mpsc, task::JoinHandle};
use tracing::debug;

use crate::{
    batch::{Batch, BatchItem, Generation},
    batcher::Processor,
    policies::{BatchingPolicy, Limits, PostFinish, PreAdd},
    BatchError,
};

pub(crate) struct Worker<K, I, O, F, E: Display> {
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
    batches: HashMap<K, Batch<K, I, O, E>>,
}

pub(crate) enum Message<K> {
    Process(K, Generation),
    Finished(K),
}

#[derive(Debug)]
pub(crate) struct WorkerHandle {
    handle: JoinHandle<()>,
}

impl<K, I, O, F, E> Worker<K, I, O, F, E>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    F: 'static + Send + Clone + Processor<K, I, O, E>,
    E: 'static + Send + Clone + Display,
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

            batches: HashMap::new(),
        };

        let handle = tokio::spawn(async move {
            worker.run().await;
        });

        (WorkerHandle { handle }, item_tx)
    }

    /// Add an item to the batch.
    fn add(&mut self, item: BatchItem<K, I, O, E>) {
        let key = item.key.clone();

        let batch = self
            .batches
            .entry(key.clone())
            .or_insert_with(|| Batch::new(key.clone()));

        match self.batching_policy.pre_add(&self.limits, batch) {
            PreAdd::AddAndProcess => {
                batch.push(item);

                let generation = batch.generation();

                self.process_generation(key, generation);
            }
            PreAdd::AddAndProcessAfter(duration) => {
                batch.push(item);

                batch.time_out_after(duration, self.msg_tx.clone());
            }
            PreAdd::Add => {
                batch.push(item);
            }
            PreAdd::Reject(reason) => {
                if item.tx.send(Err(BatchError::Rejected(reason))).is_err() {
                    // Whatever was waiting for the output must have shut down. Presumably it
                    // doesn't care anymore, but we log here anyway. There's not much else we can do
                    // here.
                    debug!("Unable to send output over oneshot channel. Receiver deallocated.");
                }
            }
        }
    }

    fn process_generation(&mut self, key: K, generation: Generation) {
        let batch = self.batches.get_mut(&key).expect("batch should exist");

        if let Some(batch) = batch.take_generation_for_processing(generation) {
            let on_finished = self.msg_tx.clone();

            batch.process(self.processor.clone(), on_finished);
        }
    }

    fn on_batch_finished(&mut self, key: K) {
        let batch = self.batches.get_mut(&key).expect("batch should exist");

        match self.batching_policy.post_finish(&self.limits, batch) {
            PostFinish::Process => {
                if let Some(batch) = batch.take_batch_for_processing() {
                    let on_finished = self.msg_tx.clone();

                    batch.process(self.processor.clone(), on_finished);
                }
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
                            self.on_batch_finished(key);
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
    use async_trait::async_trait;
    use tokio::sync::oneshot;

    use super::*;

    #[derive(Debug, Clone)]
    struct SimpleBatchProcessor;

    #[async_trait]
    impl Processor<String, String, String> for SimpleBatchProcessor {
        async fn process(
            &self,
            _key: String,
            inputs: impl Iterator<Item = String> + Send,
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
                    span_id: None,
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
                    span_id: None,
                })
                .await
                .unwrap();

            rx
        };

        let o1 = rx1.await.unwrap().unwrap();
        let o2 = rx2.await.unwrap().unwrap();

        assert_eq!(o1, "I1 processed".to_string());
        assert_eq!(o2, "I2 processed".to_string());
    }
}
