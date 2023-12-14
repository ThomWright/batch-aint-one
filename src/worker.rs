use std::{collections::HashMap, fmt::Display, hash::Hash};

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    batch::{Batch, BatchItem, Generation, GenerationalBatch},
    batcher::Processor,
    batching::{BatchingResult, BatchingStrategy},
};

pub(crate) struct Worker<K, I, O, F, E> {
    /// Used to receive new batch items.
    item_rx: mpsc::Receiver<BatchItem<K, I, O, E>>,
    /// The callback to process a batch of inputs.
    processor: F,

    /// Used to signal that a batch for key `K` should be processed.
    process_tx: mpsc::Sender<(K, Generation)>,
    /// Receives signals to process a batch for key `K`.
    process_rx: mpsc::Receiver<(K, Generation)>,

    /// Controls when to start processing a batch.
    batching_strategy: BatchingStrategy,

    /// Unprocessed batches, grouped by key `K`.
    batches: HashMap<K, GenerationalBatch<K, I, O, E>>,
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
        batching_strategy: BatchingStrategy,
    ) -> (WorkerHandle, mpsc::Sender<BatchItem<K, I, O, E>>) {
        let (item_tx, item_rx) = mpsc::channel(10);

        let (timeout_tx, timeout_rx) = mpsc::channel(10);

        let mut worker = Worker {
            item_rx,
            processor,

            process_tx: timeout_tx,
            process_rx: timeout_rx,

            batching_strategy,

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
            .or_insert_with(|| GenerationalBatch::Batch(Batch::new(key.clone(), None)));

        let batch_inner: &mut Batch<K, I, O, E> = batch.add_item(item);

        match self.batching_strategy.apply(batch_inner) {
            BatchingResult::Process => {
                let generation = batch_inner.generation();

                self.process(key, generation);
            }
            BatchingResult::ProcessAfter(duration) => {
                batch_inner.time_out_after(duration, self.process_tx.clone());
            }
            BatchingResult::DoNothing => {}
        }
    }

    fn process(&mut self, key: K, generation: Generation) {
        let batch = self.batches.get_mut(&key).expect("batch should exist");

        if let Some(batch) = batch.take_batch_for_processing(generation) {
            let process_next = self
                .batching_strategy
                .is_sequential()
                .then(|| self.process_tx.clone());

            batch.process(self.processor.clone(), process_next);
        }
    }

    /// Start running the worker event loop.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(item) = self.item_rx.recv() => {
                    self.add(item);
                }

                Some((key, generation)) = self.process_rx.recv() => {
                    self.process(key, generation);
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
        let (_worker_handle, item_tx) =
            Worker::<String, _, _, _, _>::spawn(SimpleBatchProcessor, BatchingStrategy::Size(2));

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
