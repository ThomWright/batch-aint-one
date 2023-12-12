use std::{collections::HashMap, hash::Hash, sync::Arc};

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    batch::{Batch, BatchItem, Generation, GenerationalBatch},
    batcher::Processor,
    batching::{BatchingResult, BatchingStrategy},
    BatchError,
};

pub(crate) struct Worker<K, I, O, F> {
    /// Used to receive new batch items.
    item_rx: mpsc::Receiver<BatchItem<K, I, O>>,
    /// The callback to process a batch of inputs.
    processor: F,

    /// Used to signal that a batch for key `K` should be processed.
    process_tx: mpsc::Sender<(K, Generation)>,
    /// Receives signals to process a batch for key `K`.
    process_rx: mpsc::Receiver<(K, Generation)>,

    /// Controls when to start processing a batch.
    batching_strategy: BatchingStrategy,

    /// Unprocessed batches, grouped by key `K`.
    batches: HashMap<K, GenerationalBatch<K, I, O>>,
}

#[derive(Debug)]
pub(crate) struct WorkerHandle<K, I, O> {
    handle: Arc<JoinHandle<()>>,
    tx: mpsc::Sender<BatchItem<K, I, O>>,
}

impl<K, I, O, F> Worker<K, I, O, F>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    F: 'static + Send + Clone + Processor<K, I, O>,
{
    pub fn spawn(processor: F, batching_strategy: BatchingStrategy) -> WorkerHandle<K, I, O> {
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

        WorkerHandle {
            handle: Arc::new(handle),
            tx: item_tx,
        }
    }

    /// Add an item to the batch.
    fn add(&mut self, item: BatchItem<K, I, O>) {
        let key = item.key.clone();

        let batch = self
            .batches
            .entry(key.clone())
            .or_insert_with(|| GenerationalBatch::Batch(Batch::new(key.clone(), None)));

        let batch_inner: &mut Batch<K, I, O> = batch.add_item(item);

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

impl<K, I, O> WorkerHandle<K, I, O> {
    pub async fn send(&self, value: BatchItem<K, I, O>) -> Result<(), BatchError> {
        Ok(self.tx.send(value).await?)
    }
}

impl<K, I, O> Drop for WorkerHandle<K, I, O> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl<K, I, O> Clone for WorkerHandle<K, I, O> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            tx: self.tx.clone(),
        }
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
        ) -> Vec<String> {
            inputs.map(|s| s + " processed").collect()
        }
    }

    #[tokio::test]
    async fn simple_test_over_channel() {
        let worker_handle =
            Worker::<String, _, _, _>::spawn(SimpleBatchProcessor, BatchingStrategy::Size(2));

        let rx1 = {
            let (tx, rx) = oneshot::channel();
            worker_handle
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
            worker_handle
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

        let o1 = rx1.await.unwrap();
        let o2 = rx2.await.unwrap();

        assert_eq!(o1, "I1 processed".to_string());
        assert_eq!(o2, "I2 processed".to_string());
    }
}
