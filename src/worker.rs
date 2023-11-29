use std::{collections::HashMap, hash::Hash};

use tokio::sync::{mpsc, oneshot};

use crate::{
    batch::Batch,
    batcher::BatchFn,
    limit::{LimitResult, Limits},
};

pub struct Worker<K, I, O, F> {
    /// Used to receive new batch items.
    item_rx: mpsc::Receiver<BatchItem<K, I, O>>,
    /// The callback to process a batch of inputs.
    processor: F,

    /// Used to signal that a batch for key `K` should be processed.
    process_tx: mpsc::Sender<K>,
    /// Receives signals to process a batch for key `K`.
    process_rx: mpsc::Receiver<K>,

    /// Controls when to start processing a batch.
    limits: Limits<K, I, O>,

    /// Unprocessed batches, grouped by key `K`.
    batches: HashMap<K, Batch<K, I, O>>,
}

#[derive(Debug)]
pub struct BatchItem<K, I, O> {
    pub key: K,
    pub input: I,
    pub tx: oneshot::Sender<O>,
}

impl<K, I, O, F> Worker<K, I, O, F>
where
    K: 'static + Send + Sync + Eq + Hash + Clone,
    I: 'static + Send + Sync,
    O: 'static + Send,
    F: 'static + Send + Sync + Clone + BatchFn<I, O>,
{
    pub fn spawn(processor: F, limits: Limits<K, I, O>) -> mpsc::Sender<BatchItem<K, I, O>> {
        let (item_tx, item_rx) = mpsc::channel(10);

        let (timeout_tx, timeout_rx) = mpsc::channel(10);

        let mut worker = Worker {
            item_rx,
            processor,

            process_tx: timeout_tx,
            process_rx: timeout_rx,

            limits,

            batches: HashMap::new(),
        };

        tokio::spawn(async move {
            worker.run().await;
        });

        item_tx
    }

    /// Add an item to the batch.
    async fn add(&mut self, item: BatchItem<K, I, O>) {
        let key = item.key.clone();

        let batch = self
            .batches
            .entry(key.clone())
            .or_insert_with(|| Batch::new(key.clone()));

        batch.add_item(item);

        for limit in self.limits.iter() {
            let limit_result = limit.limit(batch);
            match limit_result {
                LimitResult::Process => {
                    let batch = self
                        .batches
                        .remove(&key)
                        .expect("batch should exist (we just used it)");

                    self.process(batch).await;

                    return;
                }

                LimitResult::ProcessAfter(duration) => {
                    batch.time_out_after(duration, &self.process_tx);
                }

                LimitResult::DoNothing => {}
            };
        }
    }

    async fn process(&self, mut batch: Batch<K, I, O>) {
        if batch.start_processing() {
            let processor = self.processor.clone();

            // Spawn a new task so we can process multiple batches concurrently,
            // without blocking the run loop.
            tokio::spawn(async move {
                let (inputs, txs): (Vec<I>, Vec<oneshot::Sender<O>>) = batch.unzip_items();

                let outputs = processor.process_batch(inputs.into_iter()).await;

                for (tx, output) in txs.into_iter().zip(outputs) {
                    // FIXME: handle error
                    tx.send(output).unwrap_or_else(|_| panic!("TODO: fix"));
                }
            });
        }
    }

    async fn handle_timeout(&mut self, key: K) {
        let batch = self.batches.remove(&key).expect("batch should exist");

        self.process(batch).await;
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(item) = self.item_rx.recv() => {
                    self.add(item).await;
                }

                Some(key) = self.process_rx.recv() => {
                    self.handle_timeout(key).await;
                }
            }
        }
    }
}
