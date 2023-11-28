use std::{collections::HashMap, hash::Hash, sync::Arc};

use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};

use crate::batcher::{BatchFn, BatchLimit};

pub struct Worker<K, I, O, F> {
    state: Arc<Mutex<State<K, I, O>>>,
    item_rx: mpsc::Receiver<BatchItem<K, I, O>>,
    f: F,

    timeout_tx: mpsc::Sender<K>,
    timeout_rx: mpsc::Receiver<K>,

    limits: Vec<BatchLimit>,
}

#[derive(Debug)]
pub struct BatchItem<K, I, O> {
    pub key: K,
    pub input: I,
    pub tx: oneshot::Sender<O>,
}

/// Unprocessed batches, grouped by `K`.
#[derive(Debug)]
struct State<K, I, O> {
    batches: HashMap<K, Batch<K, I, O>>,
}

#[derive(Debug)]
struct Batch<K, I, O> {
    items: Vec<BatchItem<K, I, O>>,
    sleep_handle: Option<JoinHandle<()>>,
}

impl<K, I, O, F> Worker<K, I, O, F>
where
    K: Send + Sync + 'static + Eq + Hash + Clone,
    I: Send + Sync + 'static,
    O: Send + 'static,
    F: BatchFn<I, O> + Send + Sync + 'static,
{
    pub fn spawn(
        batch_fn: F,
        limits: impl Iterator<Item = BatchLimit>,
    ) -> mpsc::Sender<BatchItem<K, I, O>> {
        let (item_tx, item_rx) = mpsc::channel(10);

        let (timeout_tx, timeout_rx) = mpsc::channel(10);

        let mut worker = Worker {
            state: Arc::new(Mutex::new(State {
                batches: HashMap::new(),
            })),
            item_rx,
            f: batch_fn,

            timeout_tx,
            timeout_rx,

            limits: limits.collect(),
        };

        tokio::spawn(async move {
            worker.run().await;
        });

        item_tx
    }

    /// Add an item to the batch.
    async fn add(&self, item: BatchItem<K, I, O>) {
        let mut state = self.state.lock().await;

        let key = item.key.clone();

        let batch = state.batches.entry(key.clone()).or_default();
        batch.items.push(item);

        for limit in self.limits.iter() {
            match limit {
                BatchLimit::Size(max_size) => {
                    if batch.items.len() >= *max_size {
                        let mut batch = state
                            .batches
                            .remove(&key)
                            .expect("batch should exist (we just used it)");

                        if let Some(handle) = batch.sleep_handle.take() {
                            handle.abort();
                        }

                        drop(state);

                        self.execute(batch).await;

                        return;
                    }
                }
                BatchLimit::Duration(duration) => {
                    if batch.items.len() == 1 {
                        let key = key.clone();
                        let dur = *duration;
                        let tx = self.timeout_tx.clone();

                        let handle = tokio::spawn(async move {
                            tokio::time::sleep(dur).await;

                            // FIXME: handle error
                            tx.send(key).await.unwrap_or_else(|_| panic!("TODO: fix"));
                        });

                        batch.sleep_handle = Some(handle);
                    }
                }
            }
        }
    }

    // TODO: execute different keys concurrently, in the background
    async fn execute(&self, batch: Batch<K, I, O>) {
        let (inputs, txs): (Vec<I>, Vec<oneshot::Sender<O>>) = batch
            .items
            .into_iter()
            .map(|item| (item.input, item.tx))
            .unzip();

        let results = self.f.process_batch(inputs.into_iter()).await;

        for (tx, output) in txs.into_iter().zip(results) {
            // FIXME: handle error
            tx.send(output).unwrap_or_else(|_| panic!("TODO: fix"));
        }
    }

    async fn handle_timeout(&self, key: K) {
        let mut state = self.state.lock().await;

        let mut batch = state.batches.remove(&key).expect("batch should exist");

        if let Some(handle) = batch.sleep_handle.take() {
            handle.abort();
        }

        drop(state);

        self.execute(batch).await;
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(item) = self.item_rx.recv() => {
                    self.add(item).await;
                }

                Some(key) = self.timeout_rx.recv() => {
                    self.handle_timeout(key).await;
                }
            }
        }
    }
}

impl<K, I, O> Default for Batch<K, I, O> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            sleep_handle: None,
        }
    }
}
