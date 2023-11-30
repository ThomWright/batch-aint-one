use std::{collections::HashMap, hash::Hash};

use tokio::sync::mpsc;

use crate::{
    batch::{Batch, BatchItem, Generation, GenerationalBatch},
    batcher::Processor,
    limit::{LimitResult, Limits},
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
    limits: Limits<K, I, O>,

    /// Unprocessed batches, grouped by key `K`.
    batches: HashMap<K, GenerationalBatch<K, I, O>>,
}

impl<K, I, O, F> Worker<K, I, O, F>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    F: 'static + Send + Clone + Processor<I, O>,
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
    fn add(&mut self, item: BatchItem<K, I, O>) {
        let key = item.key.clone();

        let batch = self
            .batches
            .entry(key.clone())
            .or_insert_with(|| GenerationalBatch::Batch(Batch::new(key.clone(), 0)));

        let batch_inner = batch.add_item(item);

        for limit in self.limits.iter() {
            let limit_result = limit.limit(batch_inner);
            match limit_result {
                LimitResult::Process => {
                    let generation = batch_inner.generation();
                    let processor = self.processor.clone();

                    Self::process(processor, batch, generation);

                    return;
                }

                LimitResult::ProcessAfter(duration) => {
                    batch_inner.time_out_after(duration, self.process_tx.clone());
                }

                LimitResult::DoNothing => {}
            };
        }
    }

    fn process(processor: F, batch: &mut GenerationalBatch<K, I, O>, generation: Generation) {
        // Only process this batch if it's the correct generation.
        if let Some(batch) = batch.take_batch(generation) {
            let (inputs, txs) = batch.start_processing();

            // Spawn a new task so we can process multiple batches concurrently,
            // without blocking the run loop.
            tokio::spawn(async move {
                let outputs = processor.process(inputs.into_iter()).await;

                for (tx, output) in txs.into_iter().zip(outputs) {
                    // FIXME: handle error
                    tx.send(output).unwrap_or_else(|_| panic!("TODO: fix"));
                }
            });
        }
    }

    fn handle_timeout(&mut self, key: K, generation: Generation) {
        let processor = self.processor.clone();
        let batch = self.batches.get_mut(&key).expect("batch should exist");

        Self::process(processor, batch, generation);
    }

    /// Start running the worker event loop.
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(item) = self.item_rx.recv() => {
                    self.add(item);
                }

                Some((key, generation)) = self.process_rx.recv() => {
                    self.handle_timeout(key, generation);
                }
            }
        }
    }
}
