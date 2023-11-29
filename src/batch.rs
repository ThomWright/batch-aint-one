use std::time::Duration;

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use crate::worker::BatchItem;

#[derive(Debug)]
pub struct Batch<K, I, O> {
    key: K,
    items: Vec<BatchItem<K, I, O>>,

    processing: bool,
    timeout_deadline: Option<Instant>,
    timeout_handle: Option<JoinHandle<()>>,
}

impl<K, I, O> Batch<K, I, O> {
    pub(crate) fn new(key: K) -> Self {
        Self {
            key,
            items: Vec::default(),

            processing: false,
            timeout_deadline: None,
            timeout_handle: None,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn add_item(&mut self, item: BatchItem<K, I, O>) {
        self.items.push(item);
    }

    pub(crate) fn unzip_items(self) -> (Vec<I>, Vec<oneshot::Sender<O>>) {
        self.items
            .into_iter()
            .map(|item| (item.input, item.tx))
            .unzip()
    }

    pub(crate) fn cancel_timeout(&mut self) {
        if let Some(handle) = self.timeout_handle.take() {
            handle.abort();
        }
    }

    /// Returns whether this batch should be processed.
    pub(crate) fn start_processing(&mut self) -> bool {
        self.cancel_timeout();

        if self.processing {
            false
        } else {
            self.processing = true;
            true
        }
    }
}

impl<K, I, O> Batch<K, I, O>
where
    K: 'static + Send + Clone,
{
    pub fn key(&self) -> K {
        self.key.clone()
    }

    pub(crate) fn time_out_after(&mut self, duration: Duration, timeout_tx: &mpsc::Sender<K>) {
        let new_deadline = Instant::now() + duration;

        if let Some(current_deadline) = self.timeout_deadline {
            if new_deadline < current_deadline {
                self.cancel_timeout();
                self.timeout_deadline = Some(new_deadline);

                let key = self.key();
                let tx = timeout_tx.clone();

                let new_handle = tokio::spawn(async move {
                    tokio::time::sleep_until(new_deadline).await;

                    // FIXME: handle error
                    tx.send(key).await.unwrap_or_else(|_| panic!("TODO: fix"));
                });

                self.timeout_handle = Some(new_handle);
            }
        }
    }
}
