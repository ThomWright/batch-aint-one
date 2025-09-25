use std::time::Duration;

use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tracing::debug;

use crate::{batch_inner::Generation, processor::Processor, worker::Message};

#[derive(Debug)]
pub(crate) struct TimeoutHandle<P: Processor> {
    key: P::Key,
    generation: Generation,
    deadline: Option<Instant>,
    handle: Option<JoinHandle<()>>,
    _phantom: std::marker::PhantomData<P>,
}

impl<P: Processor> TimeoutHandle<P> {
    pub fn new(key: P::Key, generation: Generation) -> Self {
        Self {
            key,
            generation,
            deadline: None,
            handle: None,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn is_ready_for_processing(&self) -> bool {
        self.deadline
            .is_none_or(|deadline| deadline <= Instant::now())
    }

    pub fn is_expired(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| deadline <= Instant::now())
    }

    pub fn set_timeout(&mut self, duration: Duration, tx: mpsc::Sender<Message<P::Key, P::Error>>) {
        self.cancel();

        let new_deadline = Instant::now() + duration;
        self.deadline = Some(new_deadline);

        let key = self.key.clone();
        let generation = self.generation;
        let new_handle = tokio::spawn(async move {
            tokio::time::sleep_until(new_deadline).await;

            if tx.send(Message::Process(key, generation)).await.is_err() {
                // The worker must have shut down. In this case, we don't want to process any more
                // batches anyway.
                debug!("A batch reached a timeout but the worker has shut down");
            }
        });

        self.handle = Some(new_handle);
    }

    pub fn cancel(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        self.deadline = None;
    }
}

impl<P: Processor> Drop for TimeoutHandle<P> {
    fn drop(&mut self) {
        self.cancel();
    }
}
