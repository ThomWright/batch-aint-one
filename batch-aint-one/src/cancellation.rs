//! Lets the worker tell when a queue might hold cancelled items, without checking each one.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::oneshot;

/// Counts items whose callers stopped waiting before their result arrived.
///
/// An item counts once its result channel is closed, so anything that reads the count and then
/// [checks items](crate::batch::BatchItem::is_cancelled) sees every item counted so far as
/// cancelled. If the count is the same as before an earlier check, nothing has been cancelled
/// since, so the check needn't be repeated. Some counted items were never queued, or have already
/// left the queue, so a change only means there might be cancelled items.
#[derive(Debug, Default)]
pub(crate) struct Cancellations(AtomicUsize);

impl Cancellations {
    pub(crate) fn count(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    fn record(&self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

/// Receives an item's result, recording a [cancellation](Cancellations) if dropped before it
/// arrives.
pub(crate) struct ResultReceiver<'a, T> {
    rx: Option<oneshot::Receiver<T>>,
    cancellations: &'a Cancellations,
}

impl<'a, T> ResultReceiver<'a, T> {
    pub(crate) fn new(rx: oneshot::Receiver<T>, cancellations: &'a Cancellations) -> Self {
        Self {
            rx: Some(rx),
            cancellations,
        }
    }

    pub(crate) async fn recv(mut self) -> Result<T, oneshot::error::RecvError> {
        let result = self.rx.as_mut().expect("only taken on drop or here").await;
        self.rx = None;
        result
    }
}

impl<T> Drop for ResultReceiver<'_, T> {
    fn drop(&mut self) {
        if let Some(rx) = self.rx.take() {
            // Closing the channel before counting means a reader that sees the count also sees
            // the item as cancelled.
            drop(rx);
            self.cancellations.record();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropping_before_the_result_arrives_records_a_cancellation() {
        let cancellations = Cancellations::default();
        let (tx, rx) = oneshot::channel::<()>();

        drop(ResultReceiver::new(rx, &cancellations));

        assert!(tx.is_closed());
        assert_eq!(cancellations.count(), 1);
    }

    #[tokio::test]
    async fn receiving_the_result_records_nothing() {
        let cancellations = Cancellations::default();
        let (tx, rx) = oneshot::channel();
        tx.send(()).unwrap();

        ResultReceiver::new(rx, &cancellations)
            .recv()
            .await
            .unwrap();

        assert_eq!(cancellations.count(), 0);
    }

    #[tokio::test]
    async fn the_worker_dropping_the_item_records_nothing() {
        let cancellations = Cancellations::default();
        let (tx, rx) = oneshot::channel::<()>();
        drop(tx);

        let result = ResultReceiver::new(rx, &cancellations).recv().await;

        assert!(result.is_err());
        assert_eq!(cancellations.count(), 0);
    }
}
