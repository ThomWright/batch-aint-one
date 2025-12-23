use std::sync::{Arc, atomic::AtomicUsize};

use tokio::sync::{Mutex, futures::OwnedNotified};
use tracing::Span;

use crate::{Processor, batch::BatchItem};

#[derive(Clone)]
pub(super) struct TestProcessor;

impl Processor for TestProcessor {
    type Key = String;
    type Input = String;
    type Output = String;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: String) -> Result<(), String> {
        Ok(())
    }

    async fn process(
        &self,
        _key: String,
        inputs: impl Iterator<Item = String> + Send,
        _resources: (),
    ) -> Result<Vec<String>, String> {
        Ok(inputs.collect())
    }
}

#[derive(Default, Clone)]
pub(super) struct ControlledProcessor {
    // We control when acquire_resources completes by holding locks.
    pub(super) acquire_locks: Vec<Arc<Mutex<()>>>,
    pub(super) acquire_counter: Arc<AtomicUsize>,
}

impl Processor for ControlledProcessor {
    type Key = ();
    type Input = OwnedNotified;
    type Output = ();
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: ()) -> Result<(), String> {
        let n = self
            .acquire_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if let Some(lock) = self.acquire_locks.get(n) {
            let _guard = lock.lock().await;
        }
        Ok(())
    }

    async fn process(
        &self,
        _key: (),
        inputs: impl Iterator<Item = OwnedNotified> + Send,
        _resources: (),
    ) -> Result<Vec<()>, String> {
        let mut outputs = vec![];
        for item in inputs {
            item.await;
            outputs.push(());
        }
        Ok(outputs)
    }
}

pub(super) fn new_item<P: Processor>(key: P::Key, input: P::Input) -> BatchItem<P> {
    let (tx, _rx) = tokio::sync::oneshot::channel();
    BatchItem {
        key,
        input,
        submitted_at: tokio::time::Instant::now(),
        tx,
        requesting_span: Span::none(),
    }
}
