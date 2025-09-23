use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use assert_matches::assert_matches;
use batch_aint_one::{Batcher, BatchingPolicy, Limits, Processor};
use futures::future::join_all;
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Debug, Clone)]
pub struct LockingResourceProcessor {
    acquisition_lock: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    acquisition_dur: Duration,
    processing_dur: Duration,

    batches: Arc<Mutex<HashMap<String, Vec<usize>>>>,
}

impl LockingResourceProcessor {
    fn new(acquisition_dur: Duration, processing_dur: Duration) -> Self {
        Self {
            acquisition_lock: Arc::new(Mutex::new(HashMap::new())),
            acquisition_dur,
            processing_dur,
            batches: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Processor for LockingResourceProcessor {
    type Key = String;
    type Input = String;
    type Output = String;
    type Error = String;
    type Resources = OwnedMutexGuard<()>;

    async fn acquire_resources(&self, key: String) -> Result<OwnedMutexGuard<()>, String> {
        let lock = {
            let map_lock = self.acquisition_lock.clone();
            let mut map = map_lock.lock().await;
            let entry = map
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            entry.clone().lock_owned().await
        };

        tokio::time::sleep(self.acquisition_dur).await;

        Ok(lock)
    }

    async fn process(
        &self,
        key: String,
        inputs: impl Iterator<Item = String> + Send,
        _resources: OwnedMutexGuard<()>,
    ) -> Result<Vec<String>, String> {
        tokio::time::sleep(self.processing_dur).await;

        let outputs: Vec<String> = inputs
            .map(|s| "Item ".to_string() + &s + " processed for " + &key)
            .collect();

        let mut batches = self.batches.lock().await;
        batches.entry(key.clone()).or_default().push(outputs.len());

        Ok(outputs)
    }
}

/// Given we use an Immediate batching strategy
/// When the resource acquisition acquires a lock on the key
/// Then items should continue to be added to the batch while resources are being acquired
#[tokio::test]
async fn immediate_resource_locking() {
    tokio::time::pause();

    let acquisition_dur = Duration::from_millis(1000);
    let processing_dur = Duration::from_millis(5);

    let processor = LockingResourceProcessor::new(acquisition_dur, processing_dur);

    let batcher = Batcher::builder()
        .name("test_immediate_batches_while_acquiring")
        .processor(processor.clone())
        .limits(
            Limits::default()
                .with_max_batch_size(5)
                .with_max_key_concurrency(2),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let handler = |i: i32| {
        let f = batcher.add("key".to_string(), i.to_string());
        async move { f.await }
    };

    let mut tasks = vec![];
    for i in 1..=10 {
        tasks.push(tokio_test::task::spawn(handler(i)));
    }

    let outputs = join_all(tasks.into_iter()).await;
    let mut failed_outputs = outputs.iter().filter(|r| r.is_err());
    assert_matches!(failed_outputs.next(), None);
}
