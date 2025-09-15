use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use batch_aint_one::{Batcher, BatchingPolicy, Limits, Processor};
use futures::{future::join_all, lock::Mutex};

#[derive(Debug, Clone)]
pub struct ResourceAcquiringProcessor {
    acquisition_dur: Duration,
    processing_dur: Duration,

    resource_count: Arc<AtomicUsize>,
    batches: Arc<Mutex<HashMap<String, Vec<usize>>>>,
}

impl ResourceAcquiringProcessor {
    pub fn new(acquisition_dur: Duration, processing_dur: Duration) -> Self {
        Self {
            acquisition_dur,
            processing_dur,
            resource_count: Arc::new(AtomicUsize::new(0)),
            batches: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Processor<String, String, String, String, String> for ResourceAcquiringProcessor {
    async fn acquire_resources(&self, key: String) -> String {
        tokio::time::sleep(self.acquisition_dur).await;
        let count = self.resource_count.fetch_add(1, Ordering::SeqCst);
        key + "_" + &count.to_string()
    }

    async fn process(
        &self,
        key: String,
        inputs: impl Iterator<Item = String> + Send,
        resources: String,
    ) -> Result<Vec<String>, String> {
        tokio::time::sleep(self.processing_dur).await;

        let outputs: Vec<String> = inputs
            .map(|s| {
                "Item ".to_string()
                    + &s
                    + " processed for "
                    + &key
                    + " with resources "
                    + &resources
            })
            .collect();

        let mut batches = self.batches.lock().await;
        batches.entry(key.clone()).or_default().push(outputs.len());

        Ok(outputs)
    }
}

/// Given we acquire resources before processing
/// When we use an Immediate batching strategy
/// Then items should continue to be added to the batch while resources are being acquired
#[tokio::test]
async fn strategy_duration() {
    tokio::time::pause();

    let acquisition_dur = Duration::from_millis(100);
    let processing_dur = Duration::from_millis(5);

    let processor = ResourceAcquiringProcessor::new(acquisition_dur, processing_dur);

    let batcher = Batcher::new(
        processor.clone(),
        Limits::default().max_batch_size(10).max_key_concurrency(2),
        BatchingPolicy::Immediate,
    );

    let handler = |i: i32| {
        let f = batcher.add("key".to_string(), i.to_string());
        async move { f.await.unwrap() }
    };

    let mut tasks = vec![];
    for i in 1..=20 {
        tasks.push(tokio_test::task::spawn(handler(i)));
    }

    let outputs = join_all(tasks.into_iter()).await;

    assert_eq!(
        outputs.first().unwrap(),
        "Item 1 processed for key with resources key_0"
    );
    assert_eq!(
        outputs.last().unwrap(),
        "Item 20 processed for key with resources key_1"
    );

    let batches = processor.batches.lock().await;
    let batch_sizes = batches.get("key").unwrap();
    assert_eq!(batch_sizes.len(), 2);
    assert_eq!(batch_sizes[0], 10);
    assert_eq!(batch_sizes[1], 10);
}
