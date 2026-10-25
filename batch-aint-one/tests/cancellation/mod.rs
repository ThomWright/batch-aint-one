//! Items whose callers stop waiting, by dropping the future returned by `Batcher::add`, before
//! their batch starts processing.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use batch_aint_one::{Batcher, BatchingPolicy, Limits, OnFull, Processor, error::BatchResult};
use tokio::{task::JoinHandle, time::Instant};

/// Records the inputs of every batch it processes.
#[derive(Debug, Clone, Default)]
struct RecordingProcessor {
    acquisition_dur: Duration,
    batches: Arc<Mutex<Vec<Vec<String>>>>,
}

impl RecordingProcessor {
    fn with_acquisition(acquisition_dur: Duration) -> Self {
        Self {
            acquisition_dur,
            ..Default::default()
        }
    }

    fn batches(&self) -> Vec<Vec<String>> {
        self.batches.lock().unwrap().clone()
    }
}

impl Processor for RecordingProcessor {
    type Key = String;
    type Input = String;
    type Output = String;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: String) -> Result<(), String> {
        tokio::time::sleep(self.acquisition_dur).await;
        Ok(())
    }

    async fn process(
        &self,
        _key: String,
        inputs: impl Iterator<Item = String> + Send,
        _resources: (),
    ) -> Result<Vec<String>, String> {
        let inputs: Vec<_> = inputs.collect();
        self.batches.lock().unwrap().push(inputs.clone());
        Ok(inputs)
    }
}

fn batcher(
    processor: &RecordingProcessor,
    limits: Limits,
    policy: BatchingPolicy,
) -> Batcher<RecordingProcessor> {
    Batcher::builder()
        .name("cancellation")
        .processor(processor.clone())
        .limits(limits)
        .batching_policy(policy)
        .build()
}

/// Add an item, and let it reach the worker.
async fn add(
    batcher: &Batcher<RecordingProcessor>,
    input: &str,
) -> JoinHandle<BatchResult<String, String>> {
    let batcher = batcher.clone();
    let input = input.to_string();
    let handle = tokio::spawn(async move { batcher.add("key".to_string(), input).await });
    tokio::time::sleep(Duration::from_millis(1)).await;
    handle
}

/// Stop waiting for an item, by dropping the future returned by `add`.
async fn cancel(handle: JoinHandle<BatchResult<String, String>>) {
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
}

/// Given we use a Size strategy
/// When some callers stop waiting before their batch is full
/// Then their items should not be processed, and batches should be full of the others
#[tokio::test]
async fn batches_fill_with_items_still_waiting() {
    tokio::time::pause();

    let processor = RecordingProcessor::default();
    let batcher = batcher(
        &processor,
        Limits::builder().max_batch_size(3).build(),
        BatchingPolicy::Size,
    );

    let first = add(&batcher, "1").await;
    let second = add(&batcher, "2").await;
    cancel(first).await;
    let third = add(&batcher, "3").await;
    assert!(
        processor.batches().is_empty(),
        "should wait for a full batch of items still waiting"
    );
    let fourth = add(&batcher, "4").await;

    for handle in [second, third, fourth] {
        handle.await.unwrap().unwrap();
    }
    assert_eq!(processor.batches(), [["2", "3", "4"]]);
}

/// Given we use an Immediate strategy
/// When a caller stops waiting while resources are being acquired for its batch
/// Then its item should not be processed
#[tokio::test]
async fn items_cancelled_during_acquisition_are_not_processed() {
    tokio::time::pause();

    let processor = RecordingProcessor::with_acquisition(Duration::from_millis(100));
    let batcher = batcher(
        &processor,
        Limits::builder().max_batch_size(10).build(),
        BatchingPolicy::Immediate,
    );

    let first = add(&batcher, "1").await;
    let second = add(&batcher, "2").await;
    cancel(second).await;

    first.await.unwrap().unwrap();
    assert_eq!(processor.batches(), [["1"]]);
}

/// Given the queue for a key is full
/// When some of its callers have stopped waiting
/// Then their items should not count against the limit
#[tokio::test]
async fn cancelled_items_do_not_count_against_the_queue_limit() {
    tokio::time::pause();

    let processor = RecordingProcessor::default();
    let batcher = batcher(
        &processor,
        Limits::builder()
            .max_batch_size(2)
            .max_key_concurrency(1)
            .max_key_queued_items(2)
            .build(),
        BatchingPolicy::Size,
    );

    let first = add(&batcher, "1").await;
    cancel(first).await;
    let second = add(&batcher, "2").await;
    let third = add(&batcher, "3").await;

    // If the cancelled item had taken a place in the queue, item 3 would be rejected, or wait
    // forever for a full batch.
    let third = tokio::time::timeout(Duration::from_secs(1), third)
        .await
        .expect("item 3 should be processed");
    assert_eq!(third.unwrap().unwrap(), "3");
    assert_eq!(second.await.unwrap().unwrap(), "2");
    assert_eq!(processor.batches(), [["2", "3"]]);
}

/// Given we use a Duration strategy
/// When the caller of the oldest item stops waiting
/// Then the next item should wait the full duration, not just what's left of the oldest one's
#[tokio::test]
async fn duration_waits_for_the_oldest_item_still_waiting() {
    tokio::time::pause();

    let processor = RecordingProcessor::default();
    let duration = Duration::from_millis(100);
    let batcher = batcher(
        &processor,
        Limits::builder().max_batch_size(10).build(),
        BatchingPolicy::Duration(duration, OnFull::Process),
    );

    let first = add(&batcher, "1").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let start = Instant::now();
    let second = add(&batcher, "2").await;
    cancel(first).await;

    second.await.unwrap().unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= duration && elapsed < duration + Duration::from_millis(5),
        "expected about {duration:?}, got {elapsed:?}"
    );
}
