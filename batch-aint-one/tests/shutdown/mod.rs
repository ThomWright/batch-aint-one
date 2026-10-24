use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use batch_aint_one::{Batcher, BatchingPolicy, Limits, OnFull, Processor};
use futures::future::join_all;
use rstest::rstest;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{assert_elapsed, types::SimpleBatchProcessor};

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn shut_down_when_last_batcher_dropped() {
    tokio::time::pause();

    let batcher = Batcher::builder()
        .name("shut_down_when_last_batcher_dropped")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::builder().max_batch_size(3).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let worker = batcher.worker_handle();
    let shut_down = tokio_test::task::spawn(async move {
        worker.wait_for_shutdown().await;
    });

    drop(batcher);

    tokio::time::timeout(Duration::from_secs(1), shut_down)
        .await
        .expect("Worker should shut down");
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn shut_down_when_shut_down_called() {
    tokio::time::pause();

    let batcher = Batcher::builder()
        .name("shut_down_when_shut_down_called")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::builder().max_batch_size(3).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let worker = batcher.worker_handle();

    let shut_down = {
        let worker = worker.clone();
        tokio_test::task::spawn(async move {
            worker.wait_for_shutdown().await;
        })
    };

    worker.shut_down().await;

    tokio::time::timeout(Duration::from_secs(1), shut_down)
        .await
        .expect("Worker should shut down");
}

#[derive(Debug, Clone)]
struct SignalingProcessor {
    processing_started_tx: mpsc::UnboundedSender<()>,
    processing_duration: Duration,
}

impl SignalingProcessor {
    fn new(
        processing_started_tx: mpsc::UnboundedSender<()>,
        processing_duration: Duration,
    ) -> Self {
        Self {
            processing_started_tx,
            processing_duration,
        }
    }
}

impl Processor for SignalingProcessor {
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
        key: String,
        inputs: impl Iterator<Item = String> + Send,
        _resources: (),
    ) -> Result<Vec<String>, String> {
        // Signal that processing has started
        let _ = self.processing_started_tx.send(());

        tokio::time::sleep(self.processing_duration).await;
        Ok(inputs.map(|s| s + " processed for " + &key).collect())
    }
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn shut_down_during_batch_processing() {
    tokio::time::pause();

    let processing_duration = Duration::from_millis(500);
    let (processing_started_tx, mut processing_started_rx) = mpsc::unbounded_channel();

    let batcher = Batcher::builder()
        .name("shutdown_during_processing")
        .processor(SignalingProcessor::new(
            processing_started_tx,
            processing_duration,
        ))
        .limits(Limits::builder().max_batch_size(1).build())
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let worker = batcher.worker_handle();

    let start = tokio::time::Instant::now();

    // Spawn the processing task
    let processing_future = tokio::spawn(async move {
        batcher
            .add("test_key".to_string(), "item".to_string())
            .await
    });

    // Wait for processing to actually start
    processing_started_rx
        .recv()
        .await
        .expect("Processing should start");

    // Now shutdown while processing is active
    worker.shut_down().await;

    // The processing should still complete
    let result = processing_future.await.expect("Task should complete");
    assert!(
        result.is_ok(),
        "In-flight batch should complete during shutdown"
    );

    // Worker should shut down gracefully
    tokio::time::timeout(Duration::from_secs(1), worker.wait_for_shutdown())
        .await
        .expect("Worker should shut down after in-flight batches complete");

    assert_elapsed!(start, processing_duration, Duration::from_millis(2));
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn idempotent_shutdown() {
    tokio::time::pause();

    let batcher = Batcher::builder()
        .name("idempotent_shutdown")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::builder().max_batch_size(3).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let worker = batcher.worker_handle();

    // Call shut_down multiple times
    let mut tasks = vec![];
    for _ in 1..=3 {
        tasks.push(tokio_test::task::spawn(worker.shut_down()));
    }

    join_all(tasks.into_iter()).await;

    // Should still shut down gracefully without issues
    tokio::time::timeout(Duration::from_secs(1), worker.wait_for_shutdown())
        .await
        .expect("Worker should shut down gracefully even with multiple shutdown calls");
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn shut_down_processes_partial_batch() {
    tokio::time::pause();

    let batcher = Batcher::builder()
        .name("shut_down_processes_partial_batch")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::builder().max_batch_size(3).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let worker = batcher.worker_handle();

    let add = tokio::spawn(async move { batcher.add("key".to_string(), "item".to_string()).await });
    // Let the worker receive the item, so it isn't idle when shutting down.
    tokio::time::sleep(Duration::from_millis(1)).await;

    worker.shut_down().await;

    let output = tokio::time::timeout(Duration::from_secs(1), add)
        .await
        .expect("The partial batch should be processed")
        .unwrap()
        .unwrap();
    assert_eq!(output, "item processed for key");

    tokio::time::timeout(Duration::from_secs(1), worker.wait_for_shutdown())
        .await
        .expect("Worker should shut down");
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn shut_down_processes_batch_before_its_deadline() {
    tokio::time::pause();

    let batcher = Batcher::builder()
        .name("shut_down_processes_batch_before_its_deadline")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::builder().max_batch_size(3).build())
        .batching_policy(BatchingPolicy::Duration(
            Duration::from_secs(10),
            OnFull::Process,
        ))
        .build();

    let worker = batcher.worker_handle();

    let start = tokio::time::Instant::now();

    let add = tokio::spawn(async move { batcher.add("key".to_string(), "item".to_string()).await });
    // Let the worker receive the item, so it isn't idle when shutting down.
    tokio::time::sleep(Duration::from_millis(1)).await;

    worker.shut_down().await;

    add.await.unwrap().unwrap();
    worker.wait_for_shutdown().await;

    assert!(
        start.elapsed() < Duration::from_secs(1),
        "The batch should be processed without waiting for its deadline, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn shut_down_respects_key_concurrency() {
    tokio::time::pause();

    let processing_duration = Duration::from_millis(100);

    let batcher = Batcher::builder()
        .name("shut_down_respects_key_concurrency")
        .processor(SimpleBatchProcessor(processing_duration))
        .limits(
            Limits::builder()
                .max_batch_size(3)
                .max_key_concurrency(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Size)
        .build();

    let worker = batcher.worker_handle();

    let start = tokio::time::Instant::now();

    // A full batch starts processing, leaving a partial batch waiting.
    let adds: Vec<_> = (1..=4)
        .map(|i| {
            let batcher = batcher.clone();
            tokio::spawn(async move { batcher.add("key".to_string(), format!("item {i}")).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(1)).await;

    worker.shut_down().await;

    for result in join_all(adds).await {
        result.unwrap().unwrap();
    }
    worker.wait_for_shutdown().await;

    // The partial batch waits for the full batch to finish.
    assert_elapsed!(start, processing_duration * 2, Duration::from_millis(2));
}

/// Counts how many resource acquisitions and batches have finished.
#[derive(Debug, Clone, Default)]
struct CountingProcessor {
    acquisition_duration: Duration,
    processing_duration: Duration,
    acquired: Arc<AtomicUsize>,
    processed: Arc<AtomicUsize>,
}

impl Processor for CountingProcessor {
    type Key = String;
    type Input = String;
    type Output = String;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: String) -> Result<(), String> {
        tokio::time::sleep(self.acquisition_duration).await;
        self.acquired.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn process(
        &self,
        _key: String,
        inputs: impl Iterator<Item = String> + Send,
        _resources: (),
    ) -> Result<Vec<String>, String> {
        tokio::time::sleep(self.processing_duration).await;
        self.processed.fetch_add(1, Ordering::SeqCst);
        Ok(inputs.collect())
    }
}

/// Cancel a caller's `add`, then drop the last `Batcher`.
async fn drop_last_batcher<P: Processor>(add: JoinHandle<()>, batcher: Batcher<P>) {
    add.abort();
    let _ = add.await;
    drop(batcher);
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn dropping_last_batcher_waits_for_processing() {
    tokio::time::pause();

    let processing_duration = Duration::from_millis(500);
    let processor = CountingProcessor {
        processing_duration,
        ..Default::default()
    };
    let processed = Arc::clone(&processor.processed);

    let batcher = Batcher::builder()
        .name("dropping_last_batcher_waits_for_processing")
        .processor(processor)
        .limits(Limits::builder().max_batch_size(1).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let worker = batcher.worker_handle();

    let start = tokio::time::Instant::now();

    let add = {
        let batcher = batcher.clone();
        tokio::spawn(async move {
            let _ = batcher.add("key".to_string(), "item".to_string()).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(1)).await;

    drop_last_batcher(add, batcher).await;
    worker.wait_for_shutdown().await;

    assert_eq!(processed.load(Ordering::SeqCst), 1);
    assert_elapsed!(start, processing_duration, Duration::from_millis(2));
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn dropping_last_batcher_waits_for_acquisition_without_processing() {
    tokio::time::pause();

    let acquisition_duration = Duration::from_millis(500);
    let processor = CountingProcessor {
        acquisition_duration,
        ..Default::default()
    };
    let acquired = Arc::clone(&processor.acquired);
    let processed = Arc::clone(&processor.processed);

    let batcher = Batcher::builder()
        .name("dropping_last_batcher_waits_for_acquisition_without_processing")
        .processor(processor)
        .limits(Limits::builder().max_batch_size(3).build())
        // Starts acquiring resources as soon as an item is added.
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let worker = batcher.worker_handle();

    let start = tokio::time::Instant::now();

    let add = {
        let batcher = batcher.clone();
        tokio::spawn(async move {
            let _ = batcher.add("key".to_string(), "item".to_string()).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(1)).await;

    drop_last_batcher(add, batcher).await;
    worker.wait_for_shutdown().await;

    assert_eq!(acquired.load(Ordering::SeqCst), 1);
    assert_eq!(
        processed.load(Ordering::SeqCst),
        0,
        "Nothing is waiting for the output, so the batch shouldn't be processed"
    );
    assert_elapsed!(start, acquisition_duration, Duration::from_millis(2));
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn dropping_last_batcher_drops_unstarted_batches() {
    tokio::time::pause();

    let processor = CountingProcessor::default();
    let acquired = Arc::clone(&processor.acquired);
    let processed = Arc::clone(&processor.processed);

    let batcher = Batcher::builder()
        .name("dropping_last_batcher_drops_unstarted_batches")
        .processor(processor)
        .limits(Limits::builder().max_batch_size(3).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let worker = batcher.worker_handle();

    let add = {
        let batcher = batcher.clone();
        tokio::spawn(async move {
            let _ = batcher.add("key".to_string(), "item".to_string()).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(1)).await;

    drop_last_batcher(add, batcher).await;
    worker.wait_for_shutdown().await;

    assert_eq!(acquired.load(Ordering::SeqCst), 0);
    assert_eq!(processed.load(Ordering::SeqCst), 0);
}
