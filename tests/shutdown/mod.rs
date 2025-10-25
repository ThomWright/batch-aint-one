use std::time::Duration;

use batch_aint_one::{Batcher, BatchingPolicy, Limits, Processor};
use futures::future::join_all;
use rstest::rstest;
use tokio::sync::mpsc;

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
