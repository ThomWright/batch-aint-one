use std::time::Duration;

use batch_aint_one::{Batcher, BatchingPolicy, Limits};

use crate::types::SimpleBatchProcessor;

#[tokio::test]
async fn shut_down_when_last_batcher_dropped() {
    tokio::time::pause();

    let batcher = Batcher::builder()
        .name("test_immediate_batches_while_acquiring")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::default().with_max_batch_size(3))
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
async fn shut_down_when_shut_down_called() {
    tokio::time::pause();

    let batcher = Batcher::builder()
        .name("test_immediate_batches_while_acquiring")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::default().with_max_batch_size(3))
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
