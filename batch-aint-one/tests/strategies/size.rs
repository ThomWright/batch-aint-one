use std::time::Duration;

use assert_matches::assert_matches;
use batch_aint_one::{
    BatchError, Batcher, BatchingPolicy, Limits,
    error::{ConcurrencyStatus, RejectionReason},
};
use futures::future::join_all;
use tokio::join;

use crate::types::SimpleBatchProcessor;

/// Given we use a Size strategy
/// When we submit exactly one batch worth of items
/// Then it should process them all immediately
#[tokio::test]
async fn process_when_full() {
    let batcher = Batcher::builder()
        .name("test_process_when_full")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(Limits::builder().max_batch_size(3).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let h1 = tokio_test::task::spawn(batcher.add("A".to_string(), "1".to_string()));
    let h2 = tokio_test::task::spawn(batcher.add("A".to_string(), "2".to_string()));
    let h3 = tokio_test::task::spawn(batcher.add("A".to_string(), "3".to_string()));

    let (o1, o2, o3) = join!(h1, h2, h3);

    assert_eq!("1 processed for A".to_string(), o1.unwrap());
    assert_eq!("2 processed for A".to_string(), o2.unwrap());
    assert_eq!("3 processed for A".to_string(), o3.unwrap());
}

/// Given we use a Size strategy
/// When we submit several batches worth of items at once
/// Then they should all succeed
#[tokio::test]
async fn loaded() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_loaded")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(Limits::builder().max_batch_size(10).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let handler = |i: i32| {
        let f = batcher.add("key".to_string(), i.to_string());
        async move { f.await.unwrap() }
    };

    let mut tasks = vec![];
    for i in 1..=100 {
        tasks.push(tokio_test::task::spawn(handler(i)));
    }

    let outputs = join_all(tasks.into_iter()).await;

    assert_eq!(outputs.last().unwrap(), "100 processed for key");
}

#[tokio::test]
async fn max_concurrency_limit() {
    let batcher = Batcher::builder()
        .name("test_max_concurrency_limit")
        .processor(SimpleBatchProcessor(Duration::ZERO))
        .limits(
            Limits::builder()
                .max_batch_size(1)
                .max_key_concurrency(2)
                .max_batch_queue_size(2)
                .build(),
        )
        .batching_policy(BatchingPolicy::Size)
        .build();

    // Two processed for A immediately
    let h1 = tokio_test::task::spawn(batcher.add("A".to_string(), "1".to_string()));
    let h2 = tokio_test::task::spawn(batcher.add("A".to_string(), "2".to_string()));

    // Two processed for A after another finished
    let h3 = tokio_test::task::spawn(batcher.add("A".to_string(), "3".to_string()));
    let h4 = tokio_test::task::spawn(batcher.add("A".to_string(), "4".to_string()));

    // One rejected
    let h5 = tokio_test::task::spawn(batcher.add("A".to_string(), "5".to_string()));

    // One different key
    let h6 = tokio_test::task::spawn(batcher.add("B".to_string(), "1".to_string()));

    let (o1, o2, o3, o4, o5, o6) = join!(h1, h2, h3, h4, h5, h6);

    assert_eq!(o1.unwrap(), "1 processed for A".to_string());
    assert_eq!(o2.unwrap(), "2 processed for A".to_string());
    assert_eq!(o3.unwrap(), "3 processed for A".to_string());
    assert_eq!(o4.unwrap(), "4 processed for A".to_string());
    assert_matches!(
        o5.unwrap_err(),
        BatchError::Rejected(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
    );
    assert_eq!(o6.unwrap(), "1 processed for B".to_string());
}
