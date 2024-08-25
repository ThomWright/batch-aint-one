use std::time::Duration;

use batch_aint_one::{Batcher, BatchingPolicy, Limits, OnFull};
use futures::future::join_all;
use tokio::time::Instant;

use crate::{assert_elapsed, types::SimpleBatchProcessor};

/// Given we use a Duration strategy
/// When we process one item
/// Then it should take as long as the batching duration + the processing duration
#[tokio::test]
async fn strategy_duration() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(30);
    let batching_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(10),
        BatchingPolicy::Duration(batching_dur, OnFull::Process),
    );

    let now = Instant::now();

    let handler = || async {
        let now = Instant::now();

        batcher.add("A".to_string(), "1".to_string()).await.unwrap();

        now.elapsed()
    };

    let h1 = tokio_test::task::spawn(handler());

    h1.await;

    assert_elapsed!(now, batching_dur + processing_dur, Duration::from_millis(2));
}

/// Given we use a Duration strategy
/// When we submit more items at once than the maximum batch size
///  And we process when full
/// Then they should all succeed
#[tokio::test]
async fn strategy_duration_loaded_process_on_full() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(10),
        BatchingPolicy::Duration(Duration::from_millis(10), OnFull::Process),
    );

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

/// Given we use a Duration strategy
/// When we submit more items at once than the maximum batch size
///  And we reject when full
/// Then only one batch should succeed, the rest should get rejected
#[tokio::test]
async fn strategy_duration_loaded_reject_on_full() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_key_concurrency(1).max_batch_size(10),
        BatchingPolicy::Duration(Duration::from_millis(10), OnFull::Reject),
    );

    let handler = |i: i32| {
        let f = batcher.add("key".to_string(), i.to_string());
        f
    };

    let mut tasks = vec![];
    for i in 1..=100 {
        tasks.push(tokio_test::task::spawn(handler(i)));
    }

    let outputs = join_all(tasks.into_iter()).await;

    let (ok, err): (Vec<bool>, Vec<bool>) = outputs
        .iter()
        .map(|item| item.is_ok())
        .partition(|item| *item);

    assert_eq!(ok.len(), 10);
    assert_eq!(err.len(), 90);
}
