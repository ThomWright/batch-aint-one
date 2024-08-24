use std::time::Duration;

use batch_aint_one::{Batcher, BatchingPolicy, Limits};
use futures::future::join_all;
use tokio::{join, time::Instant};

use crate::{assert_duration, types::SimpleBatchProcessor};

/// Given we use a Sequential strategy with max concurrency = 1
/// When we process two items
/// Then it should process them serially, i.e. it should take twice the processing duration
#[tokio::test]
async fn strategy_sequential_single() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(10).max_key_concurrency(1),
        BatchingPolicy::Immediate,
    );

    let handler = || async {
        let now = Instant::now();

        batcher.add("A".to_string(), "1".to_string()).await.unwrap();

        now.elapsed()
    };

    let h1 = tokio_test::task::spawn(handler());
    let h2 = tokio_test::task::spawn(handler());

    let (dur1, dur2) = join!(h1, h2);

    let d = dur1.min(dur2);
    assert_duration!(d, processing_dur, std::time::Duration::from_millis(2));

    let d = dur1.max(dur2);
    assert_duration!(d, processing_dur * 2, std::time::Duration::from_millis(2));
}

/// Given we use a Sequential strategy with max concurrency = 2
/// When we process two items
/// Then it should process them concurrently
#[tokio::test]
async fn strategy_sequential_dual() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(1).max_key_concurrency(2),
        BatchingPolicy::Immediate,
    );

    let handler = || async {
        let now = Instant::now();

        batcher.add("A".to_string(), "1".to_string()).await.unwrap();

        now.elapsed()
    };

    let h1 = tokio_test::task::spawn(handler());
    let h2 = tokio_test::task::spawn(handler());

    let (dur1, dur2) = join!(h1, h2);

    let d = dur1.min(dur2);
    assert_duration!(d, processing_dur, std::time::Duration::from_millis(2));

    let d = dur1.max(dur2);
    assert_duration!(d, processing_dur, std::time::Duration::from_millis(2));
}

/// Given we use a Sequential strategy with max concurrency = 1
/// When we process the first item
///  And wait for it to complete
///  And then add another item
/// Then it should succeed
#[tokio::test]
async fn strategy_sequential_single_with_wait() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(10).max_key_concurrency(1),
        BatchingPolicy::Immediate,
    );

    let handler = || async {
        let now = Instant::now();

        batcher.add("A".to_string(), "1".to_string()).await.unwrap();

        now.elapsed()
    };

    let d1 = tokio_test::task::spawn(handler()).await;
    assert_duration!(d1, processing_dur, std::time::Duration::from_millis(2));

    let d1 = tokio_test::task::spawn(handler()).await;
    assert_duration!(d1, processing_dur, std::time::Duration::from_millis(2));
}

/// Given we use a Sequential strategy with max concurrency = 1
/// When we submit the maximum size + 1 at once (first batch of 1, then a full batch)
/// Then they should all succeed
#[tokio::test]
async fn strategy_sequential_single_full() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(100).max_key_concurrency(1),
        BatchingPolicy::Immediate,
    );

    let handler = |i: i32| {
        let f = batcher.add("key".to_string(), i.to_string());
        async move { f.await.unwrap() }
    };

    let mut tasks = vec![];
    for i in 1..=101 {
        tasks.push(tokio_test::task::spawn(handler(i)));
    }

    let outputs = join_all(tasks.into_iter()).await;

    assert_eq!(outputs.last().unwrap(), "101 processed for key");
}

/// Given we use a Sequential strategy with max concurrency = 1
/// When we submit > the maximum size + 1 at once
/// Then they should all succeed
#[tokio::test]
async fn strategy_sequential_single_reject() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(100).max_key_concurrency(1),
        BatchingPolicy::Immediate,
    );

    let handler = |i: i32| {
        let f = batcher.add("key".to_string(), i.to_string());
        f
    };

    let mut tasks = vec![];
    for i in 1..=102 {
        tasks.push(tokio_test::task::spawn(handler(i)));
    }

    let outputs = join_all(tasks.into_iter()).await;

    let (ok, err): (Vec<_>, Vec<_>) = outputs.into_iter().partition(|item| item.is_ok());

    assert_eq!(ok.len(), 101);
    assert_eq!(err.len(), 1);
}
