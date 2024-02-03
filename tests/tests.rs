use std::time::Duration;

use batch_aint_one::{Batcher, BatchingPolicy, Limits, OnFull};
use futures::future::join_all;
use tokio::{join, time::Instant};

use crate::types::SimpleBatchProcessor;

mod types;

#[tokio::test]
async fn strategy_size() {
    let batcher = Batcher::new(
        SimpleBatchProcessor(Duration::ZERO),
        Limits::default().max_batch_size(3),
        BatchingPolicy::Size,
    );

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
async fn strategy_size_loaded() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(10),
        BatchingPolicy::Size,
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

#[tokio::test]
async fn strategy_size_max_concurrency_limit() {
    let batcher = Batcher::new(
        SimpleBatchProcessor(Duration::ZERO),
        Limits::default().max_batch_size(1).max_key_concurrency(2),
        BatchingPolicy::Size,
    );

    // Two processed for A immediately
    let h1 = tokio_test::task::spawn(batcher.add("A".to_string(), "1".to_string()));
    let h2 = tokio_test::task::spawn(batcher.add("A".to_string(), "2".to_string()));

    // One processed for A after another finished
    let h3 = tokio_test::task::spawn(batcher.add("A".to_string(), "3".to_string()));

    // One rejected
    let h4 = tokio_test::task::spawn(batcher.add("A".to_string(), "4".to_string()));

    // One different key
    let h5 = tokio_test::task::spawn(batcher.add("B".to_string(), "1".to_string()));

    let (o1, o2, o3, o4, o5) = join!(h1, h2, h3, h4, h5);

    assert_eq!("1 processed for A".to_string(), o1.unwrap());
    assert_eq!("2 processed for A".to_string(), o2.unwrap());
    assert_eq!("3 processed for A".to_string(), o3.unwrap());
    assert_eq!(
        "Batch item rejected: the key has reached maximum concurrency",
        o4.unwrap_err().to_string()
    );
    assert_eq!("1 processed for B".to_string(), o5.unwrap());
}

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
/// When we submit more items at once than the maximum size
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
/// When we submit more items at once than the maximum size
///  And we reject when full
/// Then only one batch should succeed, the rest should get rejected
#[tokio::test]
async fn strategy_duration_loaded_reject_on_full() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        Limits::default().max_batch_size(10),
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

#[macro_export]
macro_rules! assert_elapsed {
    ($start:expr, $dur:expr, $tolerance:expr) => {{
        let elapsed = $start.elapsed();
        let lower: std::time::Duration = $dur;

        // Handles ms rounding
        assert!(
            elapsed >= lower && elapsed <= lower + $tolerance,
            "actual = {:?}, expected = {:?}",
            elapsed,
            lower
        );
    }};
}

#[macro_export]
macro_rules! assert_duration {
    ($actual:expr, $expected:expr, $tolerance:expr) => {{
        let lower: std::time::Duration = $expected;

        // Handles ms rounding
        assert!(
            $actual >= lower && $actual <= lower + $tolerance,
            "actual = {:?}, expected = {:?}",
            $actual,
            lower
        );
    }};
}
