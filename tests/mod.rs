use std::{marker::Send, time::Duration};

use async_trait::async_trait;
use batch_aint_one::{Batcher, BatchingPolicy, OnFull, Processor};
use futures::future::join_all;
use tokio::{join, time::Instant};
// use tokio_test::assert_elapsed;

#[derive(Debug, Clone)]
struct SimpleBatchProcessor(Duration);

#[async_trait]
impl Processor<String, String, String> for SimpleBatchProcessor {
    async fn process(
        &self,
        _key: String,
        inputs: impl Iterator<Item = String> + Send,
    ) -> Result<Vec<String>, String> {
        tokio::time::sleep(self.0).await;
        Ok(inputs.map(|s| s + " processed").collect())
    }
}

struct NotCloneable {}
type Cloneable = Batcher<String, NotCloneable, NotCloneable>;

/// A [Batcher] should be cloneable, even when the `I`s and `O`s are not.
#[derive(Clone)]
#[allow(unused)]
struct CanDeriveClone {
    batcher: Cloneable,
}

#[tokio::test]
async fn strategy_size() {
    let batcher = Batcher::new(
        SimpleBatchProcessor(Duration::ZERO),
        3,
        BatchingPolicy::Size,
    );

    let h1 = tokio_test::task::spawn(batcher.add("A".to_string(), "1".to_string()));
    let h2 = tokio_test::task::spawn(batcher.add("A".to_string(), "2".to_string()));
    let h3 = tokio_test::task::spawn(batcher.add("A".to_string(), "3".to_string()));

    let (o1, o2, o3) = join!(h1, h2, h3);

    assert_eq!("1 processed".to_string(), o1.unwrap());
    assert_eq!("2 processed".to_string(), o2.unwrap());
    assert_eq!("3 processed".to_string(), o3.unwrap());
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
        10,
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

    assert_eq!(outputs.last().unwrap(), "100 processed");
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
        10,
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
        10,
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

    assert_eq!(outputs.last().unwrap(), "100 processed");
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
        10,
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

/// Given we use a Sequential strategy
/// When we process two items
/// Then it should process them serially, i.e. it should take twice the processing duration
#[tokio::test]
async fn strategy_sequential() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        10,
        BatchingPolicy::Sequential,
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

/// Given we use a Sequential strategy
/// When we process the first item
///  And wait for it to complete
///  And then add another item
/// Then it should succeed
#[tokio::test]
async fn strategy_sequential_with_wait() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        10,
        BatchingPolicy::Sequential,
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

/// Given we use a Sequential strategy
/// When we submit the maximum size + 1 at once (first batch of 1, then a full batch)
/// Then they should all succeed
#[tokio::test]
async fn strategy_sequential_full() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        100,
        BatchingPolicy::Sequential,
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

    assert_eq!(outputs.last().unwrap(), "101 processed");
}

/// Given we use a Sequential strategy
/// When we submit > the maximum size + 1 at once
/// Then they should all succeed
#[tokio::test]
async fn strategy_sequential_reject() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        100,
        BatchingPolicy::Sequential,
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
