use std::{marker::Send, time::Duration};

use async_trait::async_trait;
use batch_aint_one::{Batcher, BatchingStrategy, Processor};
use futures::future::join_all;
use tokio::{join, time::Instant};
// use tokio_test::assert_elapsed;

#[derive(Debug, Clone)]
struct SimpleBatchProcessor(Duration);

#[async_trait]
impl Processor<String, String> for SimpleBatchProcessor {
    async fn process(&self, inputs: impl Iterator<Item = String> + Send) -> Vec<String> {
        tokio::time::sleep(self.0).await;
        inputs.map(|s| s + " processed").collect()
    }
}

struct NotCloneable {}
type Cloneable = Batcher<String, NotCloneable, NotCloneable>;

/// A [Batcher] should be cloneable, even when the `I`s and `O`s are not.
#[derive(Clone)]
#[allow(unused)]
struct CanDeriveClone {
    batcher: Cloneable
}

#[tokio::test]
async fn strategy_size() {
    let batcher = Batcher::new(
        SimpleBatchProcessor(Duration::ZERO),
        BatchingStrategy::Size(3),
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
/// When we process lots of items
/// Then they should all succeed
#[tokio::test]
async fn strategy_size_loaded() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        BatchingStrategy::Size(10),
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
/// Then it should take as long at the batching duration + the processing duration
#[tokio::test]
async fn strategy_duration() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(30);
    let batching_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        BatchingStrategy::Duration(batching_dur),
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
/// When we process lots of items
/// Then they should all succeed
#[tokio::test]
async fn strategy_duration_loaded() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        BatchingStrategy::Duration(Duration::from_millis(10)),
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

/// Given we use a Sequential strategy
/// When we process two items
/// Then it should process them serially, i.e. it should take twice the processing duration
#[tokio::test]
async fn strategy_sequential() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        BatchingStrategy::Sequential,
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
///     And wait for it to complete
///     And then add another item
/// Then it should succeed
#[tokio::test]
async fn strategy_sequential_with_wait() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        BatchingStrategy::Sequential,
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
/// When we process lots of items
/// Then they should all succeed
#[tokio::test]
async fn strategy_sequential_loaded() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::new(
        SimpleBatchProcessor(processing_dur),
        BatchingStrategy::Sequential,
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
