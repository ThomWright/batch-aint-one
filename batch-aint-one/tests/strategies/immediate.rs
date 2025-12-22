use std::time::Duration;

use assert_matches::assert_matches;
use batch_aint_one::{
    BatchError, Batcher, BatchingPolicy, Limits,
    error::{ConcurrencyStatus, RejectionReason},
};
use futures::{FutureExt, future::join_all};
use tokio::{join, time::Instant};

use crate::{assert_duration, types::SimpleBatchProcessor};

/// Given we use a Immediate strategy with max concurrency = 1
/// When we process two items
/// Then it should process them serially, i.e. it should take twice the processing duration
#[tokio::test]
async fn single_concurrency() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_single_concurrency")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let handler = || async {
        let now = Instant::now();

        batcher.add("A".to_string(), "1".to_string()).await.unwrap();

        now.elapsed()
    };

    let h1 = tokio_test::task::spawn(handler());

    // Sleep a bit to ensure the first batch acquires resources and starts processing before the
    // second item gets submitted
    let h2 =
        tokio_test::task::spawn(tokio::time::sleep(Duration::from_millis(1)).then(|_| handler()));

    let (dur1, dur2) = join!(h1, h2);

    let d = dur1.min(dur2);
    assert_duration!(d, processing_dur, std::time::Duration::from_millis(2));

    let d = dur1.max(dur2);
    assert_duration!(d, processing_dur * 2, std::time::Duration::from_millis(2));
}

/// Given we use a Immediate strategy with max concurrency = 2
/// When we process two items
/// Then it should process them concurrently
#[tokio::test]
async fn dual() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_dual")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(1)
                .max_key_concurrency(2)
                .build(),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let handler = || async {
        let now = Instant::now();

        batcher.add("A".to_string(), "1".to_string()).await.unwrap();

        now.elapsed()
    };

    let h1 = tokio_test::task::spawn(handler());

    // Sleep a bit to ensure the first batch acquires resources and starts processing before the
    // second item gets submitted
    let h2 =
        tokio_test::task::spawn(tokio::time::sleep(Duration::from_millis(1)).then(|_| handler()));

    let (dur1, dur2) = join!(h1, h2);

    let d = dur1.min(dur2);
    assert_duration!(d, processing_dur, std::time::Duration::from_millis(2));

    let d = dur1.max(dur2);
    assert_duration!(d, processing_dur, std::time::Duration::from_millis(2));
}

/// Given we use a Immediate strategy with max concurrency = 1
/// When we process the first item
///  And wait for it to complete
///  And then add another item
/// Then it should succeed
#[tokio::test]
async fn single_concurrency_with_wait() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_single_concurrency_with_wait")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

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

/// Given we use a Immediate strategy with max concurrency = 1 and default queue size
/// When we submit the maximum batch size + 1 at once (first batch of 1, then a full batch)
/// Then they should all succeed
#[tokio::test]
async fn single_concurrency_default_queue_size() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_single_concurrency_full")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let handler = |i: u64| {
        let f = batcher.add("key".to_string(), i.to_string());
        f
    };

    let mut tasks = vec![];
    for i in 1..=11 {
        tasks.push(tokio_test::task::spawn(
            tokio::time::sleep(Duration::from_millis(i)).then(move |_| handler(i)),
        ));
    }

    let outputs = join_all(tasks.into_iter()).await;

    let (ok, err): (Vec<_>, Vec<_>) = outputs.into_iter().partition(|item| item.is_ok());

    assert_eq!(ok.len(), 11, "All items should succeed");
    assert_eq!(err.len(), 0, "No items should fail");
}

/// Given we use a Immediate strategy with max concurrency = 1 and max queue size = 1
/// When we submit > the maximum size + 1 at once
/// Then they should all succeed except one
#[tokio::test]
async fn single_concurrency_reject_when_exceeding_queue_size() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(500);

    let batcher = Batcher::builder()
        .name("test_single_concurrency_reject")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .max_batch_queue_size(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let handler = |i: u64| {
        let f = batcher.add("key".to_string(), i.to_string());
        f
    };

    let mut tasks = vec![];
    for i in 1..=12 {
        tasks.push(tokio_test::task::spawn(
            tokio::time::sleep(Duration::from_millis(i)).then(move |_| handler(i)),
        ));
    }

    let outputs = join_all(tasks.into_iter()).await;

    let (ok, err): (Vec<_>, Vec<_>) = outputs.into_iter().partition(|item| item.is_ok());

    assert_eq!(ok.len(), 11, "All items except one should succeed");
    assert_eq!(err.len(), 1, "One item should fail");
    assert_matches!(
        err.first().unwrap().as_ref().err().unwrap(),
        BatchError::Rejected(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
    );
}

/// Given we use a Immediate strategy with max concurrency = 2
/// When we submit 2 * maximum_size + 1 at once (first batch of 1, then two full batches)
/// Then they should all succeed
#[tokio::test]
async fn double_concurrency_full() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_double_concurrency_full")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(2)
                .build(),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let handler = |i: i32| {
        let f = batcher.add("key".to_string(), i.to_string());
        async move { f.await.unwrap() }
    };

    let mut tasks = vec![];
    for i in 1..=21 {
        tasks.push(tokio_test::task::spawn(handler(i)));
    }

    let outputs = join_all(tasks.into_iter()).await;

    assert_eq!(outputs.last().unwrap(), "21 processed for key");
}
