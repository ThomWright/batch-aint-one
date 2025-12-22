use std::{sync::Arc, time::Duration};

use batch_aint_one::{Batcher, BatchingPolicy, Limits};
use futures::future::join_all;
use tokio::{join, time::Instant};

use crate::{assert_duration, types::SimpleBatchProcessor};

/// Given we use Balanced strategy with min_size_hint
/// When the first item arrives and no batches are processing
/// Then it should process immediately (low first-item latency)
#[tokio::test]
async fn first_item_processes_immediately() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_first_item_immediate")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Balanced { min_size_hint: 5 })
        .build();

    let now = Instant::now();
    batcher.add("A".to_string(), "1".to_string()).await.unwrap();
    let elapsed = now.elapsed();

    // Should complete in processing_dur, not wait for more items
    assert_duration!(elapsed, processing_dur, Duration::from_millis(2));
}

/// Given we use Balanced strategy with min_size_hint=5
/// And one batch is already processing
/// And a second item arrives
/// When the first batch finishes
/// Then the second item should start processing
#[tokio::test]
async fn waits_below_min_size_hint() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_waits_below_hint")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Balanced { min_size_hint: 5 })
        .build();

    let h1 = tokio_test::task::spawn(async {
        let now = Instant::now();
        batcher.add("A".to_string(), "1".to_string()).await.unwrap();
        now.elapsed()
    });

    // Add second item - should wait for first batch to finish (batch size = 1 < hint = 5)
    let h2 = tokio_test::task::spawn(async {
        // Let first item start processing
        tokio::time::sleep(Duration::from_millis(1)).await;
        let now = Instant::now();
        batcher.add("A".to_string(), "2".to_string()).await.unwrap();
        now.elapsed()
    });

    let (dur1, dur2) = join!(h1, h2);

    // First item completes in ~50ms
    assert_duration!(dur1, processing_dur, Duration::from_millis(2));

    // Second item waits for first to finish (~49ms) + processes (~50ms) = ~99ms
    assert_duration!(
        dur2,
        processing_dur * 2 - Duration::from_millis(1),
        Duration::from_millis(2)
    );
}

/// Given we use Balanced strategy with min_size_hint=5
/// When items arrive and reach min_size_hint before any batch finishes
/// Then the batch should process immediately without waiting
#[tokio::test]
async fn processes_at_min_size_hint() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Arc::new(
        Batcher::builder()
            .name("test_processes_at_hint")
            .processor(SimpleBatchProcessor(processing_dur))
            .limits(
                Limits::builder()
                    .max_batch_size(10)
                    .max_key_concurrency(2) // Allow 2 concurrent batches
                    .build(),
            )
            .batching_policy(BatchingPolicy::Balanced { min_size_hint: 5 })
            .build(),
    );

    // First item starts processing
    let batcher_clone = Arc::clone(&batcher);
    let h1 = tokio_test::task::spawn(async move {
        batcher_clone
            .add("A".to_string(), "1".to_string())
            .await
            .unwrap();
    });

    // Add 5 more items quickly - should reach min_size_hint and process
    let mut handles = vec![];
    for i in 2..=6 {
        let batcher_clone = Arc::clone(&batcher);
        handles.push(tokio_test::task::spawn(async move {
            let now = Instant::now();
            batcher_clone
                .add("A".to_string(), i.to_string())
                .await
                .unwrap();
            now.elapsed()
        }));
    }

    h1.await;
    let durations = join_all(handles).await;

    // Items 2-6 should process together without waiting for first batch to finish
    // They wait ~1ms then process for ~50ms = ~51ms total
    for dur in durations {
        assert!(
            dur < Duration::from_millis(60),
            "Expected < 60ms, got {:?}",
            dur
        );
    }
}

/// Given we use Balanced strategy with min_size_hint=1
/// Then it should behave like Immediate policy
#[tokio::test]
async fn min_size_hint_one_behaves_like_immediate() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_hint_one")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(10)
                .max_key_concurrency(2)
                .build(),
        )
        .batching_policy(BatchingPolicy::Balanced { min_size_hint: 1 })
        .build();

    let h1 = tokio_test::task::spawn(async {
        let now = Instant::now();
        batcher.add("A".to_string(), "1".to_string()).await.unwrap();
        now.elapsed()
    });

    // Second item should process immediately in parallel (min_size_hint=1)
    let h2 = tokio_test::task::spawn(async {
        let now = Instant::now();
        batcher.add("A".to_string(), "2".to_string()).await.unwrap();
        now.elapsed()
    });

    let (dur1, dur2) = join!(h1, h2);

    // Both should complete in ~50ms (processing concurrently)
    assert_duration!(dur1, processing_dur, Duration::from_millis(2));
    assert_duration!(
        dur2,
        processing_dur + Duration::from_millis(1),
        Duration::from_millis(2)
    );
}

/// Given we use Balanced strategy with min_size_hint=max_batch_size
/// When items arrive below max_batch_size
/// Then it should still process when another batch finishes
#[tokio::test]
async fn min_size_hint_equals_max_batch_size() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Batcher::builder()
        .name("test_hint_equals_max")
        .processor(SimpleBatchProcessor(processing_dur))
        .limits(
            Limits::builder()
                .max_batch_size(5)
                .max_key_concurrency(1)
                .build(),
        )
        .batching_policy(BatchingPolicy::Balanced { min_size_hint: 5 })
        .build();

    let h1 = tokio_test::task::spawn(async {
        batcher.add("A".to_string(), "1".to_string()).await.unwrap();
    });

    // Add 2 more items (total = 2 < min_size_hint = 5)
    let h2 = tokio_test::task::spawn(async {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let now = Instant::now();
        batcher.add("A".to_string(), "2".to_string()).await.unwrap();
        now.elapsed()
    });

    let h3 = tokio_test::task::spawn(async {
        tokio::time::sleep(Duration::from_millis(1)).await;
        batcher.add("A".to_string(), "3".to_string()).await.unwrap();
    });

    let ((), dur2, ()) = join!(h1, h2, h3);

    // Items 2-3 should wait for first batch to finish, then process together
    // ~50ms wait + ~50ms process - 1ms wait = ~99ms
    assert_duration!(
        dur2,
        processing_dur * 2 - Duration::from_millis(1),
        Duration::from_millis(2)
    );
}

/// Given we use Balanced strategy
/// When multiple concurrent batches are allowed
/// Then batches should process independently when they reach min_size_hint
#[tokio::test]
async fn multiple_concurrent_batches() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Arc::new(
        Batcher::builder()
            .name("test_concurrent_batches")
            .processor(SimpleBatchProcessor(processing_dur))
            .limits(
                Limits::builder()
                    .max_batch_size(10)
                    .max_key_concurrency(3)
                    .build(),
            )
            .batching_policy(BatchingPolicy::Balanced { min_size_hint: 3 })
            .build(),
    );

    let mut handles = vec![];

    // Submit 9 items quickly - should create 3 batches of 3 items each
    for i in 1..=9 {
        let batcher_clone = Arc::clone(&batcher);
        handles.push(tokio_test::task::spawn(async move {
            let now = Instant::now();
            batcher_clone
                .add("A".to_string(), i.to_string())
                .await
                .unwrap();
            now.elapsed()
        }));
    }

    let durations = join_all(handles).await;

    // All should complete relatively quickly (within 2 * processing_dur)
    // First batch: ~50ms, second batch: ~50ms, third batch: ~50ms (all concurrent)
    for dur in durations {
        assert!(dur < processing_dur * 2, "Expected < 100ms, got {:?}", dur);
    }
}

/// Given we use Balanced strategy
/// When we exceed max queue size
/// Then additional items should be rejected
#[tokio::test]
async fn rejects_when_exceeding_queue_size() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(500);

    let batcher = Arc::new(
        Batcher::builder()
            .name("test_rejection")
            .processor(SimpleBatchProcessor(processing_dur))
            .limits(
                Limits::builder()
                    .max_batch_size(10)
                    .max_key_concurrency(1)
                    .max_batch_queue_size(2)
                    .build(),
            )
            .batching_policy(BatchingPolicy::Balanced { min_size_hint: 5 })
            .build(),
    );

    let mut handles = vec![];

    // Submit 21 items: first processes immediately (1), next wait to reach hint (4),
    // then 5 more reach hint and process, then queue fills (10), then 1 should reject
    for i in 1..=21 {
        let batcher_clone = Arc::clone(&batcher);
        handles.push(tokio_test::task::spawn(async move {
            batcher_clone.add("A".to_string(), i.to_string()).await
        }));
    }

    let results = join_all(handles).await;

    let (ok, err): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);

    assert_eq!(ok.len(), 20, "Expected 20 successful, got {}", ok.len());
    assert_eq!(err.len(), 1, "Expected 1 rejection, got {}", err.len());
}

/// Given we use Balanced strategy
/// When processing a steady stream at moderate load
/// Then it should achieve good average batch sizes
#[tokio::test]
async fn steady_load_achieves_good_batch_sizes() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(20);
    let inter_arrival = Duration::from_millis(3); // ~333 items/sec

    let batcher = Arc::new(
        Batcher::builder()
            .name("test_steady_load")
            .processor(SimpleBatchProcessor(processing_dur))
            .limits(
                Limits::builder()
                    .max_batch_size(20)
                    .max_key_concurrency(1)
                    .build(),
            )
            .batching_policy(BatchingPolicy::Balanced {
                min_size_hint: 6, // Expect ~7 items per batch (20ms / 3ms)
            })
            .build(),
    );

    let mut handles = vec![];

    for i in 0..50 {
        let delay = inter_arrival * i;
        let batcher_clone = Arc::clone(&batcher);
        handles.push(tokio_test::task::spawn(async move {
            tokio::time::sleep(delay).await;
            batcher_clone
                .add("A".to_string(), i.to_string())
                .await
                .unwrap();
        }));
    }

    join_all(handles).await;

    // Test completes successfully - batch sizes should be around min_size_hint
    // (We can't easily assert on batch sizes without modifying the processor,
    // but the test validates the behavior doesn't hang or reject)
}

/// Given we use Balanced strategy
/// When a burst of items arrives
/// Then it should use available concurrency
#[tokio::test]
async fn burst_load_uses_concurrency() {
    tokio::time::pause();

    let processing_dur = Duration::from_millis(50);

    let batcher = Arc::new(
        Batcher::builder()
            .name("test_burst_load")
            .processor(SimpleBatchProcessor(processing_dur))
            .limits(
                Limits::builder()
                    .max_batch_size(10)
                    .max_key_concurrency(3)
                    .build(),
            )
            .batching_policy(BatchingPolicy::Balanced { min_size_hint: 5 })
            .build(),
    );

    let mut handles = vec![];

    // Submit 30 items at once (burst)
    for i in 1..=30 {
        let batcher_clone = Arc::clone(&batcher);
        handles.push(tokio_test::task::spawn(async move {
            let now = Instant::now();
            batcher_clone
                .add("A".to_string(), i.to_string())
                .await
                .unwrap();
            now.elapsed()
        }));
    }

    let durations = join_all(handles).await;

    // With 3 concurrent batches and bursting load, should complete much faster
    // than serial processing (30 items * 50ms = 1500ms serial)
    // Expected: ~3-4 rounds of 3 batches = ~150-200ms
    let max_duration = durations.iter().max().unwrap();
    assert!(
        *max_duration < Duration::from_millis(300),
        "Expected < 300ms, got {:?}",
        max_duration
    );
}
