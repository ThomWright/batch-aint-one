//! Benchmarks of the worker's cost per item, through the public API.
//!
//! Everything runs on one thread, for less noise. Each measurement includes the callers' work as
//! well as the worker's.

use std::{
    cell::RefCell,
    collections::VecDeque,
    future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use batch_aint_one::{
    Batcher, BatchingPolicy, GaugeRecording, Limits, MetricsRecorder, MetricsRecorderFactory,
    Processor,
    error::{BatchError, BatchResult},
};
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use futures::{FutureExt, StreamExt};
use tokio::{runtime::Runtime, time::Instant};

/// How many adds are in flight at once, so the worker always has items waiting.
const CONCURRENT_ADDS: usize = 256;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Adding items which are processed straight away.
fn add(c: &mut Criterion) {
    let runtime = runtime();
    // Building a batcher spawns its worker.
    let _runtime = runtime.enter();
    let batcher = Batcher::builder()
        .name("add")
        .processor(Echo)
        .limits(Limits::default())
        .batching_policy(BatchingPolicy::Immediate)
        .build();
    let mut group = c.benchmark_group("add");
    group.throughput(Throughput::Elements(1));

    for callers in [1, CONCURRENT_ADDS] {
        group.bench_with_input(
            BenchmarkId::new("callers", callers),
            &callers,
            |b, &callers| {
                b.to_async(&runtime).iter_custom(|adds| {
                    let batcher = &batcher;
                    async move {
                        let start = Instant::now();
                        futures::stream::iter(0..adds)
                            .for_each_concurrent(callers, |_| async {
                                batcher.add(0, ()).await.unwrap();
                            })
                            .await;
                        start.elapsed()
                    }
                });
            },
        );
    }
    group.finish();
}

/// Rejecting items for a full queue, by queue length. No caller stops waiting, so there are never
/// any cancelled items to remove.
fn rejection(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("rejection");
    group.throughput(Throughput::Elements(1));

    for queue_length in [10, 2_000, 10_000, 100_000] {
        let queued = runtime.block_on(Queued::new(queue_length, &[queue_length]));
        group.bench_with_input(
            BenchmarkId::new("queue_length", queue_length),
            &queued,
            |b, queued| {
                b.to_async(&runtime).iter_custom(|rejections| async move {
                    let start = Instant::now();
                    queued.reject((0..rejections).map(|_| 0)).await;
                    start.elapsed()
                });
            },
        );
    }
    group.finish();
}

/// Rejecting items for a full queue of 10,000, while its queued callers stop waiting, and are
/// replaced.
fn rejection_with_cancellations(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("rejection_with_cancellations");
    group.throughput(Throughput::Elements(1));

    let queued = runtime.block_on(Queued::new(10_000, &[10_000]));
    for ratio in [(10, 1), (1, 1), (1, 10), (1, 100)] {
        bench_cancellations(&mut group, &runtime, &queued, ratio, 0, |_| 0);
    }
    group.finish();
}

/// Rejecting items for a full queue of 10,000, while callers stop waiting, and are replaced, for
/// another key whose queue isn't full. To the full queue, these look like callers giving up on
/// items which are already in a batch: neither leaves anything in it to remove.
fn rejection_with_cancellations_elsewhere(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("rejection_with_cancellations_elsewhere");
    group.throughput(Throughput::Elements(1));

    let queued = runtime.block_on(Queued::new(10_000, &[10_000, 5_000]));
    for ratio in [(10, 1), (1, 1), (1, 10), (1, 100)] {
        bench_cancellations(&mut group, &runtime, &queued, ratio, 1, |_| 0);
    }
    group.finish();
}

/// Rejecting items for 100 keys, each with a full queue of 1,000, while one key's queued callers
/// stop waiting, and are replaced. Rejections are spread evenly across the keys.
fn rejection_with_cancellations_across_keys(c: &mut Criterion) {
    const KEYS: usize = 100;

    let runtime = runtime();
    let mut group = c.benchmark_group("rejection_with_cancellations_across_keys");
    group.throughput(Throughput::Elements(1));

    let queued = runtime.block_on(Queued::new(1_000, &[1_000; KEYS]));
    for ratio in [(1, 100), (1, 1_000)] {
        bench_cancellations(&mut group, &runtime, &queued, ratio, 0, |i| i % KEYS);
    }
    group.finish();
}

/// Measure the time per rejection while, in a repeating cycle, `cancellations` of
/// `cancelled_key`'s callers stop waiting and are replaced, then `rejections` items are rejected,
/// the `i`th for `rejected_key(i)`.
fn bench_cancellations(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &Runtime,
    queued: &Queued,
    (cancellations, rejections): (usize, usize),
    cancelled_key: usize,
    rejected_key: impl Fn(usize) -> usize + Copy,
) {
    group.bench_function(
        BenchmarkId::new("cancellations", format!("{cancellations}_per_{rejections}")),
        |b| {
            b.to_async(runtime).iter_custom(|total| async move {
                let start = Instant::now();
                for _ in 0..(total as usize).div_ceil(rejections) {
                    for _ in 0..cancellations {
                        queued.cancel_and_replace(cancelled_key).await;
                    }
                    queued.reject((0..rejections).map(rejected_key)).await;
                }
                start.elapsed()
            });
        },
    );
}

/// Acquires nothing, and returns its inputs.
#[derive(Debug, Clone)]
struct Echo;

impl Processor for Echo {
    type Key = usize;
    type Input = ();
    type Output = ();
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: usize) -> Result<(), String> {
        Ok(())
    }

    async fn process(
        &self,
        _key: usize,
        inputs: impl Iterator<Item = ()> + Send,
        _resources: (),
    ) -> Result<Vec<()>, String> {
        Ok(inputs.collect())
    }
}

/// Never finishes acquiring resources, so every item stays queued.
#[derive(Debug, Clone)]
struct NeverAcquires;

impl Processor for NeverAcquires {
    type Key = usize;
    type Input = ();
    type Output = ();
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: usize) -> Result<(), String> {
        future::pending().await
    }

    async fn process(
        &self,
        _key: usize,
        _inputs: impl Iterator<Item = ()> + Send,
        _resources: (),
    ) -> Result<Vec<()>, String> {
        unreachable!("resources are never acquired")
    }
}

type Add = Pin<Box<dyn Future<Output = BatchResult<(), String>>>>;

/// A batcher with items queued for each key, whose callers are all waiting.
struct Queued {
    batcher: Batcher<NeverAcquires>,
    received: CountReceived,
    /// Per key, the waiting callers' `add` futures, oldest first. Dropping one cancels its item.
    callers: RefCell<Vec<VecDeque<Add>>>,
}

impl Queued {
    /// Queue `queued[key]` items for each key, checking that queues of `queue_limit` are full.
    async fn new(queue_limit: usize, queued: &[usize]) -> Self {
        let total: usize = queued.iter().sum();
        let received = CountReceived::default();
        let batcher = Batcher::builder()
            .name("queued")
            .processor(NeverAcquires)
            .limits(
                Limits::builder()
                    .max_key_concurrency(1)
                    .max_key_queued_items(queue_limit)
                    // Room for every item, so sending never waits for the worker.
                    .max_items_awaiting_worker(total + CONCURRENT_ADDS)
                    .build(),
            )
            .batching_policy(BatchingPolicy::Immediate)
            .metrics(Box::new(received.clone()))
            .build();
        let callers = queued
            .iter()
            .enumerate()
            .map(|(key, &count)| (0..count).map(|_| send(&batcher, key)).collect())
            .collect();
        let full_keys = (0..queued.len()).filter(|&key| queued[key] == queue_limit);
        let queued = Self {
            callers: RefCell::new(callers),
            batcher,
            received,
        };

        queued.received.wait_for(total).await;
        queued.reject(full_keys).await;
        queued
    }

    /// Add items for these keys, checking each is rejected.
    async fn reject(&self, keys: impl Iterator<Item = usize>) {
        futures::stream::iter(keys)
            .for_each_concurrent(CONCURRENT_ADDS, |key| async move {
                let result = self.batcher.add(key, ()).await;
                assert!(
                    matches!(result, Err(BatchError::Rejected(_))),
                    "key {key}'s queue should be full, got {result:?}"
                );
            })
            .await;
    }

    /// Cancel the key's oldest queued item, and queue another in its place.
    async fn cancel_and_replace(&self, key: usize) {
        let target = self.received.count() + 1;
        {
            let mut callers = self.callers.borrow_mut();
            callers[key].pop_front();
            callers[key].push_back(send(&self.batcher, key));
        }
        self.received.wait_for(target).await;
    }
}

/// Start adding an item, and send it to the worker, without waiting for its result.
fn send(batcher: &Batcher<NeverAcquires>, key: usize) -> Add {
    let batcher = batcher.clone();
    // Unconstrained, or once this task's tokio budget runs out, sending returns `Pending` without
    // sending, and nothing polls it again.
    let mut add: Add = Box::pin(tokio::task::unconstrained(async move {
        batcher.add(key, ()).await
    }));
    // There's room in the channel, so the first poll sends the item.
    assert!((&mut add).now_or_never().is_none());
    add
}

/// Counts the items the worker has received.
#[derive(Debug, Clone, Default)]
struct CountReceived(Arc<AtomicUsize>);

impl CountReceived {
    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    async fn wait_for(&self, count: usize) {
        while self.count() < count {
            tokio::task::yield_now().await;
        }
    }
}

impl MetricsRecorder for CountReceived {
    fn gauge_recording(&self) -> GaugeRecording {
        // As without a recorder, so the worker does the same work per item.
        GaugeRecording::OnDemand
    }

    fn item_received(&self, _channel_duration: Duration) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl MetricsRecorderFactory for CountReceived {
    fn create_recorder(&self, _batcher_name: &str) -> Box<dyn MetricsRecorder> {
        Box::new(self.clone())
    }
}

criterion_group!(
    benches,
    add,
    rejection,
    rejection_with_cancellations,
    rejection_with_cancellations_elsewhere,
    rejection_with_cancellations_across_keys
);
criterion_main!(benches);
