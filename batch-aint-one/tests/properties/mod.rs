//! Property tests of the whole batcher.
//!
//! Each case generates a configuration and a sequence of operations: adds, callers giving up,
//! resource acquisitions and batches finishing (or failing, or panicking), time passing, and
//! shutdown. The test processor holds every call until the sequence releases it, so the sequence
//! decides the order things finish in. After every step, the harness checks that what the batcher
//! did is consistent with its documented behaviour.
//!
//! It doesn't predict which batches form. It tracks what it did, e.g. which items are still
//! queued, and checks invariants against that: limits, results, and that a batch starts acquiring
//! resources exactly when the policy's rule allows it (see [`Harness::policy_allows`]).

use std::{
    collections::{BTreeMap, VecDeque},
    panic,
    sync::Arc,
    time::Duration,
};

use batch_aint_one::{
    Batcher, BatchingPolicy, Limits, OnFull, Processor, WorkerHandle,
    error::{BatchError, BatchResult, ConcurrencyStatus, RejectionReason},
};
use futures::FutureExt;
use proptest::{prelude::*, test_runner::FileFailurePersistence};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

type Key = usize;
type Input = u64;

const SETTLE_YIELDS: usize = 100;

proptest! {
    #![proptest_config(proptest_config(256))]

    #[test]
    fn batcher_behaves_as_documented(config in config(), ops in prop::collection::vec(op(), 0..50)) {
        run(config, ops);
    }
}

/// A longer run, for soak testing locally.
#[test]
#[ignore]
fn batcher_behaves_as_documented_for_longer_sequences() {
    proptest!(proptest_config(10_000), |(
        config in config(),
        ops in prop::collection::vec(op(), 0..500),
    )| {
        run(config, ops);
    });
}

fn proptest_config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        // The default looks for the crate's `lib.rs`, which integration tests don't have.
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
        ..ProptestConfig::default()
    }
}

fn run(config: Config, ops: Vec<Op>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
        .block_on(async {
            let mut harness = Harness::new(config);
            harness.settle().await;
            for op in ops {
                harness.step(op).await;
            }
            harness.finish().await;
        });
}

#[derive(Debug, Clone, Copy)]
struct Config {
    policy: Policy,
    max_batch_size: usize,
    max_key_concurrency: usize,
    max_key_queued_items: usize,
    keys: usize,
}

/// Mirrors [`BatchingPolicy`], so the checks can match on it exhaustively.
#[derive(Debug, Clone, Copy)]
enum Policy {
    Immediate,
    Size,
    Duration {
        duration: Duration,
        process_on_full: bool,
    },
    Balanced {
        min_size_hint: usize,
    },
}

impl Policy {
    fn to_batching_policy(self) -> BatchingPolicy {
        match self {
            Policy::Immediate => BatchingPolicy::Immediate,
            Policy::Size => BatchingPolicy::Size,
            Policy::Duration {
                duration,
                process_on_full,
            } => BatchingPolicy::Duration(
                duration,
                if process_on_full {
                    OnFull::Process
                } else {
                    OnFull::Wait
                },
            ),
            Policy::Balanced { min_size_hint } => BatchingPolicy::Balanced { min_size_hint },
        }
    }
}

fn config() -> impl Strategy<Value = Config> {
    let policy = prop_oneof![
        Just(Policy::Immediate),
        Just(Policy::Size),
        (1..=100u64, any::<bool>()).prop_map(|(ms, process_on_full)| Policy::Duration {
            duration: Duration::from_millis(ms),
            process_on_full,
        }),
        (1..=5usize).prop_map(|min_size_hint| Policy::Balanced { min_size_hint }),
    ];
    // Small limits, so that sequences of up to 50 operations reach them.
    (policy, 1..=4usize, 1..=3usize, 1..=6usize, 1..=3usize).prop_map(
        |(policy, max_batch_size, max_key_concurrency, max_key_queued_items, keys)| Config {
            policy,
            max_batch_size,
            max_key_concurrency,
            max_key_queued_items,
            keys,
        },
    )
}

#[derive(Debug, Clone, Copy)]
enum Op {
    /// Add an item for a key, as an index into the configured keys.
    Add(usize),
    /// Stop waiting for a pending add, as an index into them, oldest first.
    GiveUp(usize),
    /// Let a held processor call return, as an index into them, oldest first.
    Release(usize, Outcome),
    Advance(Duration),
    ShutDown,
}

/// How a held processor call returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Ok,
    Err,
    Panic,
    /// For `process()`, return one output too many. For `acquire_resources()`, the same as `Ok`.
    WrongOutputs,
}

fn op() -> impl Strategy<Value = Op> {
    let outcome = prop_oneof![
        6 => Just(Outcome::Ok),
        1 => Just(Outcome::Err),
        1 => Just(Outcome::Panic),
        1 => Just(Outcome::WrongOutputs),
    ];
    prop_oneof![
        8 => any::<usize>().prop_map(Op::Add),
        2 => any::<usize>().prop_map(Op::GiveUp),
        8 => (any::<usize>(), outcome).prop_map(|(i, outcome)| Op::Release(i, outcome)),
        3 => (0..=200u64).prop_map(|ms| Op::Advance(Duration::from_millis(ms))),
        1 => Just(Op::ShutDown),
    ]
}

/// Sends every call to the harness, and waits for the harness to release it.
#[derive(Debug, Clone)]
struct HeldProcessor {
    calls: mpsc::UnboundedSender<Call>,
}

#[derive(Debug)]
struct Call {
    key: Key,
    kind: CallKind,
    release: oneshot::Sender<Outcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CallKind {
    Acquire,
    Process(Vec<Input>),
}

impl HeldProcessor {
    async fn hold(&self, key: Key, kind: CallKind) -> Outcome {
        let (release, released) = oneshot::channel();
        self.calls
            .send(Call { key, kind, release })
            .expect("harness should outlive the batcher");
        let outcome = released.await.expect("harness should release every call");
        if outcome == Outcome::Panic {
            // Unlike `panic!`, this doesn't run the panic hook, so doesn't print anything.
            panic::resume_unwind(Box::new("test processor panicked"));
        }
        outcome
    }
}

impl Processor for HeldProcessor {
    type Key = Key;
    type Input = Input;
    type Output = Input;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, key: Key) -> Result<(), String> {
        match self.hold(key, CallKind::Acquire).await {
            Outcome::Ok | Outcome::WrongOutputs => Ok(()),
            Outcome::Err => Err("acquisition failed".to_string()),
            Outcome::Panic => unreachable!(),
        }
    }

    async fn process(
        &self,
        key: Key,
        inputs: impl Iterator<Item = Input> + Send,
        _resources: (),
    ) -> Result<Vec<Input>, String> {
        let inputs: Vec<_> = inputs.collect();
        match self.hold(key, CallKind::Process(inputs.clone())).await {
            Outcome::Ok => Ok(inputs),
            Outcome::WrongOutputs => Ok(inputs.into_iter().chain([0]).collect()),
            Outcome::Err => Err("processing failed".to_string()),
            Outcome::Panic => unreachable!(),
        }
    }
}

/// The result an add should finish with.
#[derive(Debug, Clone, Copy)]
enum Expected {
    Output(Input),
    BatchFailed,
    AcquisitionFailed,
    Panic,
    WrongOutputs,
}

impl Expected {
    /// The result for an item whose batch ended with `call` returning `outcome`.
    fn after(call: &CallKind, outcome: Outcome, input: Input) -> Self {
        match (outcome, call) {
            (Outcome::Ok, _) => Expected::Output(input),
            (Outcome::Err, CallKind::Acquire) => Expected::AcquisitionFailed,
            (Outcome::Err, CallKind::Process(_)) => Expected::BatchFailed,
            (Outcome::Panic, _) => Expected::Panic,
            (Outcome::WrongOutputs, _) => Expected::WrongOutputs,
        }
    }

    fn matches(self, result: &BatchResult<Input, String>) -> bool {
        match (self, result) {
            (Expected::Output(input), Ok(output)) => *output == input,
            (Expected::BatchFailed, Err(BatchError::BatchFailed(_)))
            | (Expected::AcquisitionFailed, Err(BatchError::ResourceAcquisitionFailed(_)))
            | (Expected::Panic, Err(BatchError::Panic))
            | (Expected::WrongOutputs, Err(BatchError::ProcessorInvariantViolation(_))) => true,
            _ => false,
        }
    }
}

struct QueuedItem {
    input: Input,
    /// When the worker received it. No time passes while settling, so the harness sees the same
    /// instant.
    received_at: Instant,
}

/// What a step should cause, beyond starting acquisitions.
#[derive(Default)]
struct Expectations {
    /// The batch a returned acquisition should start processing.
    process: Option<(Key, Vec<Input>)>,
    /// The adds which should finish, and how.
    finished: BTreeMap<Input, Expected>,
    /// The key whose batch finished, if any.
    batch_finished: Option<Key>,
}

struct Harness {
    config: Config,
    batcher: Batcher<HeldProcessor>,
    worker: Arc<WorkerHandle>,
    calls: mpsc::UnboundedReceiver<Call>,
    /// Processor calls waiting to be released, oldest first.
    held: Vec<Call>,
    /// Adds without a result, whose callers are still waiting.
    pending: BTreeMap<Input, JoinHandle<BatchResult<Input, String>>>,
    /// Per key, the items queued in the worker, oldest first: added, not given up on, and not yet
    /// taken by a batch.
    queued: Vec<VecDeque<QueuedItem>>,
    next_input: Input,
    shut_down: bool,
}

impl Harness {
    fn new(config: Config) -> Self {
        let (calls_tx, calls) = mpsc::unbounded_channel();
        let batcher = Batcher::builder()
            .name("properties")
            .processor(HeldProcessor { calls: calls_tx })
            .limits(
                Limits::builder()
                    .max_batch_size(config.max_batch_size)
                    .max_key_concurrency(config.max_key_concurrency)
                    .max_key_queued_items(config.max_key_queued_items)
                    .build(),
            )
            .batching_policy(config.policy.to_batching_policy())
            .build();
        let worker = batcher.worker_handle();
        Self {
            config,
            batcher,
            worker,
            calls,
            held: Vec::new(),
            pending: BTreeMap::new(),
            queued: (0..config.keys).map(|_| VecDeque::new()).collect(),
            next_input: 1,
            shut_down: false,
        }
    }

    /// Let every task run until it's blocked, so the worker has handled everything the last
    /// operation caused.
    ///
    /// Tokio only wakes a yielding task once the scheduler has run out of ready tasks, or has
    /// polled 61 of them, so a single yield is usually enough; the rest are margin. Sleeping
    /// instead would let time pass: timers have millisecond granularity, so each sleep moves the
    /// clock on by at least 1ms, and a deadline due in the same tick could fire after the sleep.
    async fn settle(&self) {
        for _ in 0..SETTLE_YIELDS {
            tokio::task::yield_now().await;
        }
    }

    async fn step(&mut self, op: Op) {
        let expected = match op {
            Op::Add(key) => return self.add(key % self.config.keys).await,
            Op::GiveUp(i) => {
                self.give_up(i);
                Expectations::default()
            }
            Op::Release(i, outcome) => self.release(i, outcome),
            Op::Advance(duration) => {
                tokio::time::advance(duration).await;
                Expectations::default()
            }
            Op::ShutDown => {
                self.worker.shut_down().await;
                self.shut_down = true;
                Expectations::default()
            }
        };

        self.settle().await;
        self.receive_calls(expected.process, expected.batch_finished);
        let finished = self.take_finished();
        assert_eq!(
            finished.keys().collect::<Vec<_>>(),
            expected.finished.keys().collect::<Vec<_>>(),
            "after {op:?}, the adds which finished should be the ones whose batch finished"
        );
        for (input, result) in &finished {
            let expected = expected.finished[input];
            assert!(
                expected.matches(result),
                "after {op:?}, add {input} should have finished with {expected:?}, got {result:?}"
            );
        }
        self.check_progress();
    }

    fn give_up(&mut self, i: usize) {
        if let Some(&input) = self.pending.keys().nth(i % self.pending.len().max(1)) {
            self.pending.remove(&input).unwrap().abort();
            for queue in &mut self.queued {
                queue.retain(|item| item.input != input);
            }
        }
    }

    fn release(&mut self, i: usize, outcome: Outcome) -> Expectations {
        let mut expected = Expectations::default();
        if self.held.is_empty() {
            return expected;
        }
        let call = self.held.remove(i % self.held.len());
        match &call.kind {
            CallKind::Acquire => {
                let batch = self.take_batch(call.key);
                match outcome {
                    Outcome::Ok | Outcome::WrongOutputs => {
                        if !batch.is_empty() {
                            expected.process = Some((call.key, batch));
                        }
                    }
                    Outcome::Err | Outcome::Panic => {
                        expected.batch_finished = Some(call.key);
                        for input in batch {
                            expected
                                .finished
                                .insert(input, Expected::after(&call.kind, outcome, input));
                        }
                    }
                }
            }
            CallKind::Process(inputs) => {
                expected.batch_finished = Some(call.key);
                for &input in inputs {
                    if self.pending.contains_key(&input) {
                        expected
                            .finished
                            .insert(input, Expected::after(&call.kind, outcome, input));
                    }
                }
            }
        }
        // The receiver is only dropped if the call's task was cancelled.
        call.release
            .send(outcome)
            .expect("the worker should never cancel user code");
        expected
    }

    async fn add(&mut self, key: Key) {
        let input = self.next_input;
        self.next_input += 1;
        let queued_before = self.queued[key].len();
        let in_flight = self.in_flight(key);
        // While shutting down, the worker accepts items until it has no work left, then exits.
        let exited =
            self.shut_down && self.held.is_empty() && self.queued.iter().all(VecDeque::is_empty);

        let batcher = self.batcher.clone();
        self.pending.insert(
            input,
            tokio::spawn(async move { batcher.add(key, input).await }),
        );
        self.settle().await;

        let mut finished = self.take_finished();
        match finished.remove(&input) {
            None => {
                assert!(
                    !exited,
                    "add {input} should fail, since the worker has exited"
                );
                assert!(
                    queued_before < self.config.max_key_queued_items,
                    "add {input} should have been rejected, with {queued_before} items queued"
                );
                self.queued[key].push_back(QueuedItem {
                    input,
                    received_at: Instant::now(),
                });
            }
            Some(Err(BatchError::Rejected(RejectionReason::BatchQueueFull(concurrency)))) => {
                assert!(
                    queued_before >= self.config.max_key_queued_items,
                    "add {input} should only be rejected when the queue is full, with \
                    {queued_before} items queued"
                );
                let maxed_out = in_flight.0 + in_flight.1 >= self.config.max_key_concurrency;
                assert!(
                    matches!(
                        (concurrency, maxed_out),
                        (ConcurrencyStatus::MaxedOut, true) | (ConcurrencyStatus::Available, false)
                    ),
                    "add {input} rejected with concurrency {concurrency:?}, with {in_flight:?} \
                    (acquiring, processing)"
                );
            }
            Some(Err(BatchError::Tx)) if exited => {}
            Some(result) => panic!("add {input} should be pending or rejected, got {result:?}"),
        }
        self.receive_calls(None, None);
        assert!(
            finished.is_empty(),
            "adding {input} shouldn't finish other adds, but finished {finished:?}"
        );
        self.check_progress();
    }

    /// Release every held call, after shutting down, and check that shutdown completes.
    async fn finish(&mut self) {
        if !self.shut_down {
            self.step(Op::ShutDown).await;
        }
        let worker = Arc::clone(&self.worker);
        let shutdown = tokio::spawn(async move { worker.wait_for_shutdown().await });
        // Releases can start more calls, but only for items already queued, so this ends.
        for _ in 0..10_000 {
            if self.held.is_empty() {
                break;
            }
            self.settle().await;
            assert!(
                !shutdown.is_finished(),
                "shutdown shouldn't complete while calls are held: {:?}",
                self.held
                    .iter()
                    .map(|call| (call.key, &call.kind))
                    .collect::<Vec<_>>()
            );
            self.step(Op::Release(0, Outcome::Ok)).await;
        }
        assert!(
            self.held.is_empty(),
            "the worker kept starting calls after shutdown, with nothing left to add"
        );
        assert!(
            self.pending.is_empty(),
            "every add should have finished after shutdown, but {:?} are still pending",
            self.pending.keys()
        );
        tokio::time::timeout(Duration::from_secs(3600), shutdown)
            .await
            .expect("shutdown should complete once every call has returned")
            .unwrap();
        let result = self.batcher.add(0, 0).await;
        assert!(
            matches!(result, Err(BatchError::Tx)),
            "the worker should have exited once shutdown completed, but an add got {result:?}"
        );
    }

    /// Take the items an acquisition's batch should take once it returns: up to `max_batch_size`
    /// of the oldest.
    fn take_batch(&mut self, key: Key) -> Vec<Input> {
        let queue = &mut self.queued[key];
        let size = queue.len().min(self.config.max_batch_size);
        queue.drain(..size).map(|item| item.input).collect()
    }

    /// Hold the processor calls started by the last step, checking they're the ones expected.
    ///
    /// `batch_finished` is the key whose batch the step finished, if any.
    fn receive_calls(
        &mut self,
        mut expected_process: Option<(Key, Vec<Input>)>,
        batch_finished: Option<Key>,
    ) {
        // The worker starts a returned acquisition's batch before asking the policy whether to
        // start more acquisitions, whatever order the calls arrive in.
        let mut in_flight: Vec<_> = (0..self.config.keys)
            .map(|key| {
                let (acquiring, processing) = self.in_flight(key);
                let starting = expected_process.as_ref().is_some_and(|(k, _)| *k == key);
                (acquiring, processing + usize::from(starting))
            })
            .collect();
        let acquiring_before: Vec<_> = in_flight.iter().map(|&(acquiring, _)| acquiring).collect();

        let mut calls = Vec::new();
        while let Ok(call) = self.calls.try_recv() {
            calls.push(call);
        }
        // At shutdown the worker starts acquisitions for each key in `HashMap` order, which
        // varies between runs. Sorting keeps `Op::Release` indices pointing at the same calls, so
        // failures shrink and replay. Each key's own calls keep their order.
        calls.sort_by_key(|call| call.key);

        for call in calls {
            match &call.kind {
                CallKind::Process(inputs) => assert_eq!(
                    Some((call.key, inputs.clone())),
                    expected_process.take(),
                    "only a returned acquisition should start processing, with the oldest items \
                    queued for its key"
                ),
                CallKind::Acquire => {
                    let (acquiring, processing) = in_flight[call.key];
                    // Only the first acquisition the worker starts for a key follows the step's
                    // event. It asks again after each one.
                    let first = acquiring == acquiring_before[call.key];
                    let batch_finished = first && batch_finished == Some(call.key);
                    assert!(
                        self.policy_allows(call.key, acquiring, processing, batch_finished),
                        "key {} started acquiring resources before its policy allows, with {} \
                        items queued, {acquiring} acquiring and {processing} processing",
                        call.key,
                        self.queued[call.key].len(),
                    );
                    in_flight[call.key].0 += 1;
                }
            }
            self.held.push(call);
        }
        assert_eq!(
            expected_process, None,
            "a returned acquisition should start processing its batch"
        );
        // The worker decided after the batch finished, which `check_progress` can't know.
        if let Some(key) = batch_finished {
            let (acquiring, processing) = in_flight[key];
            if acquiring == acquiring_before[key] {
                assert!(
                    !self.policy_allows(key, acquiring, processing, true),
                    "key {key} should have started acquiring resources once a batch finished, \
                    with {} items queued, {acquiring} acquiring and {processing} processing",
                    self.queued[key].len(),
                );
            }
        }

        for key in 0..self.config.keys {
            let (acquiring, processing) = self.in_flight(key);
            assert!(
                acquiring + processing <= self.config.max_key_concurrency,
                "key {key} has {acquiring} acquiring and {processing} processing, over the limit \
                of {}",
                self.config.max_key_concurrency
            );
        }
        for call in &self.held {
            if let CallKind::Process(inputs) = &call.kind {
                assert!(
                    (1..=self.config.max_batch_size).contains(&inputs.len()),
                    "batch {inputs:?} should have 1 to {} items",
                    self.config.max_batch_size
                );
            }
        }
    }

    /// Take the results of the adds which have finished.
    fn take_finished(&mut self) -> BTreeMap<Input, BatchResult<Input, String>> {
        let finished: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, handle)| handle.is_finished())
            .map(|(&input, _)| input)
            .collect();
        finished
            .into_iter()
            .map(|input| {
                let result = self
                    .pending
                    .remove(&input)
                    .unwrap()
                    .now_or_never()
                    .expect("finished")
                    .expect("add shouldn't panic");
                (input, result)
            })
            .collect()
    }

    /// (acquiring, processing) calls held for a key.
    fn in_flight(&self, key: Key) -> (usize, usize) {
        let calls = self.held.iter().filter(|call| call.key == key);
        let acquiring = calls
            .clone()
            .filter(|call| call.kind == CallKind::Acquire)
            .count();
        (acquiring, calls.count() - acquiring)
    }

    /// Check no key is waiting when its policy says it should be acquiring resources for a batch.
    fn check_progress(&self) {
        for key in 0..self.config.keys {
            let (acquiring, processing) = self.in_flight(key);
            assert!(
                !self.policy_allows(key, acquiring, processing, false),
                "key {key} should be acquiring resources for a batch, with {} items queued, \
                {acquiring} acquiring and {processing} processing",
                self.queued[key].len(),
            );
        }
    }

    /// Does the policy let a key start another acquisition, with this many batches acquiring and
    /// processing, and its queued items as the harness sees them?
    ///
    /// The worker must start one whenever this is true, and only then. `batch_finished` says
    /// whether the worker is deciding straight after one of the key's batches finished.
    fn policy_allows(
        &self,
        key: Key,
        acquiring: usize,
        processing: usize,
        batch_finished: bool,
    ) -> bool {
        let max_batch_size = self.config.max_batch_size;
        if acquiring + processing >= self.config.max_key_concurrency {
            return false;
        }
        // Each acquisition will take up to `max_batch_size` of the oldest items.
        let queue = &self.queued[key];
        let uncovered: Vec<_> = queue
            .range(queue.len().min(acquiring * max_batch_size)..)
            .collect();
        let Some(oldest) = uncovered.first() else {
            return false;
        };
        // Once shutting down, the worker starts batches without waiting for them to fill up.
        self.shut_down
            || match self.config.policy {
                Policy::Immediate => true,
                Policy::Size => uncovered.len() >= max_batch_size,
                Policy::Duration {
                    duration,
                    process_on_full,
                } => {
                    oldest.received_at + duration <= Instant::now()
                        || (process_on_full && uncovered.len() >= max_batch_size)
                }
                Policy::Balanced { min_size_hint } => {
                    processing == 0
                        || uncovered.len() >= min_size_hint.min(max_batch_size)
                        || batch_finished
                }
            }
    }
}
