//! Batching policies that control when batches get processed.
//!
//! A policy is a pure decision layer: given what happened to a key and the key's [`KeyState`], it
//! decides whether to start a batch, reject an item, or wake up later. The worker carries the
//! decisions out.

use std::{
    fmt::{self, Debug, Display},
    time::Duration,
};

use tokio::time::Instant;

use crate::{
    Limits,
    error::{ConcurrencyStatus, RejectionReason},
};

/// A policy controlling when batches get processed.
///
/// Each policy is optimised for a different use case:
///
/// - [`Immediate`](Self::Immediate): Prioritises low latency
/// - [`Size`](Self::Size): Prioritises high batch utilisation
/// - [`Duration`](Self::Duration): Prioritises regularity
/// - [`Balanced`](Self::Balanced): Balances resource efficiency and latency
///
/// The policy decides when to acquire resources for a new batch, with
/// [`Processor::acquire_resources`](crate::Processor::acquire_resources). Once they've been
/// acquired, the batch takes the oldest items queued for its key, up to the maximum batch size,
/// and processes them.
///
/// Batches never exceed the concurrency limits in [`Limits`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BatchingPolicy {
    /// Process items as soon as possible.
    ///
    /// When concurrency is available, resources for a batch are acquired as soon as an item
    /// arrives, so the item may be processed on its own. Items which arrive during that
    /// acquisition join the batch, so batches grow under load while latency stays low.
    ///
    /// When concurrency is maxed out, items queue, and resources for the next batch are acquired
    /// as soon as a batch finishes. When concurrency is limited to 1, batches run serially.
    ///
    /// Prioritises low latency.
    Immediate,

    /// Acquire resources for a batch once enough items are queued to fill it.
    ///
    /// Prioritises high batch utilisation.
    Size,

    /// Acquire resources for a batch once the oldest queued item has waited for the given
    /// duration.
    ///
    /// If using `OnFull::Process`, also do so as soon as enough items are queued to fill a batch,
    /// whichever happens first.
    ///
    /// Prioritises regularity.
    Duration(Duration, OnFull),

    /// Balance between resource efficiency and latency based on system load.
    ///
    /// When no batches are processing, resources for a batch are acquired as soon as an item
    /// arrives.
    ///
    /// When batches are already processing:
    /// - If fewer than `min_size_hint` items are waiting: wait for either `min_size_hint` items or
    ///   any batch to complete
    /// - If at least `min_size_hint` items are waiting: acquire resources for a batch immediately
    ///
    /// The `min_size_hint` must be <= `max_batch_size`.
    ///
    /// Prioritises efficient resource usage while maintaining reasonable latency.
    Balanced {
        /// The minimum batch size to prefer before using additional concurrency.
        min_size_hint: usize,
    },
}

/// What to do when enough items are queued to fill a batch, for the
/// [`Duration`](BatchingPolicy::Duration) policy.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum OnFull {
    /// Acquire resources for a batch straight away, if concurrency is available. Otherwise, do so
    /// as soon as another batch finishes.
    Process,

    /// Don't acquire resources for a batch early. Once the key's queue is full (see [`Limits`]), new items are
    /// rejected.
    Reject,
}

/// What happened to a key, prompting the worker to ask its policy whether to start a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Event {
    ItemAdded,
    /// The worker just started a batch, and is asking whether to start another.
    BatchStarted,
    ResourcesAcquired,
    /// A batch finished processing, or failed to acquire resources.
    BatchFinished,
    /// A wake-up time the policy asked for has passed.
    WakeUp,
}

/// A key's queued items and work in flight, as its policy sees them.
#[derive(Debug)]
pub(crate) struct KeyState {
    pub(crate) queued: usize,
    /// How many batches are acquiring resources.
    pub(crate) acquiring: usize,
    /// How many batches are processing.
    pub(crate) processing: usize,
    /// When the oldest [uncovered](Self::uncovered) item was received, if there is one.
    pub(crate) oldest_uncovered_received_at: Option<Instant>,
    pub(crate) limits: Limits,
}

impl KeyState {
    /// How many queued items no batch acquiring resources will take.
    ///
    /// Each batch acquiring resources will take up to `max_batch_size` of the oldest items once
    /// they've been acquired.
    pub(crate) fn uncovered(&self) -> usize {
        self.queued
            .saturating_sub(self.acquiring * self.limits.max_batch_size)
    }

    /// Is there room for another batch within the concurrency limit, and an item for it to take?
    pub(crate) fn can_start_batch(&self) -> bool {
        !self.at_max_concurrency() && self.uncovered() >= 1
    }

    fn at_max_concurrency(&self) -> bool {
        self.acquiring + self.processing >= self.limits.max_key_concurrency
    }

    fn is_queue_full(&self) -> bool {
        self.queued >= self.limits.max_batch_queue_size * self.limits.max_batch_size
    }

    fn has_full_batch_uncovered(&self) -> bool {
        self.uncovered() >= self.limits.max_batch_size
    }
}

impl Display for BatchingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchingPolicy::Immediate => write!(f, "Immediate"),
            BatchingPolicy::Size => write!(f, "Size"),
            BatchingPolicy::Duration(duration, on_full) => {
                write!(f, "Duration({}ms, {:?})", duration.as_millis(), on_full)
            }
            BatchingPolicy::Balanced { min_size_hint } => {
                write!(f, "Balanced(min_size: {})", min_size_hint)
            }
        }
    }
}

impl BatchingPolicy {
    /// Normalise the policy to ensure it's valid for the given limits.
    pub(crate) fn normalise(self, limits: Limits) -> Self {
        match self {
            BatchingPolicy::Balanced { min_size_hint } => BatchingPolicy::Balanced {
                min_size_hint: min_size_hint.min(limits.max_batch_size),
            },
            other => other,
        }
    }

    /// Should a new item be rejected? Applied before adding it.
    pub(crate) fn should_reject(&self, state: &KeyState) -> Option<RejectionReason> {
        if !state.is_queue_full() {
            return None;
        }
        let concurrency = if state.at_max_concurrency() {
            ConcurrencyStatus::MaxedOut
        } else {
            ConcurrencyStatus::Available
        };
        Some(RejectionReason::BatchQueueFull(concurrency))
    }

    /// Should another batch start, after `event`?
    pub(crate) fn should_start_batch(&self, event: Event, state: &KeyState, now: Instant) -> bool {
        if !state.can_start_batch() {
            return false;
        }
        match self {
            Self::Immediate => true,
            Self::Size => state.has_full_batch_uncovered(),
            Self::Duration(duration, on_full) => {
                let waited_long_enough = state
                    .oldest_uncovered_received_at
                    .is_some_and(|received_at| received_at + *duration <= now);
                // A full batch also starts when another batch finishes, whatever `on_full` says.
                let start_when_full =
                    matches!(on_full, OnFull::Process) || event == Event::BatchFinished;
                waited_long_enough || (start_when_full && state.has_full_batch_uncovered())
            }
            Self::Balanced { min_size_hint } => {
                // While other batches are processing, only use more concurrency for a big enough
                // batch, but let a smaller batch take the place of one which finished.
                state.processing == 0
                    || state.uncovered() >= *min_size_hint
                    || event == Event::BatchFinished
            }
        }
    }

    /// When the worker should next ask whether to start a batch, if the policy depends on time.
    pub(crate) fn wake_up_at(&self, state: &KeyState) -> Option<Instant> {
        match self {
            Self::Duration(duration, _) => state
                .oldest_uncovered_received_at
                .map(|received_at| received_at + *duration),
            Self::Immediate | Self::Size | Self::Balanced { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    fn limits(max_batch_size: usize, max_key_concurrency: usize) -> Limits {
        Limits::builder()
            .max_batch_size(max_batch_size)
            .max_key_concurrency(max_key_concurrency)
            .build()
    }

    fn state(limits: Limits, queued: usize, acquiring: usize, processing: usize) -> KeyState {
        KeyState {
            queued,
            acquiring,
            processing,
            oldest_uncovered_received_at: None,
            limits,
        }
    }

    mod key_state {
        use super::*;

        #[test]
        fn acquiring_batches_cover_up_to_max_batch_size_each() {
            assert_eq!(state(limits(10, 3), 25, 2, 0).uncovered(), 5);
            assert_eq!(state(limits(10, 3), 15, 2, 0).uncovered(), 0);
        }

        #[test]
        fn can_start_batch_needs_capacity_and_an_uncovered_item() {
            let limits = limits(10, 2);
            assert!(state(limits, 1, 0, 0).can_start_batch());
            assert!(!state(limits, 0, 0, 0).can_start_batch(), "no items");
            assert!(!state(limits, 10, 1, 0).can_start_batch(), "all covered");
            assert!(!state(limits, 11, 1, 1).can_start_batch(), "no capacity");
        }
    }

    mod rejection {
        use super::*;

        #[test]
        fn rejects_once_the_queue_holds_its_limit_of_batches_worth_of_items() {
            let limits = Limits::builder()
                .max_batch_size(5)
                .max_key_concurrency(1)
                .max_batch_queue_size(2)
                .build();
            let policy = BatchingPolicy::Size;

            assert_matches!(policy.should_reject(&state(limits, 9, 0, 0)), None);
            assert_matches!(
                policy.should_reject(&state(limits, 10, 0, 0)),
                Some(RejectionReason::BatchQueueFull(
                    ConcurrencyStatus::Available
                ))
            );
            assert_matches!(
                policy.should_reject(&state(limits, 10, 0, 1)),
                Some(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
            );
        }

        #[test]
        fn items_acquiring_resources_count_as_queued() {
            let limits = Limits::builder()
                .max_batch_size(5)
                .max_key_concurrency(1)
                .max_batch_queue_size(1)
                .build();

            assert_matches!(
                BatchingPolicy::Immediate.should_reject(&state(limits, 5, 1, 0)),
                Some(RejectionReason::BatchQueueFull(ConcurrencyStatus::MaxedOut))
            );
        }
    }

    mod immediate {
        use super::*;

        const POLICY: BatchingPolicy = BatchingPolicy::Immediate;

        #[test]
        fn starts_for_the_first_item() {
            let now = Instant::now();
            assert!(POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 1, 0, 0),
                now
            ));
        }

        #[test]
        fn items_join_the_batch_acquiring_resources() {
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 10, 1, 0),
                now
            ));
        }

        #[test]
        fn starts_another_once_the_acquiring_batch_is_full() {
            let now = Instant::now();
            assert!(POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 11, 1, 0),
                now
            ));
        }

        #[test]
        fn waits_at_max_concurrency() {
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 11, 1, 1),
                now
            ));
        }

        #[test]
        fn starts_when_a_batch_finishes() {
            let now = Instant::now();
            assert!(POLICY.should_start_batch(
                Event::BatchFinished,
                &state(limits(10, 1), 3, 0, 0),
                now
            ));
        }

        #[test]
        fn does_not_start_for_items_covered_by_a_failed_acquisition_not_yet_handled() {
            // Another batch finishes before the worker handles an acquisition failure. The
            // failing acquisition still covers its items, so they must not start another batch.
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::BatchFinished,
                &state(limits(1, 2), 1, 1, 0),
                now
            ));
        }
    }

    mod size {
        use super::*;

        const POLICY: BatchingPolicy = BatchingPolicy::Size;

        #[test]
        fn waits_for_a_full_batch() {
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(3, 2), 2, 0, 0),
                now
            ));
            assert!(POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(3, 2), 3, 0, 0),
                now
            ));
        }

        #[test]
        fn waits_for_a_full_batch_beyond_those_acquiring() {
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(3, 2), 5, 1, 0),
                now
            ));
            assert!(POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(3, 2), 6, 1, 0),
                now
            ));
        }

        #[test]
        fn waits_for_a_full_batch_after_another_finishes() {
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::BatchFinished,
                &state(limits(3, 2), 2, 0, 0),
                now
            ));
        }

        #[test]
        fn waits_at_max_concurrency() {
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(1, 1), 1, 0, 1),
                now
            ));
        }
    }

    mod duration {
        use super::*;

        const DURATION: Duration = Duration::from_millis(100);

        fn waited(limits: Limits, queued: usize, waited: Duration, now: Instant) -> KeyState {
            KeyState {
                oldest_uncovered_received_at: Some(now - waited),
                ..state(limits, queued, 0, 0)
            }
        }

        #[test]
        fn starts_once_the_oldest_item_has_waited() {
            let now = Instant::now() + DURATION;
            let policy = BatchingPolicy::Duration(DURATION, OnFull::Process);
            let limits = limits(10, 1);

            let early = waited(limits, 1, DURATION - Duration::from_millis(1), now);
            assert!(!policy.should_start_batch(Event::WakeUp, &early, now));
            let on_time = waited(limits, 1, DURATION, now);
            assert!(policy.should_start_batch(Event::WakeUp, &on_time, now));
        }

        #[test]
        fn wakes_up_when_the_oldest_uncovered_item_has_waited() {
            let now = Instant::now() + DURATION;
            let policy = BatchingPolicy::Duration(DURATION, OnFull::Process);

            let waiting = waited(limits(10, 1), 1, Duration::from_millis(30), now);
            assert_eq!(
                policy.wake_up_at(&waiting),
                Some(now + Duration::from_millis(70))
            );
            assert_eq!(policy.wake_up_at(&state(limits(10, 1), 0, 0, 0)), None);
        }

        #[test]
        fn process_on_full_starts_a_full_batch_early() {
            let now = Instant::now() + DURATION;
            let policy = BatchingPolicy::Duration(DURATION, OnFull::Process);
            let limits = limits(2, 1);

            let full = waited(limits, 2, Duration::ZERO, now);
            assert!(policy.should_start_batch(Event::ItemAdded, &full, now));
            let partial = waited(limits, 1, Duration::ZERO, now);
            assert!(!policy.should_start_batch(Event::ItemAdded, &partial, now));
        }

        #[test]
        fn reject_on_full_does_not_start_a_full_batch_early() {
            let now = Instant::now() + DURATION;
            let policy = BatchingPolicy::Duration(DURATION, OnFull::Reject);

            let full = waited(limits(2, 1), 2, Duration::ZERO, now);
            assert!(!policy.should_start_batch(Event::ItemAdded, &full, now));
        }

        #[test]
        fn starts_a_full_batch_when_another_finishes_even_with_reject_on_full() {
            let now = Instant::now() + DURATION;
            let policy = BatchingPolicy::Duration(DURATION, OnFull::Reject);

            let full = waited(limits(2, 1), 2, Duration::ZERO, now);
            assert!(policy.should_start_batch(Event::BatchFinished, &full, now));
        }

        #[test]
        fn waits_at_max_concurrency() {
            let now = Instant::now() + DURATION;
            let policy = BatchingPolicy::Duration(DURATION, OnFull::Process);

            let at_max = KeyState {
                processing: 1,
                ..waited(limits(2, 1), 1, DURATION, now)
            };
            assert!(!policy.should_start_batch(Event::WakeUp, &at_max, now));
        }
    }

    mod balanced {
        use super::*;

        const POLICY: BatchingPolicy = BatchingPolicy::Balanced { min_size_hint: 5 };

        #[test]
        fn starts_immediately_when_nothing_is_processing() {
            let now = Instant::now();
            assert!(POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 1, 0, 0),
                now
            ));
        }

        #[test]
        fn waits_below_the_hint_while_processing() {
            let now = Instant::now();
            assert!(!POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 4, 0, 1),
                now
            ));
        }

        #[test]
        fn starts_at_the_hint_while_processing() {
            let now = Instant::now();
            assert!(POLICY.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 5, 0, 1),
                now
            ));
        }

        #[test]
        fn starts_below_the_hint_when_a_batch_finishes() {
            let now = Instant::now();
            assert!(POLICY.should_start_batch(
                Event::BatchFinished,
                &state(limits(10, 2), 3, 0, 1),
                now
            ));
        }

        #[test]
        fn a_finished_batch_makes_room_for_only_one_small_batch() {
            let now = Instant::now();
            // After starting a batch for the finished one, the remaining items wait again.
            assert!(!POLICY.should_start_batch(
                Event::BatchStarted,
                &state(limits(10, 3), 13, 1, 1),
                now
            ));
        }

        #[test]
        fn hint_one_behaves_like_immediate() {
            let now = Instant::now();
            let policy = BatchingPolicy::Balanced { min_size_hint: 1 };
            assert!(policy.should_start_batch(
                Event::ItemAdded,
                &state(limits(10, 2), 1, 0, 1),
                now
            ));
        }
    }
}
