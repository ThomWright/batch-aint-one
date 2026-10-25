use std::fmt::{self, Display};

use bon::bon;

/// Limits on batch sizes, queueing and concurrency.
///
/// Most limits apply per key. `max_items_awaiting_worker` applies to the whole batcher.
///
/// `max_key_concurrency * max_batch_size` is the maximum number of items that can be processed
/// concurrently for a key.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Limits {
    pub(crate) max_batch_size: usize,
    pub(crate) max_key_concurrency: usize,
    pub(crate) max_key_queued_items: usize,
    pub(crate) max_items_awaiting_worker: usize,
}

#[bon]
impl Limits {
    /// Create new limits.
    ///
    /// # Panics
    ///
    /// Panics if any of the limits are zero.
    #[builder]
    pub fn new(
        /// Limits the maximum size of a batch.
        ///
        /// Defaults to 100.
        #[builder(default = 100)]
        max_batch_size: usize,
        /// Limits the maximum number of batches that can be processed concurrently for a key,
        /// including resource acquisition.
        ///
        /// Defaults to 10.
        #[builder(default = 10)]
        max_key_concurrency: usize,
        /// Limits the number of items that can be queued for a key. Once reached, new items for
        /// the key are rejected.
        ///
        /// Items are queued until their batch starts processing, including while resources are
        /// being acquired for it.
        ///
        /// Defaults to `max_batch_size * max_key_concurrency * 2`.
        max_key_queued_items: Option<usize>,
        // bon can't mark a generated setter deprecated, so it's hidden, and the deprecated
        // `LimitsBuilder::max_batch_queue_size` setters wrap it.
        #[builder(setters(vis = "", name = max_batch_queue_size_internal))]
        max_batch_queue_size: Option<usize>,
        /// Limits the number of items, across all keys, that have been submitted with
        /// [`Batcher::add()`](crate::Batcher::add) but not yet accepted by the background
        /// worker. Once this is reached, `add()` waits for room before submitting its item.
        ///
        /// This is a buffer for when the worker itself falls behind, e.g. when the runtime is
        /// short of CPU. It is not a queue limit, and on its own it does not shed load:
        ///
        /// - The worker usually accepts items much faster than they are processed. When
        ///   processing is the bottleneck, the per-key limits reject items and this buffer stays
        ///   nearly empty.
        /// - It bounds the buffer, not the number of callers waiting for room. Waiting slows down
        ///   callers that limit their own concurrency, but callers that spawn a task per request
        ///   just queue up in those tasks instead.
        /// - It is shared by all keys, so items for a key whose queue is full still take up room
        ///   until the worker reads and rejects them.
        ///
        /// A larger buffer hides more latency when the worker is overloaded, and a smaller one
        /// makes callers wait sooner. To shed load, rely on the per-key limits, and limit how
        /// many calls to `add()` are in flight. Time spent waiting for room is included in the
        /// `channel_duration` reported to
        /// [`MetricsRecorder::item_received()`](crate::MetricsRecorder::item_received).
        ///
        /// Defaults to the maximum number of items one key can have in the system,
        /// `max_batch_size * max_key_concurrency + max_key_queued_items`. With many keys and
        /// small per-key limits, consider raising it, because the default may be smaller than
        /// normal bursts across all keys.
        max_items_awaiting_worker: Option<usize>,
    ) -> Self {
        assert!(
            max_batch_size > 0,
            "max_batch_size must be greater than zero"
        );
        assert!(
            max_key_concurrency > 0,
            "max_key_concurrency must be greater than zero"
        );
        assert!(
            max_batch_queue_size != Some(0),
            "max_batch_queue_size must be greater than zero"
        );
        let max_key_queued_items = max_key_queued_items
            .or(max_batch_queue_size.map(|batches| batches * max_batch_size))
            .unwrap_or(max_batch_size * max_key_concurrency * 2);
        assert!(
            max_key_queued_items > 0,
            "max_key_queued_items must be greater than zero"
        );

        let max_items_awaiting_worker = max_items_awaiting_worker
            .unwrap_or(max_batch_size * max_key_concurrency + max_key_queued_items);
        assert!(
            max_items_awaiting_worker > 0,
            "max_items_awaiting_worker must be greater than zero"
        );

        Self {
            max_batch_size,
            max_key_concurrency,
            max_key_queued_items,
            max_items_awaiting_worker,
        }
    }
}

impl<S: limits_builder::State> LimitsBuilder<S> {
    /// Limits the number of items that can be queued for a key to
    /// `max_batch_queue_size * max_batch_size`.
    ///
    /// Ignored if `max_key_queued_items` is set.
    #[deprecated(note = "use `max_key_queued_items`, which counts items")]
    pub fn max_batch_queue_size(
        self,
        max_batch_queue_size: usize,
    ) -> LimitsBuilder<limits_builder::SetMaxBatchQueueSize<S>>
    where
        S::MaxBatchQueueSize: limits_builder::IsUnset,
    {
        self.max_batch_queue_size_internal(max_batch_queue_size)
    }

    /// Like [`max_batch_queue_size`](Self::max_batch_queue_size), but `None` leaves it unset.
    #[deprecated(note = "use `maybe_max_key_queued_items`, which counts items")]
    pub fn maybe_max_batch_queue_size(
        self,
        max_batch_queue_size: Option<usize>,
    ) -> LimitsBuilder<limits_builder::SetMaxBatchQueueSize<S>>
    where
        S::MaxBatchQueueSize: limits_builder::IsUnset,
    {
        self.maybe_max_batch_queue_size_internal(max_batch_queue_size)
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl Display for Limits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "batch_size: {}, key_concurrency: {}, key_queued_items: {}, items_awaiting_worker: {}",
            self.max_batch_size,
            self.max_key_concurrency,
            self.max_key_queued_items,
            self.max_items_awaiting_worker
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_builder_defaults() {
        let default = Limits::default();
        let built = Limits::builder().build();

        assert_eq!(default.max_batch_size, built.max_batch_size);
        assert_eq!(default.max_key_concurrency, built.max_key_concurrency);
        assert_eq!(default.max_key_queued_items, built.max_key_queued_items);
        assert_eq!(
            default.max_items_awaiting_worker,
            built.max_items_awaiting_worker
        );
    }

    #[test]
    fn max_key_queued_items_defaults_to_twice_the_items_processed_concurrently() {
        let limits = Limits::builder()
            .max_batch_size(2)
            .max_key_concurrency(3)
            .build();

        assert_eq!(limits.max_key_queued_items, 2 * 3 * 2);
    }

    #[test]
    #[allow(deprecated)]
    fn max_batch_queue_size_counts_batches_worth_of_items() {
        let limits = Limits::builder()
            .max_batch_size(2)
            .max_batch_queue_size(4)
            .build();

        assert_eq!(limits.max_key_queued_items, 2 * 4);
    }

    #[test]
    #[allow(deprecated)]
    fn max_key_queued_items_overrides_max_batch_queue_size() {
        let limits = Limits::builder()
            .max_batch_size(2)
            .max_batch_queue_size(4)
            .max_key_queued_items(5)
            .build();

        assert_eq!(limits.max_key_queued_items, 5);
    }

    #[test]
    fn max_items_awaiting_worker_defaults_to_max_items_per_key() {
        let limits = Limits::builder()
            .max_batch_size(2)
            .max_key_concurrency(3)
            .max_key_queued_items(4)
            .build();

        assert_eq!(limits.max_items_awaiting_worker, 2 * 3 + 4);
    }

    #[test]
    fn max_items_awaiting_worker_can_be_overridden() {
        let limits = Limits::builder().max_items_awaiting_worker(5).build();

        assert_eq!(limits.max_items_awaiting_worker, 5);
    }

    #[test]
    #[should_panic(expected = "max_items_awaiting_worker must be greater than zero")]
    fn rejects_zero_max_items_awaiting_worker() {
        Limits::builder().max_items_awaiting_worker(0).build();
    }

    #[test]
    #[should_panic(expected = "max_batch_size must be greater than zero")]
    fn rejects_zero_max_batch_size() {
        Limits::builder().max_batch_size(0).build();
    }

    #[test]
    #[should_panic(expected = "max_key_concurrency must be greater than zero")]
    fn rejects_zero_max_key_concurrency() {
        Limits::builder().max_key_concurrency(0).build();
    }

    #[test]
    #[should_panic(expected = "max_key_queued_items must be greater than zero")]
    fn rejects_zero_max_key_queued_items() {
        Limits::builder().max_key_queued_items(0).build();
    }

    #[test]
    #[allow(deprecated)]
    #[should_panic(expected = "max_batch_queue_size must be greater than zero")]
    fn rejects_zero_max_batch_queue_size() {
        Limits::builder().max_batch_queue_size(0).build();
    }

    #[test]
    fn limits_builder_methods() {
        let limits = Limits::builder()
            .max_batch_size(50)
            .max_key_concurrency(5)
            .build();

        assert_eq!(limits.max_batch_size, 50);
        assert_eq!(limits.max_key_concurrency, 5);
    }
}
