use std::fmt::{self, Display};

use bon::bon;

/// A policy controlling limits on batch sizes and concurrency.
///
/// New items will be rejected when both the limits have been reached.
///
/// `max_key_concurrency * max_batch_size` is the number of items that can be processed concurrently.
///
/// `max_batch_queue_size * max_batch_size` is the number of items that can be queued.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Limits {
    pub(crate) max_batch_size: usize,
    pub(crate) max_key_concurrency: usize,
    pub(crate) max_batch_queue_size: usize,
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
        #[builder(default = 100)]
        max_batch_size: usize,
        /// Limits the maximum number of batches that can be processed concurrently for a key,
        /// including resource acquisition.
        #[builder(default = 10)]
        max_key_concurrency: usize,
        /// Limits the maximum number of batches that can be queued concurrently for a key.
        ///
        /// Defaults to `max_key_concurrency * 2`.
        max_batch_queue_size: Option<usize>,
    ) -> Self {
        assert!(
            max_batch_size > 0,
            "max_batch_size must be greater than zero"
        );
        assert!(
            max_key_concurrency > 0,
            "max_key_concurrency must be greater than zero"
        );
        let max_batch_queue_size = max_batch_queue_size.unwrap_or(max_key_concurrency * 2);
        assert!(
            max_batch_queue_size > 0,
            "max_batch_queue_size must be greater than zero"
        );

        Self {
            max_batch_size,
            max_key_concurrency,
            max_batch_queue_size,
        }
    }

    fn max_items_processing_per_key(&self) -> usize {
        self.max_batch_size * self.max_key_concurrency
    }

    fn max_items_queued_per_key(&self) -> usize {
        self.max_batch_size * self.max_batch_queue_size
    }

    /// The maximum number of items that can be in the system for a given key.
    pub(crate) fn max_items_in_system_per_key(&self) -> usize {
        self.max_items_processing_per_key() + self.max_items_queued_per_key()
    }
}

impl Default for Limits {
    fn default() -> Self {
        let max_batch_size = 100;
        let max_key_concurrency = 10;
        let max_batch_queue_size = max_key_concurrency;
        Self {
            max_batch_size,
            max_key_concurrency,
            max_batch_queue_size,
        }
    }
}

impl Display for Limits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "batch_size: {}, key_concurrency: {}, queue_size: {}",
            self.max_batch_size, self.max_key_concurrency, self.max_batch_queue_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
