use std::{hash::Hash, time::Duration};

use crate::{
    limit::{DurationLimit, LimitStrategy, Limits, SizeLimit},
    Batcher, Processor,
};

/// Builds [Batcher]s.
pub struct BatcherBuilder<K, I, O, F> {
    processor: F,
    limits: Option<Limits<K, I, O>>,
}

impl<K, I, O, F> BatcherBuilder<K, I, O, F>
where
    F: 'static + Send + Clone + Processor<I, O>,
{
    /// Create a [BatcherBuilder] with the given batch processor.
    pub fn new(processor: F) -> Self {
        Self {
            processor,
            limits: None,
        }
    }
}

impl<K, I, O, F> BatcherBuilder<K, I, O, F>
where
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    F: 'static + Send + Clone + Processor<I, O>,
{
    /// Multiple limits can be set. The batch will be processed whenever one is reached.
    pub fn with_limit(mut self, limit: impl LimitStrategy<K, I, O> + 'static) -> Self {
        self.limits
            .get_or_insert_with(Vec::default)
            .push(Box::new(limit));

        self
    }

    /// Create a new [Batcher].
    pub fn build(self) -> Batcher<K, I, O> {
        Batcher::new(
            self.processor,
            self.limits.unwrap_or_else(|| {
                vec![
                    Box::new(SizeLimit::new(10)),
                    Box::new(DurationLimit::new(Duration::from_millis(10))),
                ]
            }),
        )
    }
}
