use std::{hash::Hash, time::Duration};

use crate::{
    limit::{LimitStrategy, Limits},
    BatchFn, Batcher, DurationLimit, SizeLimit,
};

pub struct BatcherBuilder<K, I, O, F> {
    processor: F,
    limits: Option<Limits<K, I, O>>,
}

impl<K, I, O, F> BatcherBuilder<K, I, O, F>
where
    F: BatchFn<I, O> + Send + Sync,
{
    pub fn new(processor: F) -> Self {
        Self {
            processor,
            limits: None,
        }
    }
}

impl<K, I, O, F> BatcherBuilder<K, I, O, F>
where
    K: 'static + Send + Sync + Eq + Hash + Clone,
    I: 'static + Send + Sync,
    O: 'static + Send + Sync,
    F: 'static + Send + Sync + Clone + BatchFn<I, O>,
{
    /// Multiple limits can be set. The batch will be processed whenever one is reached.
    pub fn with_limit(mut self, limit: impl LimitStrategy<K, I, O> + 'static) -> Self {
        self.limits
            .get_or_insert_with(Vec::default)
            .push(Box::new(limit));

        self
    }

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
