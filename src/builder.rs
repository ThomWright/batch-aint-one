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
    F: BatchFn<I, O> + Send,
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
    K: 'static + Send + Eq + Hash + Clone,
    I: 'static + Send,
    O: 'static + Send,
    F: 'static + Send + Clone + BatchFn<I, O>,
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
