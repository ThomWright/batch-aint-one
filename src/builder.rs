use std::{hash::Hash, time::Duration};

use crate::{
    limit::{LimitStrategy, Limits},
    BatchFn, Batcher, DurationLimit, SizeLimit,
};

pub struct BatcherBuilder<K, I, O, F> {
    process_batch: F,
    limits: Option<Limits<K, I, O>>,
}

impl<K, I, O, F> BatcherBuilder<K, I, O, F>
where
    F: BatchFn<I, O> + Send + Sync,
{
    pub fn new(process_batch: F) -> Self {
        Self {
            process_batch,
            limits: None,
        }
    }
}

impl<K, I, O, F> BatcherBuilder<K, I, O, F>
where
    K: Send + Sync + 'static + Eq + Hash + Clone,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: BatchFn<I, O> + Send + Sync + 'static + Clone,
{
    pub fn with_limit(mut self, limit: impl LimitStrategy<K, I, O> + 'static) -> Self {
        self.limits
            .get_or_insert_with(Vec::default)
            .push(Box::new(limit));

        self
    }

    pub fn build(self) -> Batcher<K, I, O> {
        Batcher::new(
            self.process_batch,
            self.limits.unwrap_or_else(|| {
                vec![
                    Box::new(SizeLimit::new(10)),
                    Box::new(DurationLimit::new(Duration::from_millis(10))),
                ]
            }),
        )
    }
}
