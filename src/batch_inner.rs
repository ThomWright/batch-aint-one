use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::FutureExt;
use tokio::task::JoinError;
use tracing::{Instrument, Level, Span, instrument::WithSubscriber, span};

use crate::{BatchError, processor::Processor};

/// Core batch data.
///
/// Includes inner processing and resource acquisition.
#[derive(Debug)]
pub(crate) struct BatchInner<P: Processor> {
    batcher_name: String,
    key: P::Key,
    generation: Generation,
}

/// Generations are used to handle the case where a timer goes off after the associated batch has
/// already been processed, and a new batch has already been created with the same key.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub(crate) struct Generation(u32);

impl Generation {
    pub(crate) fn next(&self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

/// Core batch data and
impl<P: Processor> BatchInner<P> {
    pub fn new(batcher_name: String, key: P::Key) -> Self {
        Self {
            batcher_name,
            key,
            generation: Generation::default(),
        }
    }

    pub fn new_generation(&self) -> Self {
        Self {
            batcher_name: self.batcher_name.clone(),
            key: self.key.clone(),
            generation: self.generation.next(),
        }
    }

    pub fn name(&self) -> &str {
        &self.batcher_name
    }

    pub fn key(&self) -> &P::Key {
        &self.key
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub async fn process(
        self: Arc<Self>,
        processor: P,
        inputs: Vec<P::Input>,
        resources: Option<(P::Resources, Span)>,
        parent_span: Span,
    ) -> Vec<Result<P::Output, BatchError<P::Error>>> {
        let batch_size = inputs.len();

        // Spawn a task so we can catch panics.
        let result = tokio::spawn(
            self.process_inner(processor, inputs, resources, parent_span.clone())
                .instrument(parent_span)
                .with_current_subscriber(),
        )
        .await
        .map_err(join_error_to_batch_error)
        .and_then(|r| r);

        // Collect the outputs
        let outputs: Vec<Result<P::Output, BatchError<P::Error>>> = match result {
            Ok(outputs) => outputs.into_iter().map(Ok).collect(),

            // If we failed to process the batch, return the error for each item in the batch.
            Err(err) => std::iter::repeat_n(err, batch_size).map(Err).collect(),
        };

        outputs
    }

    async fn process_inner(
        self: Arc<Self>,
        processor: P,
        inputs: Vec<P::Input>,
        resources: Option<(P::Resources, Span)>,
        parent_span: Span,
    ) -> Result<Vec<P::Output>, BatchError<P::Error>> {
        let key = self.key.clone();
        let name = self.batcher_name.clone();
        let batch_size = inputs.len();

        // Acquire resources (if we don't have them already).
        let resources = self
            .acquire_resources(processor.clone(), resources, parent_span.clone())
            .await;

        // Early exit if we failed to acquire resources.
        let resources = match resources {
            Ok(resources) => resources,
            Err(err) => {
                return Err(BatchError::ResourceAcquisitionFailed(err));
            }
        };

        // Now process the batch.
        let inner_span = span!(
            parent: &parent_span,
            Level::INFO,
            "process()",
            batch.name = &name,
            batch.key = ?key,
            batch.size = batch_size as u64
        );
        processor
            .process(key.clone(), inputs.into_iter(), resources)
            .instrument(inner_span.clone())
            .map(|r| r.map_err(BatchError::BatchFailed))
            .await
    }

    pub async fn pre_acquire(
        self: Arc<Self>,
        processor: P,
    ) -> Result<(P::Resources, Span), BatchError<P::Error>> {
        let key = self.key.clone();

        let span = span!(Level::INFO, "pre-acquire resources", batch.name = self.batcher_name, batch.key = ?key);

        let result = self.pre_acquire_inner(processor, span.clone()).await;

        result.map(|resources| (resources, span))
    }

    async fn pre_acquire_inner(
        &self,
        processor: P,
        span: Span,
    ) -> Result<P::Resources, BatchError<P::Error>> {
        let key = self.key.clone();

        // Wrap resource acquisition in a spawn to catch panics
        tokio::spawn(
            async move {
                processor
                    .acquire_resources(key)
                    .instrument(span.clone())
                    .await
            }
            .with_current_subscriber(),
        )
        .await
        .map_err(join_error_to_batch_error)
        .and_then(|r| r.map_err(BatchError::ResourceAcquisitionFailed))
    }

    /// Acquire resources for processing, either from pre-acquisition or on-demand.
    async fn acquire_resources(
        self: Arc<Self>,
        processor: P,
        resources: Option<(P::Resources, Span)>,
        parent_span: Span,
    ) -> Result<P::Resources, P::Error> {
        // Try to use pre-acquired resources if available
        match resources {
            Some((resources, acquire_span)) => {
                // We've pre-acquired resources
                parent_span.follows_from(acquire_span);
                Ok(resources)
            }
            None => {
                // Not pre-acquired, so acquire now
                let acquire_span = span!(
                    parent: &parent_span,
                    Level::INFO,
                    "acquire resources",
                    batch.name = self.batcher_name,
                    batch.key = ?self.key
                );

                processor
                    .acquire_resources(self.key.clone())
                    .instrument(acquire_span.clone())
                    .await
            }
        }
    }
}

fn join_error_to_batch_error<E: Display>(join_err: JoinError) -> BatchError<E> {
    if join_err.is_cancelled() {
        BatchError::Cancelled
    } else {
        BatchError::Panic
    }
}
