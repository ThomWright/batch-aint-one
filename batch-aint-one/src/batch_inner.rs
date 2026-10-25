use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::FutureExt;
use tokio::task::JoinError;
use tracing::{Instrument, Level, Span, instrument::WithSubscriber, span};

use crate::{BatchError, error::ProcessorInvariantViolation, processor::Processor};

/// The batcher name and key that batches and resource acquisitions run under.
///
/// Runs resource acquisition and processing, catching panics.
#[derive(Debug)]
pub(crate) struct BatchInner<P: Processor> {
    batcher_name: String,
    key: P::Key,
}

impl<P: Processor> BatchInner<P> {
    pub fn new(batcher_name: String, key: P::Key) -> Self {
        Self { batcher_name, key }
    }

    pub fn name(&self) -> &str {
        &self.batcher_name
    }

    pub fn key(&self) -> &P::Key {
        &self.key
    }

    pub async fn process(
        self: Arc<Self>,
        processor: P,
        inputs: Vec<P::Input>,
        resources: P::Resources,
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
        resources: P::Resources,
        parent_span: Span,
    ) -> Result<Vec<P::Output>, BatchError<P::Error>> {
        let key = self.key.clone();
        let name = self.batcher_name.clone();
        let batch_size = inputs.len();

        let inner_span = span!(
            parent: &parent_span,
            Level::INFO,
            "process()",
            batch.name = &name,
            batch.key = ?key,
            batch.size = batch_size as u64
        );

        let outputs = processor
            .process(key.clone(), inputs.into_iter(), resources)
            .instrument(inner_span.clone())
            .map(|r| r.map_err(BatchError::BatchFailed))
            .await?;

        if outputs.len() != batch_size {
            Err(BatchError::ProcessorInvariantViolation(
                ProcessorInvariantViolation::WrongNumberOfOutputs {
                    expected: batch_size,
                    actual: outputs.len(),
                },
            ))
        } else {
            Ok(outputs)
        }
    }

    pub async fn acquire_resources(
        self: Arc<Self>,
        processor: P,
    ) -> Result<(P::Resources, Span), BatchError<P::Error>> {
        let key = self.key.clone();

        let span = span!(Level::INFO, "acquire resources", batch.name = self.batcher_name, batch.key = ?key);

        let result = self.acquire_resources_inner(processor, span.clone()).await;

        result.map(|resources| (resources, span))
    }

    async fn acquire_resources_inner(
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
}

fn join_error_to_batch_error<E: Display>(join_err: JoinError) -> BatchError<E> {
    if join_err.is_cancelled() {
        BatchError::Cancelled
    } else {
        BatchError::Panic
    }
}
