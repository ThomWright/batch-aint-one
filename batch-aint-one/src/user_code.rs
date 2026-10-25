//! Calls to user code: [`Processor::acquire_resources`] and [`Processor::process`].
//!
//! Each call runs in its own task, so a panic is caught and reported as [`BatchError::Panic`].

use std::{fmt::Display, sync::Arc};

use futures::FutureExt;
use tokio::task::JoinError;
use tracing::{Instrument, Level, Span, instrument::WithSubscriber, span};

use crate::{BatchError, error::ProcessorInvariantViolation, processor::Processor};

/// Acquire resources for a batch, returning them with the span they were acquired in.
pub(crate) async fn acquire_resources<P: Processor>(
    processor: P,
    batcher_name: &str,
    key: P::Key,
) -> Result<(P::Resources, Span), BatchError<P::Error>> {
    let span = span!(Level::INFO, "acquire resources", batch.name = batcher_name, batch.key = ?key);

    let result = tokio::spawn(
        {
            let span = span.clone();
            async move { processor.acquire_resources(key).instrument(span).await }
        }
        .with_current_subscriber(),
    )
    .await
    .map_err(join_error_to_batch_error)
    .and_then(|r| r.map_err(BatchError::ResourceAcquisitionFailed));

    result.map(|resources| (resources, span))
}

/// Process a batch's inputs, returning an output or an error for each one.
pub(crate) async fn process<P: Processor>(
    processor: P,
    batcher_name: Arc<str>,
    key: P::Key,
    inputs: Vec<P::Input>,
    resources: P::Resources,
    parent_span: Span,
) -> Vec<Result<P::Output, BatchError<P::Error>>> {
    let batch_size = inputs.len();

    let result = tokio::spawn(
        process_inner(
            processor,
            batcher_name,
            key,
            inputs,
            resources,
            parent_span.clone(),
        )
        .instrument(parent_span)
        .with_current_subscriber(),
    )
    .await
    .map_err(join_error_to_batch_error)
    .and_then(|r| r);

    match result {
        Ok(outputs) => outputs.into_iter().map(Ok).collect(),

        // If we failed to process the batch, return the error for each item in the batch.
        Err(err) => std::iter::repeat_n(err, batch_size).map(Err).collect(),
    }
}

async fn process_inner<P: Processor>(
    processor: P,
    batcher_name: Arc<str>,
    key: P::Key,
    inputs: Vec<P::Input>,
    resources: P::Resources,
    parent_span: Span,
) -> Result<Vec<P::Output>, BatchError<P::Error>> {
    let batch_size = inputs.len();

    let inner_span = span!(
        parent: &parent_span,
        Level::INFO,
        "process()",
        batch.name = &*batcher_name,
        batch.key = ?key,
        batch.size = batch_size as u64
    );

    let outputs = processor
        .process(key, inputs.into_iter(), resources)
        .instrument(inner_span)
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

fn join_error_to_batch_error<E: Display>(join_err: JoinError) -> BatchError<E> {
    if join_err.is_cancelled() {
        BatchError::Cancelled
    } else {
        BatchError::Panic
    }
}
