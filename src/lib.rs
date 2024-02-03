//! Batch up multiple items for processing as a single unit.
//!
//! _I got 99 problems, but a batch ain't one..._
//!
//! Sometimes it is more efficient to process many items at once rather than one at a time.
//! Especially when the processing step has overheads which can be shared between many items.
//!
//! A worker task is run in the background and items are submitted to it for batching. Batches are
//! processed in their own tasks, concurrently.
//!
//! See the README for an example.

#![deny(missing_docs)]

#[cfg(doctest)]
use doc_comment::doctest;
#[cfg(doctest)]
doctest!("../README.md");

mod batch;
mod batcher;
mod error;
mod policies;
mod worker;

pub use batcher::{Batcher, Processor};
pub use error::BatchError;
pub use policies::{BatchingPolicy, Limits, OnFull};

#[cfg(test)]
mod test {
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::join;
    use tracing::{span, Instrument};

    use crate::{Batcher, BatchingPolicy, Limits, Processor};

    #[derive(Debug, Clone)]
    pub struct SimpleBatchProcessor(pub Duration);

    #[async_trait]
    impl Processor<String, String, String> for SimpleBatchProcessor {
        async fn process(
            &self,
            key: String,
            inputs: impl Iterator<Item = String> + Send,
        ) -> Result<Vec<String>, String> {
            tokio::time::sleep(self.0).await;
            Ok(inputs.map(|s| s + " processed for " + &key).collect())
        }
    }

    #[tokio::test]
    async fn test_tracing() {
        use tracing::Level;
        use tracing_capture::{CaptureLayer, SharedStorage};
        use tracing_subscriber::layer::SubscriberExt;

        let subscriber = tracing_subscriber::fmt()
            .pretty()
            .with_max_level(Level::INFO)
            .finish();
        // Add the capturing layer.
        let storage = SharedStorage::default();
        let subscriber = subscriber.with(CaptureLayer::new(&storage));

        // Capture tracing information.
        let _guard = tracing::subscriber::set_default(subscriber);

        let batcher = Batcher::new(
            SimpleBatchProcessor(Duration::ZERO),
            Limits::default().max_batch_size(2),
            BatchingPolicy::Size,
        );

        let h1 = {
            tokio_test::task::spawn({
                let span = span!(Level::INFO, "test_handler_span1");

                batcher
                    .add("A".to_string(), "1".to_string())
                    .instrument(span)
            })
        };
        let h2 = {
            tokio_test::task::spawn({
                let span = span!(Level::INFO, "test_handler_span2");

                batcher
                    .add("A".to_string(), "2".to_string())
                    .instrument(span)
            })
        };

        let (_o1, _o2) = join!(h1, h2);

        let storage = storage.lock();

        let process_span: Vec<_> = storage
            .all_spans()
            .filter(|span| span.metadata().name().contains("process batch"))
            .collect();
        assert_eq!(
            process_span.len(),
            1,
            "should be a single span for processing the batch"
        );

        assert_eq!(
            process_span.first().unwrap().follows_from().len(),
            2,
            "should follow from both handler spans"
        );
    }
}
