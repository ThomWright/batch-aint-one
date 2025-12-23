//! Batch up multiple items for processing as a single unit.
//!
//! _I got 99 problems, but a batch ain't one..._
//!
//! Sometimes it is more efficient to process many items at once rather than one at a time.
//! Especially when the processing step has overheads which can be shared between many items.
//!
//! Often applications work with one item at a time, e.g. _select one row_ or _insert one row_. Many
//! of these operations can be batched up into more efficient versions: _select many rows_ and
//! _insert many rows_.
//!
//! A worker task is run in the background. Many client tasks (e.g. message handlers) can submit
//! items to the worker and wait for them to be processed. The worker task batches together many
//! items and processes them as one unit, before sending a result back to each calling task.
//!
//! See the README for an example.

#![deny(missing_docs)]

#[cfg(doctest)]
use doc_comment::doctest;
#[cfg(doctest)]
doctest!("../../README.md");

mod batch;
mod batch_inner;
mod batch_queue;
mod batcher;
pub mod error;
mod limits;
mod policies;
mod processor;
mod timeout;
mod worker;

pub use batcher::Batcher;
pub use error::BatchError;
pub use policies::{BatchingPolicy, OnFull};
pub use limits::Limits;
pub use processor::Processor;
pub use worker::WorkerHandle;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::join;
    use tracing::{Instrument, span};

    use crate::{Batcher, BatchingPolicy, Limits, Processor};

    #[derive(Debug, Clone)]
    pub struct SimpleBatchProcessor(pub Duration);

    impl Processor for SimpleBatchProcessor {
        type Key = String;
        type Input = String;
        type Output = String;
        type Error = String;
        type Resources = ();

        async fn acquire_resources(&self, _key: String) -> Result<(), String> {
            tokio::time::sleep(self.0).await;
            Ok(())
        }

        async fn process(
            &self,
            key: String,
            inputs: impl Iterator<Item = String> + Send,
            _resources: (),
        ) -> Result<Vec<String>, String> {
            tokio::time::sleep(self.0).await;
            Ok(inputs.map(|s| s + " processed for " + &key).collect())
        }
    }

    #[tokio::test]
    #[ignore = "flaky"]
    async fn test_tracing() {
        use tracing::Level;
        use tracing_capture::{CaptureLayer, SharedStorage};
        use tracing_subscriber::layer::SubscriberExt;

        let subscriber = tracing_subscriber::fmt()
            .pretty()
            .with_max_level(Level::INFO)
            .with_test_writer()
            .finish();
        // Add the capturing layer.
        let storage = SharedStorage::default();
        let subscriber = subscriber.with(CaptureLayer::new(&storage));

        // Capture tracing information.
        let _guard = tracing::subscriber::set_default(subscriber);

        let batcher = Batcher::builder()
            .name("test_tracing")
            .processor(SimpleBatchProcessor(Duration::from_millis(10)))
            .limits(Limits::builder().max_batch_size(2).build())
            .batching_policy(BatchingPolicy::Size)
            .build();

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

        let (o1, o2) = join!(h1, h2);

        assert!(o1.is_ok());
        assert!(o2.is_ok());

        let worker = batcher.worker_handle();
        worker.shut_down().await;
        tokio::time::timeout(Duration::from_secs(1), worker.wait_for_shutdown())
            .await
            .expect("Worker should shut down");

        let storage = storage.lock();

        let outer_process_spans: Vec<_> = storage
            .all_spans()
            .filter(|span| span.metadata().name().contains("process batch"))
            .collect();
        assert_eq!(
            outer_process_spans.len(),
            1,
            "should be a single outer span for processing the batch"
        );
        let process_span = outer_process_spans.first().unwrap().clone();

        assert_eq!(
            process_span["batch.size"], 2u64,
            "batch.size shouldn't be emitted as a string",
        );

        assert_eq!(
            process_span.follows_from().len(),
            2,
            "should follow from both handler spans"
        );

        let resource_spans: Vec<_> = storage
            .all_spans()
            .filter(|span| span.metadata().name().contains("acquire resources"))
            .collect();
        assert_eq!(
            resource_spans.len(),
            1,
            "should be a single span for acquiring resources"
        );
        let resource_span_parent = resource_spans
            .first()
            .unwrap()
            .parent()
            .expect("resource span should have a parent");
        assert_eq!(
            resource_span_parent, process_span,
            "resource acquisition should be a child of the outer process span"
        );

        let inner_process_spans: Vec<_> = storage
            .all_spans()
            .filter(|span| span.metadata().name().contains("process()"))
            .collect();
        assert_eq!(
            inner_process_spans.len(),
            1,
            "should be a single inner span for processing the batch"
        );
        let inner_span_parent = inner_process_spans
            .first()
            .unwrap()
            .parent()
            .expect("resource span should have a parent");
        assert_eq!(
            inner_span_parent, process_span,
            "resource acquisition should be a child of the outer process span"
        );

        let link_back_spans: Vec<_> = storage
            .all_spans()
            .filter(|span| span.metadata().name().contains("batch finished"))
            .collect();
        assert_eq!(
            link_back_spans.len(),
            2,
            "should be two spans for linking back to the process span"
        );

        for span in link_back_spans {
            assert_eq!(
                span.follows_from().len(),
                1,
                "link back spans should follow from the process span"
            );
        }

        assert_eq!(storage.all_spans().len(), 7, "should be 7 spans in total");
    }
}
