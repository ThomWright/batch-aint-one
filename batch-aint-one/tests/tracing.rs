//! Kept in its own test binary: tracing-core can cache a callsite as disabled when it's first hit
//! on another thread, which would silently drop spans this test asserts on. See
//! <https://github.com/tokio-rs/tracing/issues/2874>.

use std::time::Duration;

use batch_aint_one::{Batcher, BatchingPolicy, Limits, Processor};
use tokio::join;
use tracing::{Instrument, span};

#[derive(Debug, Clone)]
struct SimpleBatchProcessor(Duration);

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
    let process_span = *outer_process_spans.first().unwrap();

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
