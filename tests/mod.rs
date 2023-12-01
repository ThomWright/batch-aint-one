use std::{marker::Send, time::Duration};

use async_trait::async_trait;
use batch_aint_one::{
    limit::{DurationLimit, LimitResult, LimitStrategy, SizeLimit},
    Batch, BatcherBuilder, Processor,
};
use tokio::time::Instant;
use tokio_test::assert_elapsed;

#[derive(Debug, Clone)]
struct SimpleBatchProcessor;

#[async_trait]
impl Processor<String, String> for SimpleBatchProcessor {
    async fn process(&self, inputs: impl Iterator<Item = String> + Send) -> Vec<String> {
        inputs.map(|s| s + " processed").collect()
    }
}

#[tokio::test]
async fn limit_size() {
    let batcher = BatcherBuilder::new(SimpleBatchProcessor)
        .with_limit(SizeLimit::new(3))
        .build();

    let fo1 = batcher.add("A".to_string(), "1".to_string()).await.unwrap();
    let fo2 = batcher.add("A".to_string(), "2".to_string()).await.unwrap();
    let fo3 = batcher.add("A".to_string(), "3".to_string()).await.unwrap();

    assert_eq!("1 processed".to_string(), fo1.await.unwrap());
    assert_eq!("2 processed".to_string(), fo2.await.unwrap());
    assert_eq!("3 processed".to_string(), fo3.await.unwrap());
}

#[tokio::test]
async fn limit_duration() {
    tokio::time::pause();
    let now = Instant::now();
    let fifty_ms = Duration::from_millis(50);

    let batcher = BatcherBuilder::new(SimpleBatchProcessor)
        .with_limit(DurationLimit::new(fifty_ms))
        .build();

    let added = batcher.add("A".to_string(), "1".to_string()).await.unwrap();

    let output_fut = tokio_test::task::spawn(added);

    tokio::time::advance(Duration::ZERO).await; // Allows the worker to receive the message
    tokio::time::advance(fifty_ms).await;

    let output = output_fut.await;
    assert_elapsed!(now, fifty_ms);

    assert_eq!("1 processed".to_string(), output.unwrap());
}

#[tokio::test]
async fn limit_custom() {
    #[derive(Debug)]
    struct CustomLimit;
    impl<I, O> LimitStrategy<String, I, O> for CustomLimit {
        fn limit(&self, batch: &Batch<String, I, O>) -> LimitResult {
            match batch.key_ref().as_str() {
                "slow" => LimitResult::ProcessAfter(Duration::from_secs(10)),
                _ => LimitResult::ProcessAfter(Duration::from_millis(50)),
            }
        }
    }

    tokio::time::pause();
    let now = Instant::now();
    let fifty_ms = Duration::from_millis(50);

    let batcher = BatcherBuilder::new(SimpleBatchProcessor)
        .with_limit(CustomLimit)
        .build();

    let added = batcher.add("A".to_string(), "1".to_string()).await.unwrap();

    let output_fut = tokio_test::task::spawn(added);

    tokio::time::advance(Duration::ZERO).await; // Allows the worker to receive the message
    tokio::time::advance(fifty_ms).await;

    let output = output_fut.await;
    assert_elapsed!(now, fifty_ms);

    assert_eq!("1 processed".to_string(), output.unwrap());
}
