use assert_matches::assert_matches;
use batch_aint_one::{BatchError, Batcher, BatchingPolicy, Limits, Processor};
use futures::future::join_all;

#[derive(Debug, Clone)]
pub struct PanickingProcessor {
    panic_on_acquire: bool,
    panic_on_process: bool,
}

impl PanickingProcessor {
    fn new() -> Self {
        Self {
            panic_on_acquire: false,
            panic_on_process: false,
        }
    }

    fn with_acquire_panic(mut self) -> Self {
        self.panic_on_acquire = true;
        self
    }

    fn with_process_panic(mut self) -> Self {
        self.panic_on_process = true;
        self
    }
}

impl Processor for PanickingProcessor {
    type Key = String;
    type Input = String;
    type Output = String;
    type Error = String;
    type Resources = String;

    async fn acquire_resources(&self, key: String) -> Result<String, String> {
        if self.panic_on_acquire {
            panic!("Resource acquisition panic for key: {}", key);
        }
        Ok(format!("resources-{}", key))
    }

    async fn process(
        &self,
        key: String,
        inputs: impl Iterator<Item = String> + Send,
        resources: String,
    ) -> Result<Vec<String>, String> {
        if self.panic_on_process {
            panic!("Processing panic for key: {}", key);
        }
        Ok(inputs
            .map(|input| format!("{}-{}-{}", key, input, resources))
            .collect())
    }
}

/// Given a processor that panics during resource acquisition
/// When we submit items for processing
/// Then all items should fail with a Panic error
#[tokio::test]
async fn immediate_resource_acquisition_panic_handling() {
    tokio::time::pause();

    let processor = PanickingProcessor::new().with_acquire_panic();

    let batcher = Batcher::builder()
        .name("panic_test")
        .processor(processor)
        .limits(
            Limits::default()
                .with_max_batch_size(10)
                .with_max_key_concurrency(1),
        )
        .batching_policy(BatchingPolicy::Immediate)
        .build();

    let f = batcher.add("test_key".to_string(), "item1".to_string());

    let result = f.await;
    assert_matches!(result, Err(BatchError::Panic));
}

/// Given a processor that panics during batch processing
/// When we submit items for processing
/// Then all items should fail with a Panic error
#[tokio::test]
async fn process_panic_handling() {
    tokio::time::pause();

    let processor = PanickingProcessor::new().with_process_panic();

    let batcher = Batcher::builder()
        .name("panic_test")
        .processor(processor)
        .limits(
            Limits::default()
                .with_max_batch_size(2)
                .with_max_key_concurrency(1),
        )
        .batching_policy(BatchingPolicy::Size)
        .build();

    let f1 = batcher.add("test_key".to_string(), "item1".to_string());
    let f2 = batcher.add("test_key".to_string(), "item2".to_string());

    let results = join_all([f1, f2]).await;

    for result in results {
        assert_matches!(result, Err(BatchError::Panic));
    }
}
