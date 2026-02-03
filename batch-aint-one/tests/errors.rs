use assert_matches::assert_matches;
use batch_aint_one::error::{BatchError, ProcessorInvariantViolation};
use batch_aint_one::{Batcher, BatchingPolicy, Limits, Processor};
use tokio::join;

#[test]
fn inner_error_reported_as_source() {
    use std::error::Error;
    let e = BatchError::<std::io::Error>::BatchFailed(std::io::Error::new(
        std::io::ErrorKind::Other,
        "underlying error",
    ));
    let display = format!("{}", e);
    assert_eq!(display, "The entire batch failed");
    let source = e.source().unwrap();
    assert_eq!(source.to_string(), "underlying error");
}

/// A processor that returns fewer outputs than inputs.
#[derive(Debug, Clone)]
struct WrongOutputCountProcessor;

impl Processor for WrongOutputCountProcessor {
    type Key = String;
    type Input = String;
    type Output = String;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: String) -> Result<(), String> {
        Ok(())
    }

    async fn process(
        &self,
        _key: String,
        inputs: impl Iterator<Item = String> + Send,
        _resources: (),
    ) -> Result<Vec<String>, String> {
        // Return one fewer output than inputs
        let outputs: Vec<_> = inputs.collect();
        Ok(outputs.into_iter().skip(1).collect())
    }
}

#[tokio::test]
async fn returns_error_when_processor_returns_wrong_number_of_outputs() {
    let batcher = Batcher::builder()
        .name("test_wrong_output_count")
        .processor(WrongOutputCountProcessor)
        .limits(Limits::builder().max_batch_size(2).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let (r1, r2) = join!(
        batcher.add("key".to_string(), "input1".to_string()),
        batcher.add("key".to_string(), "input2".to_string()),
    );

    // Both items should receive the same error
    assert_matches!(
        r1,
        Err(BatchError::ProcessorInvariantViolation(
            ProcessorInvariantViolation::WrongNumberOfOutputs {
                expected: 2,
                actual: 1
            }
        ))
    );
    assert_matches!(
        r2,
        Err(BatchError::ProcessorInvariantViolation(
            ProcessorInvariantViolation::WrongNumberOfOutputs {
                expected: 2,
                actual: 1
            }
        ))
    );
}
