use std::time::Duration;

use batch_aint_one::{Batcher, Processor};

#[derive(Debug, Clone)]
pub struct SimpleBatchProcessor(pub Duration);

impl Processor for SimpleBatchProcessor {
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
        key: String,
        inputs: impl Iterator<Item = String> + Send,
        _resources: (),
    ) -> Result<Vec<String>, String> {
        tokio::time::sleep(self.0).await;
        Ok(inputs.map(|s| s + " processed for " + &key).collect())
    }
}

#[derive(Clone)]
struct ProcessorWithNonCloneableIO;
impl Processor for ProcessorWithNonCloneableIO {
    type Key = String;
    type Input = NotCloneable;
    type Output = NotCloneable;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: String) -> Result<(), String> {
        Ok(())
    }

    async fn process(
        &self,
        _key: String,
        inputs: impl Iterator<Item = NotCloneable> + Send,
        _resources: (),
    ) -> Result<Vec<NotCloneable>, String> {
        Ok(inputs.collect())
    }
}

struct NotCloneable {}
type Cloneable = Batcher<ProcessorWithNonCloneableIO>;

/// A [Batcher] should be cloneable, even when the `I`s and `O`s are not.
#[derive(Clone)]
#[allow(unused)]
struct CanDeriveClone {
    batcher: Cloneable,
}


/// The BatchError type should implement Error when E does.
#[test]
fn assert_batch_error_impls_error() {
    use batch_aint_one::error::BatchError;
    use std::error::Error;
    let e = BatchError::<std::io::Error>::Cancelled;
    let _ = e.source();
}
