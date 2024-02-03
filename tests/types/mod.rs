use std::time::Duration;

use async_trait::async_trait;
use batch_aint_one::{Processor, Batcher};

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

struct NotCloneable {}
type Cloneable = Batcher<String, NotCloneable, NotCloneable>;

/// A [Batcher] should be cloneable, even when the `I`s and `O`s are not.
#[derive(Clone)]
#[allow(unused)]
struct CanDeriveClone {
    batcher: Cloneable,
}
