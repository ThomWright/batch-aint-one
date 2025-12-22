use std::time::Duration;

use batch_aint_one::{Batcher, BatchingPolicy, Limits, OnFull, Processor};
use rand::Rng;
use rstest::rstest;
use tokio::time::sleep;

#[derive(Debug, Clone)]
struct VariableTimingProcessor {
    resource_delay_range_ms: (u64, u64),
    processing_delay_range_ms: (u64, u64),
}

impl VariableTimingProcessor {
    fn new(resource_delay_range_ms: (u64, u64), processing_delay_range_ms: (u64, u64)) -> Self {
        Self {
            resource_delay_range_ms,
            processing_delay_range_ms,
        }
    }

    fn random_duration(range: (u64, u64)) -> Duration {
        let mut rng = rand::rng();
        let millis = rng.random_range(range.0..=range.1);
        Duration::from_millis(millis)
    }
}

impl Processor for VariableTimingProcessor {
    type Key = String;
    type Input = i32;
    type Output = i32;
    type Error = String;
    type Resources = ();

    async fn acquire_resources(&self, _key: String) -> Result<(), String> {
        let delay = Self::random_duration(self.resource_delay_range_ms);
        sleep(delay).await;
        Ok(())
    }

    async fn process(
        &self,
        _key: String,
        inputs: impl Iterator<Item = i32> + Send,
        _resources: (),
    ) -> Result<Vec<i32>, String> {
        let items: Vec<_> = inputs.collect();

        let delay = Self::random_duration(self.processing_delay_range_ms);
        sleep(delay).await;

        Ok(items.into_iter().map(|x| x * 2).collect())
    }
}

#[tokio::test]
#[rstest]
#[timeout(Duration::from_secs(5))]
async fn variable_timing(
    #[values(
        BatchingPolicy::Immediate,
        BatchingPolicy::Duration(Duration::from_millis(100), OnFull::Process)
    )]
    policy: BatchingPolicy,
    #[values(5, 50)] batch_size: usize,
    #[values(1, 2)] key_concurrency: usize,
) {
    let resource_delay_ms = 50;
    let processing_delay_ms = 50;
    let arrival_delay_ms = 20;

    let processor = VariableTimingProcessor::new((0, resource_delay_ms), (0, processing_delay_ms));

    let batcher = Batcher::builder()
        .name("variable_timing_immediate")
        .processor(processor)
        .limits(
            Limits::builder()
                .max_batch_size(batch_size)
                .max_key_concurrency(key_concurrency)
                .max_batch_queue_size(10)
                .build(),
        )
        .batching_policy(policy)
        .build();

    let mut tasks = Vec::new();

    for i_key in 1..=3 {
        let batcher = batcher.clone();
        let mut total_delay = 0;

        for j_item in 1..=100 {
            let batcher = batcher.clone();

            let next_delay = {
                let mut rng = rand::rng();
                rng.random_range(0..=arrival_delay_ms)
            };
            let delay = Duration::from_millis(total_delay + next_delay);
            total_delay += next_delay;

            let task = tokio::spawn(async move {
                sleep(delay).await;

                let key = format!("key_{}", i_key);
                batcher.add(key, j_item).await.expect("task should succeed");
            });
            tasks.push(task);
        }
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await.expect("Task should complete");
    }

    // Shutdown
    let worker = batcher.worker_handle();
    worker.shut_down().await;
    worker.wait_for_shutdown().await;
}
