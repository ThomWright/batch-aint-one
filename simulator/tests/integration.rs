use batch_aint_one::{Batcher, BatchingPolicy, Limits};
use simulator::processor::{SimProcessor, SimulatedInput};
use std::collections::HashMap;
use tokio::time::Duration;

#[tokio::test]
async fn test_minimal_simulation() {
    // Create a processor with 10ms processing latency
    let processor = SimProcessor::new(Duration::from_millis(10));

    // Configure batcher with Size policy (batch_size = 5)
    let batcher = Batcher::builder()
        .name("test-batcher")
        .processor(processor)
        .limits(Limits::builder().max_batch_size(5).build())
        .batching_policy(BatchingPolicy::Size)
        .build();

    let key = "test-key".to_string();
    let num_items = 10;

    // Submit 10 items immediately
    let mut tasks = Vec::new();

    for item_id in 0..num_items {
        let batcher = batcher.clone();
        let key = key.clone();

        let task = tokio::spawn(async move {
            let input = SimulatedInput {
                item_id,
                submitted_at: tokio::time::Instant::now(),
            };

            batcher.add(key, input).await
        });

        tasks.push(task);
    }

    // Collect all results
    let mut outputs = Vec::new();
    for task in tasks {
        let result = task.await.unwrap();
        match result {
            Ok(output) => outputs.push(output),
            Err(e) => panic!("Item processing failed: {:?}", e),
        }
    }

    // Analyze results
    assert_eq!(outputs.len(), num_items, "All items should be processed");

    // Group by batch_id
    let mut batches: HashMap<usize, Vec<_>> = HashMap::new();
    for output in &outputs {
        batches.entry(output.batch_id).or_default().push(output);
    }

    let batch_sizes: Vec<_> = batches.values().map(|b| b.len()).collect();

    // Validate
    assert_eq!(
        batches.len(),
        2,
        "Should have 2 batches (10 items / 5 batch size)"
    );

    for batch_size in batch_sizes {
        assert_eq!(batch_size, 5, "Each batch should have 5 items");
    }
}
