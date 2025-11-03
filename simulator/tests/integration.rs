use batch_aint_one::{Batcher, BatchingPolicy, Limits, OnFull};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use simulator::arrival::PoissonArrivals;
use simulator::latency::LatencyProfile;
use simulator::metrics::MetricsCollector;
use simulator::processor::{SimProcessor, SimulatedInput};
use simulator::visualise::Visualiser;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::time::Duration;

static TEST_SEED: LazyLock<u64> = LazyLock::new(|| {
    let seed = StdRng::from_os_rng().next_u64();
    println!("Using test seed: {}", seed);
    seed
});

#[tokio::test(start_paused = true)]
async fn test_minimal_simulation() {
    let seed = *TEST_SEED;

    let metrics = Arc::new(Mutex::new(MetricsCollector::new()));

    // Create a processor with ~10ms processing latency
    let processor = SimProcessor::builder()
        .processing_latency(LatencyProfile::new(2, 10.0, Some(seed)))
        .metrics(metrics.clone())
        .build();

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

#[tokio::test]
async fn test_distributed_arrivals_and_latency() {
    tokio::time::pause();
    let seed = *TEST_SEED;

    let start = std::time::Instant::now();

    // Arrival rate: 20 items/sec (mean inter-arrival: 50ms)
    let mut arrivals = PoissonArrivals::new(20.0, Some(seed));

    let metrics = Arc::new(Mutex::new(MetricsCollector::new()));

    // Processing latency: Erlang(k=3, rate=100) => mean ~30ms
    let latency_profile = LatencyProfile::new(3, 100.0, Some(seed));
    let processor = SimProcessor::builder()
        .processing_latency(latency_profile)
        .metrics(metrics.clone())
        .build();

    // Configure batcher with Duration policy (50ms timeout)
    let batcher = Batcher::builder()
        .name("distributed-test")
        .processor(processor)
        .limits(Limits::builder().max_batch_size(10).build())
        .batching_policy(BatchingPolicy::Duration(
            Duration::from_millis(50),
            OnFull::Process,
        ))
        .build();

    let key = "test-key".to_string();
    let num_items = 20;

    // Spawn arrival generator
    let batcher_clone = batcher.clone();
    let key_clone = key.clone();
    let arrival_task = tokio::spawn(async move {
        let mut results = Vec::new();

        for item_id in 0..num_items {
            let batcher = batcher_clone.clone();
            let key = key_clone.clone();

            // Submit item
            let task = tokio::spawn(async move {
                let input = SimulatedInput {
                    item_id,
                    submitted_at: tokio::time::Instant::now(),
                };
                batcher.add(key, input).await
            });

            results.push(task);

            // Wait for next arrival (don't wait after last item)
            if item_id < num_items - 1 {
                let inter_arrival_time = arrivals.next_inter_arrival_duration();
                tokio::time::sleep(inter_arrival_time).await;
            }
        }

        results
    });

    // Wait for all arrivals to be submitted and collect results
    let tasks = arrival_task.await.unwrap();
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

    // Print batch statistics
    let batch_sizes: Vec<_> = batches.values().map(|b| b.len()).collect();
    let total_batches = batches.len();
    let min_batch = *batch_sizes.iter().min().unwrap();
    let max_batch = *batch_sizes.iter().max().unwrap();
    let mean_batch = batch_sizes.iter().sum::<usize>() as f64 / total_batches as f64;

    println!("Batches: {}", total_batches);
    println!(
        "Batch sizes - min: {}, max: {}, mean: {:.1}",
        min_batch, max_batch, mean_batch
    );

    // Validate reasonable behavior
    assert!(
        total_batches >= 2,
        "Should have multiple batches with Duration policy"
    );
    assert!(
        min_batch >= 1 && max_batch <= 10,
        "Batch sizes should be reasonable"
    );

    // Verify that time was paused - wall-clock time should be much faster than simulated time
    let wall_clock_elapsed = start.elapsed();
    println!("Wall-clock elapsed: {:?}", wall_clock_elapsed);

    // With 20 items arriving at ~50ms intervals, simulated time would take ~1000ms
    // With paused time, wall-clock should complete much faster
    assert!(
        wall_clock_elapsed < Duration::from_millis(100),
        "Test should complete quickly with paused time, took {:?}",
        wall_clock_elapsed
    );
}

#[tokio::test(start_paused = true)]
async fn test_longer_simulation() {
    let seed = *TEST_SEED;

    let start = std::time::Instant::now();
    let simulated_time = Arc::new(Mutex::new(Duration::ZERO));

    // Arrival rate: 500 items/sec (mean inter-arrival: 2ms)
    let arrival_rate = 500.0;
    let mut arrivals = PoissonArrivals::new(arrival_rate, Some(seed));

    let metrics = Arc::new(Mutex::new(MetricsCollector::new()));

    // Processing latency: Erlang(k=2, rate=200) => mean ~10ms
    let latency_profile = LatencyProfile::new(2, 200.0, Some(seed));
    let processor = SimProcessor::builder()
        .processing_latency(latency_profile)
        .metrics(metrics.clone())
        .build();

    // Configure batcher with Balanced policy
    let batcher = Batcher::builder()
        .name("longer-test")
        .processor(processor)
        .limits(
            Limits::builder()
                .max_batch_size(50)
                .max_key_concurrency(10)
                .build(),
        )
        .batching_policy(BatchingPolicy::Balanced { min_size_hint: 20 })
        .build();

    let key = "test-key".to_string();
    let num_items = 10_000;

    // Spawn arrival generator
    let batcher_clone = batcher.clone();
    let key_clone = key.clone();
    let simulated_time_clone = simulated_time.clone();
    let arrival_task = tokio::spawn(async move {
        let mut results = Vec::new();
        let sim_start = tokio::time::Instant::now();
        let mut precise_time_offset = Duration::ZERO;

        for item_id in 0..num_items {
            let next_inter_arrival = arrivals.next_inter_arrival_duration();
            precise_time_offset += next_inter_arrival;

            // Calculate what millisecond this item should arrive in
            let target_ms_offset = precise_time_offset.as_millis();
            let current_ms_offset = (tokio::time::Instant::now() - sim_start).as_millis();

            // If we need to advance time, sleep until target millisecond
            if target_ms_offset > current_ms_offset {
                let sleep_duration =
                    Duration::from_millis((target_ms_offset - current_ms_offset) as u64);
                tokio::time::sleep(sleep_duration).await;
            }

            // Now submit with precise timestamp
            let submitted_at = sim_start + precise_time_offset;
            let batcher = batcher_clone.clone();
            let key = key_clone.clone();

            let task = tokio::spawn(async move {
                let input = SimulatedInput {
                    item_id,
                    submitted_at,
                };
                batcher.add(key, input).await
            });

            results.push(task);
        }

        *simulated_time_clone.lock().unwrap() = precise_time_offset;
        results
    });

    // Wait for all arrivals and collect results
    let tasks = arrival_task.await.unwrap();
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

    let wall_clock_elapsed = start.elapsed();

    // Get batch efficiency metrics
    let efficiency = metrics.lock().unwrap().batch_efficiency();

    println!("\n=== Longer Simulation Results ===");
    println!("Items: {}", num_items);
    println!("Wall-clock time: {:?}", wall_clock_elapsed);
    println!("Simulated time: {:?}", simulated_time.lock().unwrap());
    println!("\nBatch Efficiency:");
    println!("  Total batches: {}", efficiency.total_batches);
    println!("  Mean batch size: {:.2}", efficiency.mean_batch_size);
    println!("  Median batch size: {:.2}", efficiency.median_batch_size);
    println!(
        "  Smallest/Largest: {}/{}",
        efficiency.smallest_batch_size, efficiency.largest_batch_size
    );
    println!("  At largest size: {:.1}%", efficiency.percentage_at_largest());

    // Basic sanity checks
    assert!(
        efficiency.total_batches > 0,
        "Should have processed batches"
    );
    assert!(
        efficiency.mean_batch_size > 1.0,
        "Should achieve some batching"
    );

    let metrics_guard = metrics.lock().unwrap();

    let rps_data = metrics_guard.rps_over_time(None);
    // average
    let average_rps = rps_data.iter().map(|&(_, rps)| rps).sum::<f64>() / rps_data.len() as f64;
    assert!(
        (average_rps - arrival_rate).abs() / arrival_rate < 0.1,
        "Average RPS {:.2} should be within 10% of expected arrival rate {:.2}",
        average_rps,
        arrival_rate
    );

    // Generate visualizations
    // Paths are relative to the simulator crate directory when running tests
    let visualiser = Visualiser::new(
        &metrics_guard,
        "tests/output/test_longer_simulation",
        "tests/templates",
    );

    visualiser
        .generate_all()
        .expect("should generate all visualizations");
}
