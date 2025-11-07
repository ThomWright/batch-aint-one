//! Reporting and visualization of simulation results

use crate::metrics::MetricsCollector;
use crate::scenario::ScenarioConfig;
use crate::visualise::Visualiser;
use std::path::PathBuf;

/// Configuration for the simulation reporter
pub struct ReporterConfig {
    /// Directory where visualizations will be written
    pub output_dir: PathBuf,
    /// Directory containing gnuplot template files
    pub templates_dir: PathBuf,
}

/// Reporter for printing summary statistics and generating visualizations
pub struct SimulationReporter<'a> {
    metrics: &'a MetricsCollector,
    scenario_config: Option<&'a ScenarioConfig>,
    config: ReporterConfig,
}

impl<'a> SimulationReporter<'a> {
    /// Create a new simulation reporter
    pub fn new(
        metrics: &'a MetricsCollector,
        scenario_config: &'a ScenarioConfig,
        config: ReporterConfig,
    ) -> Self {
        Self {
            metrics,
            scenario_config: Some(scenario_config),
            config,
        }
    }

    /// Print summary statistics to stdout
    pub fn print_summary(&self) {
        // Print scenario configuration if available
        if let Some(scenario) = self.scenario_config {
            println!("\n=== Scenario Configuration ===");
            println!("Name:                     {}", scenario.name);
            println!("Policy:                   {}", scenario.batching_policy);
            println!("Limits:                   {}", scenario.limits);
            println!(
                "Mean Processing Latency:  {:.2} ms",
                scenario.processing_latency.mean().as_secs_f64() * 1000.0
            );
            println!(
                "Arrival Rate:             {:.2} items/sec",
                scenario.arrival_rate
            );
            println!("Key Distribution:         {:?}", scenario.key_distribution);
        }

        let efficiency = self.metrics.batch_efficiency();
        let mean_latency = self.metrics.mean_latency_ms();
        let mean_rps = self.metrics.mean_rps();

        println!("\n=== Simulation Results ===");
        println!("Items processed:    {}", self.metrics.items().len());

        if let Some(duration) = self.metrics.simulated_duration() {
            println!("Simulated duration: {:.2}s", duration.as_secs_f64());
        }

        println!("Mean RPS:           {:.2}", mean_rps);
        println!("Mean Latency:       {:.2} ms", mean_latency);

        println!("\nBatch Efficiency:");
        println!("  Total batches: {}", efficiency.total_batches);
        println!("  Mean batch size: {:.2}", efficiency.mean_batch_size);
        println!("  Median batch size: {:.2}", efficiency.median_batch_size);
        println!(
            "  Batch size range: {} to {} (smallest: {:.1}%, largest: {:.1}%)",
            efficiency.smallest_batch_size,
            efficiency.largest_batch_size,
            efficiency.percentage_at_smallest(),
            efficiency.percentage_at_largest()
        );
    }

    /// Generate all visualizations
    pub fn generate_visualizations(&self) -> std::io::Result<()> {
        let visualiser = Visualiser::new(
            self.metrics,
            &self.config.output_dir,
            &self.config.templates_dir,
        );

        visualiser.generate_all()?;

        // Print generated chart paths
        println!("\n=== Charts ===");
        println!("Batch size histogram: {}", self.config.output_dir.join("batch_size_histogram.png").display());
        println!("RPS chart:            {}", self.config.output_dir.join("arrival_rate.png").display());
        println!("Latency histogram:    {}", self.config.output_dir.join("latency_histogram.png").display());
        println!("Latency over time:    {}", self.config.output_dir.join("latency_over_time.png").display());
        println!("Resource usage:       {}", self.config.output_dir.join("resource_usage.png").display());

        Ok(())
    }

    /// Print summary and generate visualizations
    pub fn report(&self) -> std::io::Result<()> {
        self.print_summary();
        self.generate_visualizations()
    }
}
