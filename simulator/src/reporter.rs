//! Reporting and visualization of simulation results

use crate::metrics::MetricsCollector;
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
    config: ReporterConfig,
}

impl<'a> SimulationReporter<'a> {
    /// Create a new simulation reporter
    pub fn new(metrics: &'a MetricsCollector, config: ReporterConfig) -> Self {
        Self { metrics, config }
    }

    /// Print summary statistics to stdout
    pub fn print_summary(&self) {
        let efficiency = self.metrics.batch_efficiency();
        let mean_latency = self.metrics.mean_latency_ms();
        let mean_rps = self.metrics.mean_rps();

        println!("\n=== Simulation Results ===");
        println!("Items processed: {}", self.metrics.items().len());
        println!("Mean RPS: {:.2}", mean_rps);
        println!("Mean Latency: {:.2} ms", mean_latency);

        println!("\nBatch Efficiency:");
        println!("  Total batches: {}", efficiency.total_batches);
        println!("  Mean batch size: {:.2}", efficiency.mean_batch_size);
        println!("  Median batch size: {:.2}", efficiency.median_batch_size);
        println!(
            "  Smallest/Largest: {}/{}",
            efficiency.smallest_batch_size, efficiency.largest_batch_size
        );
        println!(
            "  At largest size: {:.1}%",
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

        visualiser.generate_all()
    }

    /// Print summary and generate visualizations
    pub fn report(&self) -> std::io::Result<()> {
        self.print_summary();
        self.generate_visualizations()
    }
}
