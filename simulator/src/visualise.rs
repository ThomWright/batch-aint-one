//! Visualisation of metrics using gnuplot

use crate::metrics::{BatchEfficiency, MetricsCollector};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::NamedTempFile;

/// Visualiser for generating charts from collected metrics
pub struct Visualiser<'a> {
    collector: &'a MetricsCollector,
    output_dir: PathBuf,
    templates_dir: PathBuf,
}

impl<'a> Visualiser<'a> {
    /// Create a new visualiser
    ///
    /// # Arguments
    /// * `collector` - Metrics collector with simulation data
    /// * `output_dir` - Directory where visualizations will be written
    /// * `templates_dir` - Directory containing gnuplot template files
    pub fn new(
        collector: &'a MetricsCollector,
        output_dir: impl Into<PathBuf>,
        templates_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            collector,
            output_dir: output_dir.into(),
            templates_dir: templates_dir.into(),
        }
    }

    /// Generate batch size distribution histogram
    pub fn batch_size_histogram(&self) -> std::io::Result<()> {
        let efficiency = self.collector.batch_efficiency();
        let output_path = self.output_dir.join("batch_size_histogram.png");
        let template_path = self.templates_dir.join("batch_size_histogram.gnuplot");

        generate_batch_size_histogram(&efficiency, &output_path, &template_path)
    }

    /// Generate arrival rate over time (RPS) chart
    pub fn rps_chart(&self, bucket_size_secs: Option<f64>) -> std::io::Result<()> {
        let output_path = self.output_dir.join("arrival_rate.png");
        let template_path = self.templates_dir.join("arrivals_over_time.gnuplot");

        generate_rps_chart(self.collector, &output_path, &template_path, bucket_size_secs)
    }

    /// Generate latency distribution histogram
    pub fn latency_histogram(&self) -> std::io::Result<()> {
        let output_path = self.output_dir.join("latency_histogram.png");
        let template_path = self.templates_dir.join("latency_histogram.gnuplot");

        generate_latency_histogram(self.collector, &output_path, &template_path)
    }

    /// Generate all standard visualizations
    pub fn generate_all(&self) -> std::io::Result<()> {
        self.batch_size_histogram()?;
        self.rps_chart(None)?;
        self.latency_histogram()?;
        Ok(())
    }
}

// Internal helper functions

/// Generate a batch size distribution histogram using gnuplot
pub fn generate_batch_size_histogram(
    efficiency: &BatchEfficiency,
    output_path: &Path,
    template_path: &Path,
) -> std::io::Result<()> {
    let data = efficiency.size_distribution_sorted();

    // Ensure parent directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create data file
    let data_path = output_path.with_extension("dat");
    let mut data_file = std::fs::File::create(&data_path)?;
    writeln!(data_file, "# batch_size count")?;
    for (size, count) in data {
        writeln!(data_file, "{} {}", size, count)?;
    }

    // Read template and substitute placeholders
    let template = std::fs::read_to_string(template_path)?;
    let script_content = template
        .replace("{{OUTPUT_PATH}}", &output_path.display().to_string())
        .replace("{{DATA_PATH}}", &data_path.display().to_string());

    // Write gnuplot script to a unique temp file
    let mut temp_script = NamedTempFile::new()?;
    temp_script.write_all(script_content.as_bytes())?;
    temp_script.flush()?;

    // Run gnuplot (temp file will be automatically deleted when dropped)
    let output = Command::new("gnuplot")
        .arg(temp_script.path())
        .output()?;

    if !output.status.success() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "gnuplot failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }

    println!("Generated histogram: {}", output_path.display());

    Ok(())
}

/// Generate RPS of arrivals chart using gnuplot
pub fn generate_rps_chart(
    collector: &MetricsCollector,
    output_path: &Path,
    template_path: &Path,
    bucket_size_secs: Option<f64>,
) -> std::io::Result<()> {
    let data = collector.rps_over_time(bucket_size_secs);

    if data.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "No arrival data available",
        ));
    }

    // Ensure parent directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create data file
    let data_path = output_path.with_extension("dat");
    let mut data_file = std::fs::File::create(&data_path)?;
    writeln!(data_file, "# time_seconds rps")?;
    for (time, rps) in data {
        writeln!(data_file, "{} {}", time, rps)?;
    }

    // Read template and substitute placeholders
    let template = std::fs::read_to_string(template_path)?;
    let script_content = template
        .replace("{{OUTPUT_PATH}}", &output_path.display().to_string())
        .replace("{{DATA_PATH}}", &data_path.display().to_string());

    // Write gnuplot script to a unique temp file
    let mut temp_script = NamedTempFile::new()?;
    temp_script.write_all(script_content.as_bytes())?;
    temp_script.flush()?;

    // Run gnuplot
    let output = Command::new("gnuplot")
        .arg(temp_script.path())
        .output()?;

    if !output.status.success() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "gnuplot failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }

    println!("Generated RPS chart: {}", output_path.display());

    Ok(())
}

/// Generate latency distribution histogram using gnuplot
pub fn generate_latency_histogram(
    collector: &MetricsCollector,
    output_path: &Path,
    template_path: &Path,
) -> std::io::Result<()> {
    let latencies = collector.latency_samples();

    if latencies.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "No latency data available",
        ));
    }

    // Ensure parent directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create data file
    let data_path = output_path.with_extension("dat");
    let mut data_file = std::fs::File::create(&data_path)?;
    writeln!(data_file, "# latency_ms")?;
    for latency in latencies {
        writeln!(data_file, "{}", latency)?;
    }

    // Read template and substitute placeholders
    let template = std::fs::read_to_string(template_path)?;
    let script_content = template
        .replace("{{OUTPUT_PATH}}", &output_path.display().to_string())
        .replace("{{DATA_PATH}}", &data_path.display().to_string());

    // Write gnuplot script to a unique temp file
    let mut temp_script = NamedTempFile::new()?;
    temp_script.write_all(script_content.as_bytes())?;
    temp_script.flush()?;

    // Run gnuplot
    let output = Command::new("gnuplot")
        .arg(temp_script.path())
        .output()?;

    if !output.status.success() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "gnuplot failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }

    println!("Generated latency histogram: {}", output_path.display());

    Ok(())
}
