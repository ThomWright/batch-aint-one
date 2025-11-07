//! Visualisation of metrics using gnuplot

use crate::metrics::{BatchEfficiency, MetricsCollector};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::NamedTempFile;
use tokio::time::Duration;

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
    pub fn rps_chart(&self, bucket_size: Option<Duration>) -> std::io::Result<()> {
        let output_path = self.output_dir.join("arrival_rate.png");
        let template_path = self.templates_dir.join("arrivals_over_time.gnuplot");

        generate_rps_chart(self.collector, &output_path, &template_path, bucket_size)
    }

    /// Generate latency distribution histogram
    pub fn latency_histogram(&self) -> std::io::Result<()> {
        let output_path = self.output_dir.join("latency_histogram.png");
        let template_path = self.templates_dir.join("latency_histogram.gnuplot");

        generate_latency_histogram(self.collector, &output_path, &template_path)
    }

    /// Generate resource usage over time chart
    pub fn resource_usage_chart(&self) -> std::io::Result<()> {
        let output_path = self.output_dir.join("resource_usage.png");
        let template_path = self.templates_dir.join("resource_usage.gnuplot");

        generate_resource_usage_chart(self.collector, &output_path, &template_path)
    }

    /// Generate latency over time chart
    pub fn latency_over_time_chart(&self, bucket_size: Option<Duration>) -> std::io::Result<()> {
        let output_path = self.output_dir.join("latency_over_time.png");
        let template_path = self.templates_dir.join("latency_over_time.gnuplot");

        generate_latency_over_time_chart(self.collector, &output_path, &template_path, bucket_size)
    }

    /// Generate all standard visualizations
    pub fn generate_all(&self) -> std::io::Result<()> {
        self.batch_size_histogram()?;
        self.rps_chart(None)?;
        self.latency_histogram()?;
        self.latency_over_time_chart(None)?;
        self.resource_usage_chart()?;
        Ok(())
    }
}

// Internal helper functions

/// Common gnuplot chart generation logic
///
/// Handles the complete chart generation pipeline:
/// 1. Creates output directory if needed
/// 2. Writes data to a .dat file using provided writer
/// 3. Reads and processes gnuplot template
/// 4. Executes gnuplot with the processed template
/// 5. Returns path to generated chart
fn generate_gnuplot_chart<F>(
    chart_name: &str,
    output_path: &Path,
    template_path: &Path,
    data_writer: F,
) -> io::Result<PathBuf>
where
    F: FnOnce(&mut dyn Write) -> io::Result<()>,
{
    // Ensure parent directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create data file
    let data_path = output_path.with_extension("dat");
    let mut data_file = std::fs::File::create(&data_path)?;
    data_writer(&mut data_file)?;

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
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "gnuplot failed for {}: {}",
                chart_name,
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }

    Ok(output_path.to_path_buf())
}

/// Generate a batch size distribution histogram using gnuplot
fn generate_batch_size_histogram(
    efficiency: &BatchEfficiency,
    output_path: &Path,
    template_path: &Path,
) -> io::Result<()> {
    let data = efficiency.size_distribution_sorted();

    generate_gnuplot_chart("batch size histogram", output_path, template_path, |file| {
        writeln!(file, "# batch_size count")?;
        for (size, count) in data {
            writeln!(file, "{} {}", size, count)?;
        }
        Ok(())
    })?;

    Ok(())
}

/// Generate RPS of arrivals chart using gnuplot
fn generate_rps_chart(
    collector: &MetricsCollector,
    output_path: &Path,
    template_path: &Path,
    bucket_size: Option<Duration>,
) -> io::Result<()> {
    let data = collector.rps_over_time(bucket_size);

    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "No arrival data available",
        ));
    }

    generate_gnuplot_chart("RPS chart", output_path, template_path, |file| {
        writeln!(file, "# time_seconds rps")?;
        for point in data {
            writeln!(file, "{} {}", point.time.as_secs_f64(), point.rps)?;
        }
        Ok(())
    })?;

    Ok(())
}

/// Generate latency distribution histogram using gnuplot
fn generate_latency_histogram(
    collector: &MetricsCollector,
    output_path: &Path,
    template_path: &Path,
) -> io::Result<()> {
    let latencies = collector.latency_samples();

    if latencies.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "No latency data available",
        ));
    }

    generate_gnuplot_chart("latency histogram", output_path, template_path, |file| {
        writeln!(file, "# latency_ms")?;
        for latency in latencies {
            writeln!(file, "{}", latency.as_secs_f64() * 1000.0)?;
        }
        Ok(())
    })?;

    Ok(())
}

/// Generate resource usage over time chart using gnuplot
fn generate_resource_usage_chart(
    collector: &MetricsCollector,
    output_path: &Path,
    template_path: &Path,
) -> io::Result<()> {
    let data = collector.resource_usage_over_time();

    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "No resource usage data available",
        ));
    }

    generate_gnuplot_chart("resource usage chart", output_path, template_path, |file| {
        writeln!(file, "# time_seconds in_use available")?;
        for point in data {
            writeln!(file, "{} {} {}", point.time.as_secs_f64(), point.in_use, point.available)?;
        }
        Ok(())
    })?;

    Ok(())
}

/// Generate latency over time chart using gnuplot
fn generate_latency_over_time_chart(
    collector: &MetricsCollector,
    output_path: &Path,
    template_path: &Path,
    bucket_size: Option<Duration>,
) -> io::Result<()> {
    let data = collector.latency_over_time(bucket_size);

    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "No latency data available",
        ));
    }

    generate_gnuplot_chart("latency over time chart", output_path, template_path, |file| {
        writeln!(file, "# time_seconds mean_ms p50_ms p99_ms")?;
        for stats in data {
            let time_secs = stats.time.as_secs_f64();
            let mean_ms = stats.mean.as_secs_f64() * 1000.0;
            let p50_ms = stats.p50.as_secs_f64() * 1000.0;
            let p99_ms = stats.p99.as_secs_f64() * 1000.0;
            writeln!(file, "{} {} {} {}", time_secs, mean_ms, p50_ms, p99_ms)?;
        }
        Ok(())
    })?;

    Ok(())
}
