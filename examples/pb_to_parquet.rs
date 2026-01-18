//! Example: Convert OTLP protobuf files to Parquet
//!
//! This example demonstrates reading OTLP protobuf test data files and
//! converting them to Parquet format using otlp2records.
//!
//! Run with: cargo run --example pb_to_parquet --features parquet

use otlp2records::{to_parquet, transform_logs, transform_metrics, transform_traces, InputFormat};
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let testdata_dir = Path::new("testdata");
    let output_dir = testdata_dir.join("output");

    // Create output directory
    fs::create_dir_all(&output_dir)?;

    // Convert logs
    convert_logs(testdata_dir, &output_dir)?;

    // Convert traces
    convert_traces(testdata_dir, &output_dir)?;

    // Convert metrics
    convert_metrics(testdata_dir, &output_dir)?;

    println!(
        "\nAll conversions complete! Output files in: {}",
        output_dir.display()
    );
    Ok(())
}

fn convert_logs(testdata_dir: &Path, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let log_files = ["logs_large.pb"];

    for filename in log_files {
        let input_path = testdata_dir.join(filename);
        if !input_path.exists() {
            continue;
        }

        let bytes = fs::read(&input_path)?;
        let batch = transform_logs(&bytes, InputFormat::Protobuf)?;

        let output_filename = filename.replace(".pb", ".parquet");
        let output_path = output_dir.join(&output_filename);

        let parquet_bytes = to_parquet(&batch)?;
        fs::write(&output_path, parquet_bytes)?;

        println!(
            "Converted {} -> {} ({} rows)",
            filename,
            output_filename,
            batch.num_rows()
        );
    }

    Ok(())
}

fn convert_traces(
    testdata_dir: &Path,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let trace_files = ["traces_large.pb"];

    for filename in trace_files {
        let input_path = testdata_dir.join(filename);
        if !input_path.exists() {
            continue;
        }

        let bytes = fs::read(&input_path)?;
        let batch = transform_traces(&bytes, InputFormat::Protobuf)?;

        let output_filename = filename.replace(".pb", ".parquet");
        let output_path = output_dir.join(&output_filename);

        let parquet_bytes = to_parquet(&batch)?;
        fs::write(&output_path, parquet_bytes)?;

        println!(
            "Converted {} -> {} ({} rows)",
            filename,
            output_filename,
            batch.num_rows()
        );
    }

    Ok(())
}

fn convert_metrics(
    testdata_dir: &Path,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let metric_files = [
        "metrics_gauge.pb",
        "metrics_sum.pb",
        "metrics_histogram.pb",
        "metrics_exponential_histogram.pb",
        "metrics_mixed.pb",
        "metrics_summary.pb",
    ];

    for filename in metric_files {
        let input_path = testdata_dir.join(filename);
        if !input_path.exists() {
            continue;
        }

        let bytes = fs::read(&input_path)?;
        let batches = transform_metrics(&bytes, InputFormat::Protobuf)?;

        let base_name = filename.replace(".pb", "");

        // Write each metric type that has data
        if let Some(gauge) = &batches.gauge {
            let output_path = output_dir.join(format!("{base_name}_gauge.parquet"));
            let parquet_bytes = to_parquet(gauge)?;
            fs::write(&output_path, parquet_bytes)?;
            println!(
                "Converted {} -> {}_gauge.parquet ({} rows)",
                filename,
                base_name,
                gauge.num_rows()
            );
        }

        if let Some(sum) = &batches.sum {
            let output_path = output_dir.join(format!("{base_name}_sum.parquet"));
            let parquet_bytes = to_parquet(sum)?;
            fs::write(&output_path, parquet_bytes)?;
            println!(
                "Converted {} -> {}_sum.parquet ({} rows)",
                filename,
                base_name,
                sum.num_rows()
            );
        }

        if let Some(histogram) = &batches.histogram {
            let output_path = output_dir.join(format!("{base_name}_histogram.parquet"));
            let parquet_bytes = to_parquet(histogram)?;
            fs::write(&output_path, parquet_bytes)?;
            println!(
                "Converted {} -> {}_histogram.parquet ({} rows)",
                filename,
                base_name,
                histogram.num_rows()
            );
        }

        if let Some(exp_histogram) = &batches.exp_histogram {
            let output_path = output_dir.join(format!("{base_name}_exp_histogram.parquet"));
            let parquet_bytes = to_parquet(exp_histogram)?;
            fs::write(&output_path, parquet_bytes)?;
            println!(
                "Converted {} -> {}_exp_histogram.parquet ({} rows)",
                filename,
                base_name,
                exp_histogram.num_rows()
            );
        }

        // Report skipped metrics
        if batches.skipped.has_skipped() {
            println!("  Skipped: {:?}", batches.skipped);
        }
    }

    Ok(())
}
