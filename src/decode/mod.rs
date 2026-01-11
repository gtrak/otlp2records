//! OTLP decode layer - transforms raw bytes into VRL Values
//!
//! This module provides decoders for OTLP logs, traces, and metrics in both
//! protobuf and JSON formats. The output is `Vec<Value>` where each Value
//! represents a single record ready for transformation.
//!
//! # Usage
//!
//! ```ignore
//! use otlp2records::decode::{decode_logs, InputFormat};
//!
//! let records = decode_logs(bytes, InputFormat::Protobuf)?;
//! ```
//!
//! # Simplifications from otlp2pipeline
//!
//! - No Gzip decompression (caller's responsibility)

mod common;
mod logs;
mod metrics;
mod traces;

pub use common::{looks_like_json, DecodeError};
pub use metrics::{DecodeMetricsResult, SkippedMetrics};
use vrl::value::Value;

/// Input format for OTLP decoding
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputFormat {
    /// Protocol Buffers binary format
    Protobuf,
    /// JSON format
    Json,
    /// Auto-detect JSON vs protobuf, with fallback decoding
    Auto,
}

impl InputFormat {
    /// Infer input format from Content-Type header.
    pub fn from_content_type(content_type: Option<&str>) -> Self {
        let content_type = content_type.map(|v| v.trim().to_ascii_lowercase());

        match content_type.as_deref() {
            Some("application/json") | Some("application/otlp+json") => InputFormat::Json,
            Some("application/x-protobuf")
            | Some("application/protobuf")
            | Some("application/otlp") => InputFormat::Protobuf,
            _ => InputFormat::Auto,
        }
    }
}

/// Decode OTLP logs from raw bytes into VRL Values.
///
/// Each returned Value represents a single log record with fields:
/// - `time_unix_nano`: i64
/// - `observed_time_unix_nano`: i64
/// - `severity_number`: i64
/// - `severity_text`: string
/// - `body`: any VRL value
/// - `trace_id`: hex string
/// - `span_id`: hex string
/// - `attributes`: object
/// - `resource`: object with `attributes`
/// - `scope`: object with `name`, `version`, `attributes`
pub fn decode_logs(bytes: &[u8], format: InputFormat) -> Result<Vec<Value>, DecodeError> {
    match format {
        InputFormat::Protobuf => logs::decode_protobuf(bytes),
        InputFormat::Json => logs::decode_json(bytes),
        InputFormat::Auto => {
            if looks_like_json(bytes) {
                match logs::decode_json(bytes) {
                    Ok(values) => Ok(values),
                    Err(json_err) => logs::decode_protobuf(bytes).map_err(|proto_err| {
                        DecodeError::Unsupported(format!(
                            "json decode failed: {}; protobuf fallback failed: {}",
                            json_err, proto_err
                        ))
                    }),
                }
            } else {
                match logs::decode_protobuf(bytes) {
                    Ok(values) => Ok(values),
                    Err(proto_err) => logs::decode_json(bytes).map_err(|json_err| {
                        DecodeError::Unsupported(format!(
                            "protobuf decode failed: {}; json fallback failed: {}",
                            proto_err, json_err
                        ))
                    }),
                }
            }
        }
    }
}

/// Decode OTLP traces from raw bytes into VRL Values.
///
/// Each returned Value represents a single span with fields:
/// - `trace_id`: hex string
/// - `span_id`: hex string
/// - `parent_span_id`: hex string
/// - `trace_state`: string
/// - `name`: string
/// - `kind`: i64 (0-5)
/// - `start_time_unix_nano`: i64
/// - `end_time_unix_nano`: i64
/// - `duration_ns`: i64 (computed)
/// - `attributes`: object
/// - `status_code`: i64 (0-2)
/// - `status_message`: string
/// - `events`: array of event objects
/// - `links`: array of link objects
/// - `resource`: object with `attributes`
/// - `scope`: object with `name`, `version`, `attributes`
/// - `dropped_*_count`: i64
/// - `flags`: i64
pub fn decode_traces(bytes: &[u8], format: InputFormat) -> Result<Vec<Value>, DecodeError> {
    match format {
        InputFormat::Protobuf => traces::decode_protobuf(bytes),
        InputFormat::Json => traces::decode_json(bytes),
        InputFormat::Auto => {
            if looks_like_json(bytes) {
                match traces::decode_json(bytes) {
                    Ok(values) => Ok(values),
                    Err(json_err) => traces::decode_protobuf(bytes).map_err(|proto_err| {
                        DecodeError::Unsupported(format!(
                            "json decode failed: {}; protobuf fallback failed: {}",
                            json_err, proto_err
                        ))
                    }),
                }
            } else {
                match traces::decode_protobuf(bytes) {
                    Ok(values) => Ok(values),
                    Err(proto_err) => traces::decode_json(bytes).map_err(|json_err| {
                        DecodeError::Unsupported(format!(
                            "protobuf decode failed: {}; json fallback failed: {}",
                            proto_err, json_err
                        ))
                    }),
                }
            }
        }
    }
}

/// Decode OTLP metrics from raw bytes into VRL Values.
///
/// Each returned Value represents a single metric data point with fields:
/// - `time_unix_nano`: i64
/// - `start_time_unix_nano`: i64
/// - `metric_name`: string
/// - `metric_description`: string
/// - `metric_unit`: string
/// - `value`: float64
/// - `attributes`: object
/// - `resource`: object with `attributes`
/// - `scope`: object with `name`, `version`, `attributes`
/// - `flags`: i64
/// - `exemplars`: array of exemplar objects
/// - `_metric_type`: "gauge" or "sum"
///
/// For sum metrics, additional fields:
/// - `aggregation_temporality`: i64
/// - `is_monotonic`: bool
///
/// # Skipped Metrics
///
/// The following are skipped and tracked in the returned [`DecodeMetricsResult::skipped`]:
/// - Histogram, ExponentialHistogram, and Summary metric types (not supported)
/// - Data points with non-finite values (NaN, Infinity)
/// - Data points with missing values
///
/// Use [`SkippedMetrics::has_skipped()`] to check if any data was dropped.
pub fn decode_metrics(
    bytes: &[u8],
    format: InputFormat,
) -> Result<DecodeMetricsResult, DecodeError> {
    match format {
        InputFormat::Protobuf => metrics::decode_protobuf(bytes),
        InputFormat::Json => metrics::decode_json(bytes),
        InputFormat::Auto => {
            if looks_like_json(bytes) {
                match metrics::decode_json(bytes) {
                    Ok(values) => Ok(values),
                    Err(json_err) => metrics::decode_protobuf(bytes).map_err(|proto_err| {
                        DecodeError::Unsupported(format!(
                            "json decode failed: {}; protobuf fallback failed: {}",
                            json_err, proto_err
                        ))
                    }),
                }
            } else {
                match metrics::decode_protobuf(bytes) {
                    Ok(values) => Ok(values),
                    Err(proto_err) => metrics::decode_json(bytes).map_err(|json_err| {
                        DecodeError::Unsupported(format!(
                            "protobuf decode failed: {}; json fallback failed: {}",
                            proto_err, json_err
                        ))
                    }),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_format_debug() {
        assert_eq!(format!("{:?}", InputFormat::Protobuf), "Protobuf");
        assert_eq!(format!("{:?}", InputFormat::Json), "Json");
        assert_eq!(format!("{:?}", InputFormat::Auto), "Auto");
    }

    #[test]
    fn input_format_equality() {
        assert_eq!(InputFormat::Protobuf, InputFormat::Protobuf);
        assert_ne!(InputFormat::Protobuf, InputFormat::Json);
        assert_ne!(InputFormat::Json, InputFormat::Auto);
    }
}
