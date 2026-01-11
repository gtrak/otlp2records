//! Arrow layer for otlp2records
//!
//! Provides Arrow RecordBatch construction from VRL-transformed values:
//! - Schema accessors for logs, traces, and metrics
//! - RecordBatch builder for converting VRL Values to Arrow arrays

mod builder;
mod schema;

pub use builder::values_to_arrow;
pub use schema::{gauge_schema, logs_schema, sum_schema, traces_schema};
