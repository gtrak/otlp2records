//! Parquet output serialization
//!
//! Serializes Arrow RecordBatches to Parquet format.
//! This module is only available when the `parquet` feature is enabled.

use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::error::Error;

/// Serialize a RecordBatch to Parquet format
///
/// Creates a single Parquet file in memory (uncompressed by default).
/// The resulting bytes can be written to a file or sent over the network.
///
/// # Arguments
///
/// * `batch` - The RecordBatch to serialize
///
/// # Returns
///
/// * `Ok(Vec<u8>)` - The Parquet file as bytes
/// * `Err(Error)` - If serialization fails
///
/// # Example
///
/// ```ignore
/// use arrow::array::RecordBatch;
/// use otlp2records::output::to_parquet;
///
/// let batch: RecordBatch = /* create batch */;
/// let parquet_bytes = to_parquet(&batch)?;
/// std::fs::write("output.parquet", parquet_bytes)?;
/// ```
pub fn to_parquet(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();

    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();

    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))
            .map_err(|e| Error::Arrow(arrow::error::ArrowError::ExternalError(Box::new(e))))?;

        writer
            .write(batch)
            .map_err(|e| Error::Arrow(arrow::error::ArrowError::ExternalError(Box::new(e))))?;

        writer
            .close()
            .map_err(|e| Error::Arrow(arrow::error::ArrowError::ExternalError(Box::new(e))))?;
    }

    Ok(buffer)
}

/// Serialize a RecordBatch to Parquet format, returning Bytes
///
/// Same as `to_parquet` but returns `bytes::Bytes` for zero-copy scenarios.
///
/// # Arguments
///
/// * `batch` - The RecordBatch to serialize
///
/// # Returns
///
/// * `Ok(Bytes)` - The Parquet file as Bytes
/// * `Err(Error)` - If serialization fails
pub fn to_parquet_bytes(batch: &RecordBatch) -> Result<Bytes, Error> {
    let vec = to_parquet(batch)?;
    Ok(Bytes::from(vec))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let name_array = Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"]));
        let value_array = Arc::new(Int64Array::from(vec![1, 2, 3]));

        RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap()
    }

    #[test]
    fn test_to_parquet_basic() {
        let batch = create_test_batch();
        let result = to_parquet(&batch).unwrap();

        // Verify it's not empty and has parquet magic bytes
        assert!(!result.is_empty());
        // Parquet files start with "PAR1"
        assert_eq!(&result[0..4], b"PAR1");
    }

    #[test]
    fn test_to_parquet_roundtrip() {
        let original_batch = create_test_batch();
        let parquet_bytes = to_parquet(&original_batch).unwrap();

        // Read back using parquet reader
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes))
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);

        let read_batch = &batches[0];
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);

        // Verify data integrity
        let name_col = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(name_col.value(0), "alpha");
        assert_eq!(name_col.value(1), "beta");
        assert_eq!(name_col.value(2), "gamma");

        assert_eq!(value_col.value(0), 1);
        assert_eq!(value_col.value(1), 2);
        assert_eq!(value_col.value(2), 3);
    }

    #[test]
    fn test_to_parquet_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let name_array = Arc::new(StringArray::from(Vec::<&str>::new()));
        let value_array = Arc::new(Int64Array::from(Vec::<i64>::new()));

        let batch = RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap();
        let result = to_parquet(&batch).unwrap();

        // Should still produce valid Parquet
        assert!(!result.is_empty());
        assert_eq!(&result[0..4], b"PAR1");

        // Verify we can read it back
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(result))
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        // Empty batch may not produce any row groups
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_to_parquet_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let name_array = Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")]));
        let value_array = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch = RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap();
        let result = to_parquet(&batch).unwrap();

        // Verify roundtrip preserves nulls
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(result))
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        let read_batch = &batches[0];

        let name_col = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert!(!name_col.is_null(0));
        assert!(name_col.is_null(1));
        assert!(!name_col.is_null(2));

        assert!(!value_col.is_null(0));
        assert!(!value_col.is_null(1));
        assert!(value_col.is_null(2));
    }

    #[test]
    fn test_to_parquet_bytes() {
        let batch = create_test_batch();
        let result = to_parquet_bytes(&batch).unwrap();

        // Verify it's valid Parquet
        assert!(!result.is_empty());
        assert_eq!(&result[0..4], b"PAR1");
    }
}
