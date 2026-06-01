use arrow_array::builder::{BooleanBuilder, ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Float32Type, Float64Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray,
};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_select::filter::filter_record_batch;
#[cfg(unix)]
use libc::{off_t, posix_fadvise, POSIX_FADV_DONTNEED};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs::File;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::coord::read_coord_groups;
use crate::parser::{parse_json_f64_array, parse_json_i32_array, parse_json_string_array};
use crate::reference::{build_dense_index, load_reference_map, DenseReferenceMap};

const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Deserialize)]
struct RestoreConfig {
    key_column: String,
    order_column: String,
    #[serde(default)]
    value_columns: Vec<String>,
    value_column: Option<String>,
    coord_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WriterConfig {
    output_file_name: Option<String>,
    file_name_rule: Option<String>,
    source_stem: Option<String>,
    coord_chunk_id: Option<usize>,
    #[serde(default)]
    partition_columns: Vec<String>,
    file_name_prefix: Option<String>,
    #[serde(default)]
    projection_columns: Vec<ProjectionColumn>,
    reference_replace: Option<ReferenceReplaceConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct ProjectionColumn {
    name: String,
    source: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ReferenceReplaceConfig {
    reference_parquet: String,
    source_column: String,
    reference_input_column: String,
    reference_output_column: String,
}

#[derive(Clone, Copy, Debug)]
enum ValueColumnKind {
    Float32,
    Float64,
    Integer,
    Text,
}

enum ValueColumnBuilder {
    Float32(ListBuilder<PrimitiveBuilder<Float32Type>>),
    Float64(ListBuilder<PrimitiveBuilder<Float64Type>>),
    Integer(ListBuilder<PrimitiveBuilder<Int32Type>>),
    Text(ListBuilder<StringBuilder>),
}

struct ManagedParquetWriter {
    writer: ArrowWriter<File>,
    final_path: PathBuf,
    temp_path: PathBuf,
}

#[derive(Default)]
struct DetailedProfile {
    batches_processed: usize,
    input_rows: usize,
    max_batch_rows: usize,
    max_dense_len: usize,
    max_restored_batch_array_bytes: usize,
    sum_restored_batch_array_bytes: usize,
    source_extract_sec: f64,
    dense_restore_sec: f64,
    record_batch_build_sec: f64,
    writer_write_sec: f64,
    cache_hint_sec: f64,
    cache_hint_calls: usize,
}

fn normalize_dtype(dtype: &str) -> &str {
    match dtype {
        "TEXT" | "Utf8" | "String" => "TEXT",
        "DATE" | "Date" => "DATE",
        "TIMESTAMP" | "Datetime" => "TIMESTAMP",
        "TINYINT" | "Int8" => "TINYINT",
        "INTEGER" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64" => {
            "INTEGER"
        }
        "FLOAT" | "Float32" => "FLOAT",
        "DOUBLE" | "Float64" => "DOUBLE",
        "INTEGER[]" | "List(Int8)" | "List(Int16)" | "List(Int32)" | "List(Int64)" => "INTEGER[]",
        "FLOAT[]" | "List(Float32)" => "FLOAT[]",
        "DOUBLE[]" | "List(Float64)" => "DOUBLE[]",
        "TEXT[]" | "List(Utf8)" | "List(String)" => "TEXT[]",
        _ => dtype,
    }
}

fn parse_config(config_json: &str) -> pyo3::PyResult<RestoreConfig> {
    let mut config: RestoreConfig = serde_json::from_str(config_json).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid restore config: {err}"))
    })?;
    if config.coord_columns.len() != 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "coord_columns must contain exactly 2 items, got {}",
            config.coord_columns.len()
        )));
    }
    if config.value_columns.is_empty() {
        if let Some(value_column) = config.value_column.clone() {
            config.value_columns.push(value_column);
        }
    }
    if config.value_columns.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "value_column 또는 value_columns 를 반드시 지정해야 합니다.",
        ));
    }
    Ok(config)
}

#[cfg(unix)]
fn drop_file_cache_hint(file: &File) {
    let fd = file.as_raw_fd();
    unsafe {
        let _ = posix_fadvise(fd, 0 as off_t, 0 as off_t, POSIX_FADV_DONTNEED);
    }
}

#[cfg(not(unix))]
fn drop_file_cache_hint(_file: &File) {}

fn output_dtype_for_column(
    schema: &HashMap<String, String>,
    column_name: &str,
    input_dtype: &DataType,
) -> pyo3::PyResult<DataType> {
    let Some(raw_dtype) = schema.get(column_name) else {
        return Ok(input_dtype.clone());
    };
    let normalized = normalize_dtype(raw_dtype);
    let output = match normalized {
        "TEXT" => DataType::Utf8,
        "DATE" => DataType::Date32,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TINYINT" => DataType::Int8,
        "INTEGER" => DataType::Int32,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "FLOAT[]" => DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
        "DOUBLE[]" => DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        "INTEGER[]" => DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        "TEXT[]" => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        _ => input_dtype.clone(),
    };
    Ok(output)
}

fn value_column_kind(
    schema: &HashMap<String, String>,
    column_name: &str,
) -> pyo3::PyResult<ValueColumnKind> {
    let Some(raw_dtype) = schema.get(column_name) else {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "missing schema for restore value column {column_name}"
        )));
    };
    match normalize_dtype(raw_dtype) {
        "FLOAT[]" => Ok(ValueColumnKind::Float32),
        "DOUBLE[]" => Ok(ValueColumnKind::Float64),
        "INTEGER[]" => Ok(ValueColumnKind::Integer),
        "TEXT[]" => Ok(ValueColumnKind::Text),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "restore value column {column_name} must be INTEGER[]/DOUBLE[]/TEXT[], got {other}"
        ))),
    }
}

fn build_value_column_builders(
    schema: &HashMap<String, String>,
    value_columns: &[String],
) -> pyo3::PyResult<Vec<(ValueColumnKind, ValueColumnBuilder)>> {
    value_columns
        .iter()
        .map(|column_name| {
            let kind = value_column_kind(schema, column_name)?;
            let builder = match kind {
                ValueColumnKind::Float32 => ValueColumnBuilder::Float32(ListBuilder::new(
                    PrimitiveBuilder::<Float32Type>::new(),
                )),
                ValueColumnKind::Float64 => ValueColumnBuilder::Float64(ListBuilder::new(
                    PrimitiveBuilder::<Float64Type>::new(),
                )),
                ValueColumnKind::Integer => ValueColumnBuilder::Integer(ListBuilder::new(
                    PrimitiveBuilder::<Int32Type>::new(),
                )),
                ValueColumnKind::Text => {
                    ValueColumnBuilder::Text(ListBuilder::new(StringBuilder::new()))
                }
            };
            Ok((kind, builder))
        })
        .collect()
}

fn restore_dense_values<T: Clone>(
    value_sparse: &[T],
    coord_a_sparse: &[i32],
    coord_b_sparse: &[i32],
    dense_index: &HashMap<(i32, i32), usize>,
    dense_len: usize,
) -> Vec<Option<T>> {
    let sparse_len = value_sparse
        .len()
        .min(coord_a_sparse.len())
        .min(coord_b_sparse.len());
    let mut dense = vec![None; dense_len];
    for idx in 0..sparse_len {
        let key = (coord_a_sparse[idx], coord_b_sparse[idx]);
        if let Some(&dense_pos) = dense_index.get(&key) {
            dense[dense_pos] = Some(value_sparse[idx].clone());
        }
    }
    dense
}

fn append_float_dense(
    builder: &mut ListBuilder<PrimitiveBuilder<Float64Type>>,
    dense_value: Vec<Option<f64>>,
) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn append_float32_dense(
    builder: &mut ListBuilder<PrimitiveBuilder<Float32Type>>,
    dense_value: Vec<Option<f64>>,
) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value as f32),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn append_int_dense(
    builder: &mut ListBuilder<PrimitiveBuilder<Int32Type>>,
    dense_value: Vec<Option<i32>>,
) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn append_text_dense(builder: &mut ListBuilder<StringBuilder>, dense_value: Vec<Option<String>>) {
    for item in dense_value {
        match item {
            Some(value) => builder.values().append_value(value),
            None => builder.values().append_null(),
        }
    }
    builder.append(true);
}

fn finish_value_column_builder(builder: ValueColumnBuilder) -> ArrayRef {
    match builder {
        ValueColumnBuilder::Float32(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Float64(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Integer(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Text(mut inner) => Arc::new(inner.finish()) as ArrayRef,
    }
}

fn batch_string_values(
    batch: &RecordBatch,
    name: &str,
    input_path: &str,
) -> pyo3::PyResult<Vec<String>> {
    let index = batch.schema().index_of(name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing {name} column in {input_path}: {err}"
        ))
    })?;
    if let Some(values) = batch.column(index).as_any().downcast_ref::<StringArray>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    String::new()
                } else {
                    values.value(row_index).to_string()
                }
            })
            .collect());
    }
    if let Some(values) = batch
        .column(index)
        .as_any()
        .downcast_ref::<LargeStringArray>()
    {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    String::new()
                } else {
                    values.value(row_index).to_string()
                }
            })
            .collect());
    }
    if let Some(values) = batch.column(index).as_any().downcast_ref::<BinaryArray>() {
        return binary_array_to_strings(values, name, input_path);
    }
    if let Some(values) = batch
        .column(index)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
    {
        return large_binary_array_to_strings(values, name, input_path);
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "restore source column {name} must be string-like in {input_path}: {:?}",
        batch.column(index).data_type()
    )))
}

fn binary_array_to_strings(
    values: &BinaryArray,
    name: &str,
    input_path: &str,
) -> pyo3::PyResult<Vec<String>> {
    (0..values.len())
        .map(|row_index| {
            if values.is_null(row_index) {
                return Ok(String::new());
            }
            std::str::from_utf8(values.value(row_index))
                .map(|value| value.to_string())
                .map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "restore source column {name} contains invalid utf-8 bytes in {input_path} at row {row_index}: {err}"
                    ))
                })
        })
        .collect()
}

fn large_binary_array_to_strings(
    values: &LargeBinaryArray,
    name: &str,
    input_path: &str,
) -> pyo3::PyResult<Vec<String>> {
    (0..values.len())
        .map(|row_index| {
            if values.is_null(row_index) {
                return Ok(String::new());
            }
            std::str::from_utf8(values.value(row_index))
                .map(|value| value.to_string())
                .map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "restore source column {name} contains invalid utf-8 bytes in {input_path} at row {row_index}: {err}"
                    ))
                })
        })
        .collect()
}

fn batch_value_as_path_component(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    column_name: &str,
) -> pyo3::PyResult<String> {
    let column = batch.column(column_index);
    if column.is_null(row_index) {
        return Ok("__null__".to_string());
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(sanitize_path_component(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(sanitize_path_component(values.value(row_index)));
    }
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok(values.value(row_index).to_string());
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.value(row_index).to_string());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "partition column {column_name} must be string/int32/int64, got {:?}",
        column.data_type()
    )))
}

fn sanitize_path_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|item| match item {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => item,
        })
        .collect();
    if sanitized.is_empty() {
        "__empty__".to_string()
    } else {
        sanitized
    }
}

fn build_output_schema(
    input_schema: &Schema,
    schema: &HashMap<String, String>,
) -> pyo3::PyResult<Arc<Schema>> {
    let mut fields = Vec::with_capacity(input_schema.fields().len());
    for field in input_schema.fields() {
        fields.push(Arc::new(Field::new(
            field.name(),
            output_dtype_for_column(schema, field.name(), field.data_type())?,
            field.is_nullable(),
        )));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn cast_array_for_output(
    array: ArrayRef,
    field: &Field,
    context: &str,
) -> pyo3::PyResult<ArrayRef> {
    if array.data_type() == field.data_type() {
        return Ok(array);
    }
    cast(array.as_ref(), field.data_type()).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to cast {context} to {:?}: {err}",
            field.data_type()
        ))
    })
}

fn apply_projection(
    batch: &RecordBatch,
    projection_columns: &[ProjectionColumn],
) -> pyo3::PyResult<RecordBatch> {
    if projection_columns.is_empty() {
        return Ok(batch.clone());
    }
    let mut fields = Vec::with_capacity(projection_columns.len());
    let mut columns = Vec::with_capacity(projection_columns.len());
    for projection in projection_columns {
        let source_name = projection.source.as_deref().unwrap_or(&projection.name);
        let index = batch.schema().index_of(source_name).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "missing projection source column {source_name}: {err}"
            ))
        })?;
        let batch_schema = batch.schema();
        let source_field = batch_schema.field(index);
        fields.push(Arc::new(Field::new(
            &projection.name,
            source_field.data_type().clone(),
            source_field.is_nullable(),
        )));
        columns.push(batch.column(index).clone());
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build projected record batch: {err}"
        ))
    })
}

fn scalar_column_as_strings(
    batch: &RecordBatch,
    column_name: &str,
    context_path: &str,
) -> pyo3::PyResult<Vec<Option<String>>> {
    let index = batch.schema().index_of(column_name).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing {column_name} column in {context_path}: {err}"
        ))
    })?;
    let column = batch.column(index);
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..batch.num_rows())
            .map(|row_index| {
                if values.is_null(row_index) {
                    None
                } else {
                    Some(values.value(row_index).to_string())
                }
            })
            .collect());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "reference replace column {column_name} must be string/int32/int64-like in {context_path}: {:?}",
        column.data_type()
    )))
}

fn load_reference_replace_map(
    config: &ReferenceReplaceConfig,
) -> pyo3::PyResult<HashMap<String, String>> {
    let file = File::open(&config.reference_parquet).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open reference_replace parquet {}: {err}",
            config.reference_parquet
        ))
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize reference_replace parquet reader {}: {err}",
                config.reference_parquet
            ))
        })?
        .build()
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to build reference_replace parquet reader {}: {err}",
                config.reference_parquet
            ))
        })?;
    let mut mapping = HashMap::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to read reference_replace batch {}: {err}",
                config.reference_parquet
            ))
        })?;
        let keys = scalar_column_as_strings(
            &batch,
            &config.reference_input_column,
            &config.reference_parquet,
        )?;
        let values = scalar_column_as_strings(
            &batch,
            &config.reference_output_column,
            &config.reference_parquet,
        )?;
        for (key, value) in keys.into_iter().zip(values.into_iter()) {
            if let (Some(key), Some(value)) = (key, value) {
                mapping.insert(key, value);
            }
        }
    }
    Ok(mapping)
}

fn apply_reference_replace(
    batch: &RecordBatch,
    config: &ReferenceReplaceConfig,
    mapping: &HashMap<String, String>,
    context_path: &str,
) -> pyo3::PyResult<RecordBatch> {
    let source_index = batch
        .schema()
        .index_of(&config.source_column)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "missing reference_replace source column {} in {context_path}: {err}",
                config.source_column
            ))
        })?;
    let source_values = scalar_column_as_strings(batch, &config.source_column, context_path)?;
    let mut replacement_builder = StringBuilder::new();
    for value in source_values {
        match value {
            Some(original) => {
                if let Some(replaced) = mapping.get(&original) {
                    replacement_builder.append_value(replaced);
                } else {
                    replacement_builder.append_value(original);
                }
            }
            None => replacement_builder.append_null(),
        }
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    let batch_schema = batch.schema();
    for index in 0..batch.num_columns() {
        let field = batch_schema.field(index);
        if index == source_index {
            fields.push(Arc::new(Field::new(
                &config.source_column,
                DataType::Utf8,
                true,
            )));
            columns.push(Arc::new(replacement_builder.finish()) as ArrayRef);
        } else {
            fields.push(Arc::new(field.clone()));
            columns.push(batch.column(index).clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build reference_replace batch for {context_path}: {err}"
        ))
    })
}

fn build_dense_index_cache(
    refs: &DenseReferenceMap,
) -> pyo3::PyResult<HashMap<String, HashMap<(i32, i32), usize>>> {
    let mut cache = HashMap::with_capacity(refs.len());
    for (key, (coord_a, coord_b)) in refs {
        cache.insert(key.clone(), build_dense_index(coord_a, coord_b)?);
    }
    Ok(cache)
}

fn row_selection_from_offsets(
    row_offsets: &[usize],
    total_rows: usize,
) -> pyo3::PyResult<RowSelection> {
    if row_offsets.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "coord group must contain at least one row offset",
        ));
    }
    let mut selectors = Vec::new();
    let mut cursor = 0usize;
    for &offset in row_offsets {
        if offset >= total_rows {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "row_offset_in_group {offset} exceeds row group row count {total_rows}"
            )));
        }
        if offset > cursor {
            selectors.push(RowSelector::skip(offset - cursor));
        }
        selectors.push(RowSelector::select(1));
        cursor = offset + 1;
    }
    if cursor < total_rows {
        selectors.push(RowSelector::skip(total_rows - cursor));
    }
    Ok(RowSelection::from(selectors))
}

fn partition_key_for_row(
    batch: &RecordBatch,
    partition_columns: &[String],
    row_index: usize,
) -> pyo3::PyResult<Vec<(String, String)>> {
    partition_columns
        .iter()
        .map(|column_name| {
            let index = batch.schema().index_of(column_name).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "missing partition column {column_name}: {err}"
                ))
            })?;
            let value = batch_value_as_path_component(batch, index, row_index, column_name)?;
            Ok((column_name.clone(), value))
        })
        .collect()
}

fn output_path_for_partition(
    output_dir: &str,
    writer_config: &WriterConfig,
    partition_key: &[(String, String)],
    row_group_id: usize,
) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(output_dir);
    for (column, value) in partition_key {
        path.push(format!("{}={}", sanitize_path_component(column), value));
    }
    let file_name = if partition_key.is_empty() {
        render_output_file_name(writer_config, Some(row_group_id))
    } else {
        render_output_file_name(writer_config, Some(row_group_id))
    };
    path.push(file_name);
    path
}

fn render_output_file_name(writer_config: &WriterConfig, row_group_id: Option<usize>) -> String {
    if let Some(rule) = &writer_config.file_name_rule {
        let source_stem = writer_config.source_stem.as_deref().unwrap_or("source");
        let chunk_id = writer_config.coord_chunk_id.unwrap_or(0);
        let row_group = row_group_id.unwrap_or(0);
        return rule
            .replace("<source_stem>", source_stem)
            .replace("{source_stem}", source_stem)
            .replace("<chunk_id>", &format!("{chunk_id:06}"))
            .replace("{chunk_id}", &format!("{chunk_id:06}"))
            .replace("<row_group_id>", &format!("{row_group:06}"))
            .replace("{row_group_id}", &format!("{row_group:06}"));
    }
    writer_config.output_file_name.clone().unwrap_or_else(|| {
        let prefix = writer_config
            .file_name_prefix
            .clone()
            .unwrap_or_else(|| "part".to_string());
        format!("{prefix}-00000.parquet")
    })
}

fn write_partitioned_batch(
    writers: &mut HashMap<String, ManagedParquetWriter>,
    output_dir: &str,
    writer_config: &WriterConfig,
    output_schema: Arc<Schema>,
    batch: &RecordBatch,
    row_group_id: usize,
) -> pyo3::PyResult<usize> {
    if writer_config.partition_columns.is_empty() {
        let output_path = output_path_for_partition(output_dir, writer_config, &[], row_group_id);
        write_batch_to_path(writers, output_path, output_schema, batch)?;
        return Ok(1);
    }

    let mut keys_by_row: Vec<Vec<(String, String)>> = Vec::with_capacity(batch.num_rows());
    let mut unique_keys: Vec<Vec<(String, String)>> = Vec::new();
    for row_index in 0..batch.num_rows() {
        let key = partition_key_for_row(batch, &writer_config.partition_columns, row_index)?;
        if !unique_keys.iter().any(|item| item == &key) {
            unique_keys.push(key.clone());
        }
        keys_by_row.push(key);
    }

    let mut files_touched = 0usize;
    for key in unique_keys {
        let mut builder = BooleanBuilder::with_capacity(batch.num_rows());
        for row_key in &keys_by_row {
            builder.append_value(row_key == &key);
        }
        let mask = builder.finish();
        let filtered = filter_record_batch(batch, &mask).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to filter partition batch: {err}"
            ))
        })?;
        if filtered.num_rows() == 0 {
            continue;
        }
        let output_path = output_path_for_partition(output_dir, writer_config, &key, row_group_id);
        write_batch_to_path(writers, output_path, output_schema.clone(), &filtered)?;
        files_touched += 1;
    }
    Ok(files_touched)
}

fn write_batch_to_path(
    writers: &mut HashMap<String, ManagedParquetWriter>,
    output_path: PathBuf,
    output_schema: Arc<Schema>,
    batch: &RecordBatch,
) -> pyo3::PyResult<()> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to create output directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    let key = output_path.to_string_lossy().to_string();
    if !writers.contains_key(&key) {
        let temp_path = temp_output_path(&output_path);
        let output_file = File::create(&temp_path).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to create temp output parquet {}: {err}",
                temp_path.display()
            ))
        })?;
        let writer = ArrowWriter::try_new(output_file, output_schema, None).map_err(|err| {
            let _ = std::fs::remove_file(&temp_path);
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize parquet writer {}: {err}",
                temp_path.display()
            ))
        })?;
        writers.insert(
            key.clone(),
            ManagedParquetWriter {
                writer,
                final_path: output_path.clone(),
                temp_path,
            },
        );
    }
    writers
        .get_mut(&key)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("missing partition writer"))?
        .writer
        .write(batch)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to write restored parquet batch {}: {err}",
                output_path.display()
            ))
        })
}

fn temp_output_path(output_path: &std::path::Path) -> PathBuf {
    let file_name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("part.parquet");
    output_path.with_file_name(format!(".{file_name}.tmp"))
}

fn restore_batch_columns(
    batch: &RecordBatch,
    input_path: &str,
    config: &RestoreConfig,
    schema: &HashMap<String, String>,
    refs: &DenseReferenceMap,
    dense_index_cache: &HashMap<String, HashMap<(i32, i32), usize>>,
    output_schema: Arc<Schema>,
    detailed_profile: Option<&mut DetailedProfile>,
) -> pyo3::PyResult<RecordBatch> {
    let extract_started = Instant::now();
    let key_values = batch_string_values(batch, &config.key_column, input_path)?;
    let value_sparse_jsons: Vec<Vec<String>> = config
        .value_columns
        .iter()
        .map(|column_name| batch_string_values(batch, column_name, input_path))
        .collect::<pyo3::PyResult<Vec<_>>>()?;
    let coord_a_sparse_json = batch_string_values(batch, &config.coord_columns[0], input_path)?;
    let coord_b_sparse_json = batch_string_values(batch, &config.coord_columns[1], input_path)?;
    let extract_sec = extract_started.elapsed().as_secs_f64();

    let mut value_builders = build_value_column_builders(schema, &config.value_columns)?;
    let mut coord_a_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());
    let mut coord_b_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());

    let dense_restore_started = Instant::now();
    for row_index in 0..batch.num_rows() {
        let group_key = &key_values[row_index];
        let (dense_coord_a, dense_coord_b) = refs.get(group_key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown lookup key '{group_key}'"))
        })?;
        let dense_index = dense_index_cache.get(group_key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "missing dense index for lookup key '{group_key}'"
            ))
        })?;

        let coord_a_sparse = parse_json_i32_array(&coord_a_sparse_json[row_index])?;
        let coord_b_sparse = parse_json_i32_array(&coord_b_sparse_json[row_index])?;

        for ((kind, builder), value_sparse_json) in
            value_builders.iter_mut().zip(value_sparse_jsons.iter())
        {
            match kind {
                ValueColumnKind::Float32 => {
                    let value_sparse = parse_json_f64_array(&value_sparse_json[row_index])?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_sparse,
                        &coord_b_sparse,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Float32(inner) = builder {
                        append_float32_dense(inner, dense_value);
                    }
                }
                ValueColumnKind::Float64 => {
                    let value_sparse = parse_json_f64_array(&value_sparse_json[row_index])?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_sparse,
                        &coord_b_sparse,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Float64(inner) = builder {
                        append_float_dense(inner, dense_value);
                    }
                }
                ValueColumnKind::Integer => {
                    let value_sparse = parse_json_i32_array(&value_sparse_json[row_index])?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_sparse,
                        &coord_b_sparse,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Integer(inner) = builder {
                        append_int_dense(inner, dense_value);
                    }
                }
                ValueColumnKind::Text => {
                    let value_sparse = parse_json_string_array(&value_sparse_json[row_index])?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_sparse,
                        &coord_b_sparse,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Text(inner) = builder {
                        append_text_dense(inner, dense_value);
                    }
                }
            }
        }

        for item in dense_coord_a {
            coord_a_builder.values().append_value(*item);
        }
        coord_a_builder.append(true);

        for item in dense_coord_b {
            coord_b_builder.values().append_value(*item);
        }
        coord_b_builder.append(true);
    }
    let dense_restore_sec = dense_restore_started.elapsed().as_secs_f64();

    let finished_value_columns: HashMap<String, ArrayRef> = config
        .value_columns
        .iter()
        .cloned()
        .zip(
            value_builders
                .into_iter()
                .map(|(_, builder)| finish_value_column_builder(builder)),
        )
        .collect();

    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let batch_schema = batch.schema();
    for index in 0..batch.num_columns() {
        let field = batch_schema.field(index);
        let output_field = output_schema.field(index);
        let column_name = field.name();
        if let Some(restored_array) = finished_value_columns.get(column_name.as_str()) {
            output_columns.push(cast_array_for_output(
                restored_array.clone(),
                output_field,
                column_name,
            )?);
        } else if column_name == &config.coord_columns[0] {
            output_columns.push(cast_array_for_output(
                Arc::new(coord_a_builder.finish()) as ArrayRef,
                output_field,
                column_name,
            )?);
        } else if column_name == &config.coord_columns[1] {
            output_columns.push(cast_array_for_output(
                Arc::new(coord_b_builder.finish()) as ArrayRef,
                output_field,
                column_name,
            )?);
        } else {
            output_columns.push(cast_array_for_output(
                batch.column(index).clone(),
                output_field,
                column_name,
            )?);
        }
    }

    let build_started = Instant::now();
    let restored_batch = RecordBatch::try_new(output_schema, output_columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build restored record batch for {input_path}: {err}"
        ))
    })?;
    let build_sec = build_started.elapsed().as_secs_f64();

    if let Some(profile) = detailed_profile {
        profile.batches_processed += 1;
        profile.input_rows += batch.num_rows();
        profile.max_batch_rows = profile.max_batch_rows.max(batch.num_rows());
        profile.max_dense_len = profile.max_dense_len.max(
            refs.values()
                .map(|(coord_a, _)| coord_a.len())
                .max()
                .unwrap_or(0),
        );
        profile.source_extract_sec += extract_sec;
        profile.dense_restore_sec += dense_restore_sec;
        profile.record_batch_build_sec += build_sec;
    }

    Ok(restored_batch)
}

fn restore_parquet_to_parquet_internal(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
    detailed: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    if print_timing {
        println!(
            "[restore_list_columns_rs] version={PKG_VERSION} input_parquet_path={input_parquet_path} output_parquet_path={output_parquet_path}"
        );
    }

    let total_started = Instant::now();
    let config = parse_config(&config_json)?;

    let reference_started = Instant::now();
    let refs = load_reference_map(
        &lookup_path,
        &config.key_column,
        &config.order_column,
        &config.coord_columns,
    )?;
    let dense_index_cache = build_dense_index_cache(&refs)?;
    let reference_load_sec = reference_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[restore_list_columns_rs] reference_load_sec={reference_load_sec:.6}");
    }

    let input_file = File::open(&input_parquet_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open input parquet {input_parquet_path}: {err}"
        ))
    })?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(input_file).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to initialize parquet reader {input_parquet_path}: {err}"
        ))
    })?;
    if let Some(size) = batch_size {
        builder = builder.with_batch_size(size);
    }
    let output_schema = build_output_schema(builder.schema().as_ref(), &schema)?;
    let reader = builder.build().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build parquet reader {input_parquet_path}: {err}"
        ))
    })?;

    let output_file = File::create(&output_parquet_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create output parquet {output_parquet_path}: {err}"
        ))
    })?;
    let mut writer =
        ArrowWriter::try_new(output_file, output_schema.clone(), None).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize parquet writer {output_parquet_path}: {err}"
            ))
        })?;

    let restore_started = Instant::now();
    let mut rows_written = 0usize;
    let mut detailed_profile = DetailedProfile::default();
    for batch_result in reader {
        let batch = batch_result.map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to read parquet batch {input_parquet_path}: {err}"
            ))
        })?;
        let restored = restore_batch_columns(
            &batch,
            &input_parquet_path,
            &config,
            &schema,
            &refs,
            &dense_index_cache,
            output_schema.clone(),
            if detailed {
                Some(&mut detailed_profile)
            } else {
                None
            },
        )?;
        rows_written += restored.num_rows();
        if detailed {
            let restored_batch_array_bytes = restored.get_array_memory_size();
            detailed_profile.max_restored_batch_array_bytes = detailed_profile
                .max_restored_batch_array_bytes
                .max(restored_batch_array_bytes);
            detailed_profile.sum_restored_batch_array_bytes += restored_batch_array_bytes;
        }
        let writer_write_started = Instant::now();
        writer.write(&restored).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to write restored parquet batch {output_parquet_path}: {err}"
            ))
        })?;
        if detailed {
            detailed_profile.writer_write_sec += writer_write_started.elapsed().as_secs_f64();
        }
    }
    let restore_sec = restore_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[restore_list_columns_rs] restore_sec={restore_sec:.6}");
    }

    let write_started = Instant::now();
    writer.close().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to finalize output parquet {output_parquet_path}: {err}"
        ))
    })?;
    let parquet_write_sec = write_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[restore_list_columns_rs] parquet_write_sec={parquet_write_sec:.6}");
    }

    let total_sec = total_started.elapsed().as_secs_f64();
    if print_timing {
        println!("[restore_list_columns_rs] total_sec={total_sec:.6}");
    }

    if drop_cache_hint {
        let cache_hint_started = Instant::now();
        if let Ok(file) = File::open(&input_parquet_path) {
            drop_file_cache_hint(&file);
            if detailed {
                detailed_profile.cache_hint_calls += 1;
            }
        }
        if let Ok(file) = File::open(&output_parquet_path) {
            drop_file_cache_hint(&file);
            if detailed {
                detailed_profile.cache_hint_calls += 1;
            }
        }
        if detailed {
            detailed_profile.cache_hint_sec += cache_hint_started.elapsed().as_secs_f64();
        }
    }

    let mut stats = HashMap::new();
    stats.insert("rows_written".to_string(), rows_written as f64);
    stats.insert("selected_row_count".to_string(), rows_written as f64);
    stats.insert("reference_load_sec".to_string(), reference_load_sec);
    stats.insert("restore_sec".to_string(), restore_sec);
    stats.insert("parquet_write_sec".to_string(), parquet_write_sec);
    stats.insert("restore_elapsed_sec".to_string(), restore_sec);
    stats.insert("write_elapsed_sec".to_string(), parquet_write_sec);
    stats.insert("total_sec".to_string(), total_sec);
    if detailed {
        stats.insert(
            "batches_processed".to_string(),
            detailed_profile.batches_processed as f64,
        );
        stats.insert("input_rows".to_string(), detailed_profile.input_rows as f64);
        stats.insert(
            "max_batch_rows".to_string(),
            detailed_profile.max_batch_rows as f64,
        );
        stats.insert(
            "max_dense_len".to_string(),
            detailed_profile.max_dense_len as f64,
        );
        stats.insert(
            "max_restored_batch_array_bytes".to_string(),
            detailed_profile.max_restored_batch_array_bytes as f64,
        );
        stats.insert(
            "avg_restored_batch_array_bytes".to_string(),
            if detailed_profile.batches_processed == 0 {
                0.0
            } else {
                detailed_profile.sum_restored_batch_array_bytes as f64
                    / detailed_profile.batches_processed as f64
            },
        );
        stats.insert(
            "value_column_count".to_string(),
            config.value_columns.len() as f64,
        );
        stats.insert(
            "source_extract_sec".to_string(),
            detailed_profile.source_extract_sec,
        );
        stats.insert(
            "dense_restore_sec".to_string(),
            detailed_profile.dense_restore_sec,
        );
        stats.insert(
            "record_batch_build_sec".to_string(),
            detailed_profile.record_batch_build_sec,
        );
        stats.insert(
            "writer_write_sec".to_string(),
            detailed_profile.writer_write_sec,
        );
        stats.insert(
            "cache_hint_sec".to_string(),
            detailed_profile.cache_hint_sec,
        );
        stats.insert(
            "cache_hint_calls".to_string(),
            detailed_profile.cache_hint_calls as f64,
        );
        if let Ok(metadata) = std::fs::metadata(&output_parquet_path) {
            stats.insert("output_file_size_bytes".to_string(), metadata.len() as f64);
        }
    }
    Ok(stats)
}

pub fn restore_parquet_to_parquet_impl(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    restore_parquet_to_parquet_internal(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
        false,
    )
}

pub fn restore_parquet_to_parquet_profiled_impl(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    restore_parquet_to_parquet_internal(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        false,
        true,
    )
}

pub fn restore_with_coord_file_impl(
    coord_path: String,
    output_dir: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    writer_config_json: Option<String>,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    if print_timing {
        println!(
            "[restore_list_columns_rs] version={PKG_VERSION} coord_path={coord_path} output_dir={output_dir}"
        );
    }

    let total_started = Instant::now();
    let config = parse_config(&config_json)?;
    let writer_config = match writer_config_json {
        Some(raw) if !raw.trim().is_empty() => {
            serde_json::from_str::<WriterConfig>(&raw).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid writer config: {err}"))
            })?
        }
        _ => WriterConfig {
            output_file_name: None,
            file_name_rule: None,
            source_stem: None,
            coord_chunk_id: None,
            partition_columns: Vec::new(),
            file_name_prefix: None,
            projection_columns: Vec::new(),
            reference_replace: None,
        },
    };

    let reference_started = Instant::now();
    let refs = load_reference_map(
        &lookup_path,
        &config.key_column,
        &config.order_column,
        &config.coord_columns,
    )?;
    let dense_index_cache = build_dense_index_cache(&refs)?;
    let reference_replace_map = match &writer_config.reference_replace {
        Some(config) => Some(load_reference_replace_map(config)?),
        None => None,
    };
    let reference_load_sec = reference_started.elapsed().as_secs_f64();

    let coord_started = Instant::now();
    let coord_groups = read_coord_groups(&coord_path)?;
    let coord_read_sec = coord_started.elapsed().as_secs_f64();
    if coord_groups.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "coord file has no row groups: {coord_path}"
        )));
    }

    std::fs::create_dir_all(&output_dir).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create output dir {output_dir}: {err}"
        ))
    })?;
    let mut writers: HashMap<String, ManagedParquetWriter> = HashMap::new();
    let mut output_schema: Option<Arc<Schema>> = None;
    let mut rows_written = 0usize;
    let mut source_files_seen: HashSet<String> = HashSet::new();
    let mut row_group_count = 0usize;
    let mut output_file_write_touches = 0usize;
    let mut detailed_profile = DetailedProfile::default();
    let restore_started = Instant::now();

    for group in &coord_groups {
        source_files_seen.insert(group.source_file.clone());
        row_group_count += 1;
        let input_file = File::open(&group.source_file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to open input parquet {}: {err}",
                group.source_file
            ))
        })?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(input_file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to initialize parquet reader {}: {err}",
                group.source_file
            ))
        })?;
        if group.row_group_id >= builder.metadata().num_row_groups() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "row_group_id {} exceeds row group count {} for {}",
                group.row_group_id,
                builder.metadata().num_row_groups(),
                group.source_file
            )));
        }
        let row_group_rows = builder
            .metadata()
            .row_group(group.row_group_id)
            .num_rows()
            .try_into()
            .map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid row count for row group {} in {}",
                    group.row_group_id, group.source_file
                ))
            })?;
        let selection = row_selection_from_offsets(&group.row_offsets, row_group_rows)?;
        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }
        let reader = builder
            .with_row_groups(vec![group.row_group_id])
            .with_row_selection(selection)
            .build()
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to build parquet reader {}: {err}",
                    group.source_file
                ))
            })?;
        for batch_result in reader {
            let raw_batch = batch_result.map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to read selected parquet batch {}: {err}",
                    group.source_file
                ))
            })?;
            let batch = apply_projection(&raw_batch, &writer_config.projection_columns)?;
            let restored = restore_batch_columns(
                &batch,
                &group.source_file,
                &config,
                &schema,
                &refs,
                &dense_index_cache,
                build_output_schema(batch.schema().as_ref(), &schema)?,
                Some(&mut detailed_profile),
            )?;
            let restored = match (&writer_config.reference_replace, &reference_replace_map) {
                (Some(config), Some(mapping)) => {
                    apply_reference_replace(&restored, config, mapping, &group.source_file)?
                }
                _ => restored,
            };
            let group_output_schema = match &output_schema {
                Some(existing) => existing.clone(),
                None => {
                    let built = build_output_schema(restored.schema().as_ref(), &schema)?;
                    output_schema = Some(built.clone());
                    built
                }
            };
            rows_written += restored.num_rows();
            let restored_batch_array_bytes = restored.get_array_memory_size();
            detailed_profile.max_restored_batch_array_bytes = detailed_profile
                .max_restored_batch_array_bytes
                .max(restored_batch_array_bytes);
            detailed_profile.sum_restored_batch_array_bytes += restored_batch_array_bytes;
            let writer_write_started = Instant::now();
            output_file_write_touches += write_partitioned_batch(
                &mut writers,
                &output_dir,
                &writer_config,
                group_output_schema.clone(),
                &restored,
                group.row_group_id,
            )?;
            detailed_profile.writer_write_sec += writer_write_started.elapsed().as_secs_f64();
        }
        if drop_cache_hint {
            if let Ok(file) = File::open(&group.source_file) {
                drop_file_cache_hint(&file);
                detailed_profile.cache_hint_calls += 1;
            }
        }
    }
    let restore_sec = restore_started.elapsed().as_secs_f64();

    let write_started = Instant::now();
    if writers.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "coord restore produced no output writers",
        ));
    }
    let mut output_paths: Vec<String> = Vec::new();
    for (_path, managed) in writers {
        managed.writer.close().map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to finalize output parquet {}: {err}",
                managed.temp_path.display()
            ))
        })?;
        if managed.final_path.exists() {
            std::fs::remove_file(&managed.final_path).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "failed to replace existing output parquet {}: {err}",
                    managed.final_path.display()
                ))
            })?;
        }
        std::fs::rename(&managed.temp_path, &managed.final_path).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to publish output parquet {} -> {}: {err}",
                managed.temp_path.display(),
                managed.final_path.display()
            ))
        })?;
        output_paths.push(managed.final_path.to_string_lossy().to_string());
    }
    let parquet_write_sec = write_started.elapsed().as_secs_f64();
    if drop_cache_hint {
        for output_path in &output_paths {
            if let Ok(file) = File::open(output_path) {
                drop_file_cache_hint(&file);
                detailed_profile.cache_hint_calls += 1;
            }
        }
    }

    let total_sec = total_started.elapsed().as_secs_f64();
    let mut stats = HashMap::new();
    stats.insert("rows_written".to_string(), rows_written as f64);
    stats.insert("selected_row_count".to_string(), rows_written as f64);
    stats.insert(
        "source_file_count".to_string(),
        source_files_seen.len() as f64,
    );
    stats.insert("row_group_count".to_string(), row_group_count as f64);
    stats.insert("output_file_count".to_string(), output_paths.len() as f64);
    let output_partition_count = if writer_config.partition_columns.is_empty() {
        0usize
    } else {
        output_paths
            .iter()
            .filter_map(|path| {
                std::path::Path::new(path)
                    .parent()
                    .map(|parent| parent.to_path_buf())
            })
            .collect::<HashSet<_>>()
            .len()
    };
    stats.insert(
        "output_partition_count".to_string(),
        output_partition_count as f64,
    );
    stats.insert(
        "output_file_write_touches".to_string(),
        output_file_write_touches as f64,
    );
    stats.insert("coord_groups".to_string(), coord_groups.len() as f64);
    stats.insert("coord_read_sec".to_string(), coord_read_sec);
    stats.insert("reference_load_sec".to_string(), reference_load_sec);
    stats.insert("restore_sec".to_string(), restore_sec);
    stats.insert("parquet_write_sec".to_string(), parquet_write_sec);
    stats.insert("restore_elapsed_sec".to_string(), restore_sec);
    stats.insert("write_elapsed_sec".to_string(), parquet_write_sec);
    stats.insert("total_sec".to_string(), total_sec);
    stats.insert(
        "batches_processed".to_string(),
        detailed_profile.batches_processed as f64,
    );
    stats.insert("input_rows".to_string(), detailed_profile.input_rows as f64);
    stats.insert(
        "max_batch_rows".to_string(),
        detailed_profile.max_batch_rows as f64,
    );
    stats.insert(
        "max_dense_len".to_string(),
        detailed_profile.max_dense_len as f64,
    );
    stats.insert(
        "max_restored_batch_array_bytes".to_string(),
        detailed_profile.max_restored_batch_array_bytes as f64,
    );
    stats.insert(
        "avg_restored_batch_array_bytes".to_string(),
        if detailed_profile.batches_processed == 0 {
            0.0
        } else {
            detailed_profile.sum_restored_batch_array_bytes as f64
                / detailed_profile.batches_processed as f64
        },
    );
    stats.insert(
        "value_column_count".to_string(),
        config.value_columns.len() as f64,
    );
    stats.insert(
        "source_extract_sec".to_string(),
        detailed_profile.source_extract_sec,
    );
    stats.insert(
        "dense_restore_sec".to_string(),
        detailed_profile.dense_restore_sec,
    );
    stats.insert(
        "record_batch_build_sec".to_string(),
        detailed_profile.record_batch_build_sec,
    );
    stats.insert(
        "writer_write_sec".to_string(),
        detailed_profile.writer_write_sec,
    );
    stats.insert(
        "cache_hint_calls".to_string(),
        detailed_profile.cache_hint_calls as f64,
    );
    let output_total_size_bytes: u64 = output_paths
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok().map(|metadata| metadata.len()))
        .sum();
    stats.insert(
        "output_file_size_bytes".to_string(),
        output_total_size_bytes as f64,
    );
    stats.insert(
        "output_total_size_bytes".to_string(),
        output_total_size_bytes as f64,
    );
    if print_timing {
        println!("[restore_list_columns_rs] coord_read_sec={coord_read_sec:.6}");
        println!("[restore_list_columns_rs] restore_sec={restore_sec:.6}");
        println!("[restore_list_columns_rs] parquet_write_sec={parquet_write_sec:.6}");
        println!("[restore_list_columns_rs] total_sec={total_sec:.6}");
    }
    Ok(stats)
}
