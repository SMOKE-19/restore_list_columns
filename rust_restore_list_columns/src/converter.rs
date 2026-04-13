use arrow_array::builder::{ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Float64Type, Int32Type};
use arrow_array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

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

#[derive(Clone, Copy, Debug)]
enum ValueColumnKind {
    Float,
    Integer,
    Text,
}

enum ValueColumnBuilder {
    Float(ListBuilder<PrimitiveBuilder<Float64Type>>),
    Integer(ListBuilder<PrimitiveBuilder<Int32Type>>),
    Text(ListBuilder<StringBuilder>),
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
        "DOUBLE[]" | "FLOAT[]" => DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        "INTEGER[]" => DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        "TEXT[]" => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        _ => input_dtype.clone(),
    };
    Ok(output)
}

fn value_column_kind(schema: &HashMap<String, String>, column_name: &str) -> pyo3::PyResult<ValueColumnKind> {
    let Some(raw_dtype) = schema.get(column_name) else {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "missing schema for restore value column {column_name}"
        )));
    };
    match normalize_dtype(raw_dtype) {
        "DOUBLE[]" | "FLOAT[]" => Ok(ValueColumnKind::Float),
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
                ValueColumnKind::Float => {
                    ValueColumnBuilder::Float(ListBuilder::new(PrimitiveBuilder::<Float64Type>::new()))
                }
                ValueColumnKind::Integer => {
                    ValueColumnBuilder::Integer(ListBuilder::new(PrimitiveBuilder::<Int32Type>::new()))
                }
                ValueColumnKind::Text => ValueColumnBuilder::Text(ListBuilder::new(StringBuilder::new())),
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
        ValueColumnBuilder::Float(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Integer(mut inner) => Arc::new(inner.finish()) as ArrayRef,
        ValueColumnBuilder::Text(mut inner) => Arc::new(inner.finish()) as ArrayRef,
    }
}

fn batch_string_values(batch: &RecordBatch, name: &str, input_path: &str) -> pyo3::PyResult<Vec<String>> {
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
    if let Some(values) = batch.column(index).as_any().downcast_ref::<LargeStringArray>() {
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
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "restore source column {name} must be string-like in {input_path}: {:?}",
        batch.column(index).data_type()
    )))
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

fn build_dense_index_cache(
    refs: &DenseReferenceMap,
) -> pyo3::PyResult<HashMap<String, HashMap<(i32, i32), usize>>> {
    let mut cache = HashMap::with_capacity(refs.len());
    for (key, (coord_a, coord_b)) in refs {
        cache.insert(key.clone(), build_dense_index(coord_a, coord_b)?);
    }
    Ok(cache)
}

fn restore_batch_columns(
    batch: &RecordBatch,
    input_path: &str,
    config: &RestoreConfig,
    schema: &HashMap<String, String>,
    refs: &DenseReferenceMap,
    dense_index_cache: &HashMap<String, HashMap<(i32, i32), usize>>,
    output_schema: Arc<Schema>,
) -> pyo3::PyResult<RecordBatch> {
    let key_values = batch_string_values(batch, &config.key_column, input_path)?;
    let value_sparse_jsons: Vec<Vec<String>> = config
        .value_columns
        .iter()
        .map(|column_name| batch_string_values(batch, column_name, input_path))
        .collect::<pyo3::PyResult<Vec<_>>>()?;
    let coord_a_sparse_json = batch_string_values(batch, &config.coord_columns[0], input_path)?;
    let coord_b_sparse_json = batch_string_values(batch, &config.coord_columns[1], input_path)?;

    let mut value_builders = build_value_column_builders(schema, &config.value_columns)?;
    let mut coord_a_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());
    let mut coord_b_builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::new());

    for row_index in 0..batch.num_rows() {
        let group_key = &key_values[row_index];
        let (dense_coord_a, dense_coord_b) = refs.get(group_key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown lookup key '{group_key}'"))
        })?;
        let dense_index = dense_index_cache.get(group_key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("missing dense index for lookup key '{group_key}'"))
        })?;

        let coord_a_sparse = parse_json_i32_array(&coord_a_sparse_json[row_index])?;
        let coord_b_sparse = parse_json_i32_array(&coord_b_sparse_json[row_index])?;

        for ((kind, builder), value_sparse_json) in value_builders.iter_mut().zip(value_sparse_jsons.iter()) {
            match kind {
                ValueColumnKind::Float => {
                    let value_sparse = parse_json_f64_array(&value_sparse_json[row_index])?;
                    let dense_value = restore_dense_values(
                        &value_sparse,
                        &coord_a_sparse,
                        &coord_b_sparse,
                        dense_index,
                        dense_coord_a.len(),
                    );
                    if let ValueColumnBuilder::Float(inner) = builder {
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

    let finished_value_columns: HashMap<String, ArrayRef> = config
        .value_columns
        .iter()
        .cloned()
        .zip(value_builders.into_iter().map(|(_, builder)| finish_value_column_builder(builder)))
        .collect();

    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let batch_schema = batch.schema();
    for index in 0..batch.num_columns() {
        let field = batch_schema.field(index);
        let column_name = field.name();
        if let Some(restored_array) = finished_value_columns.get(column_name.as_str()) {
            output_columns.push(restored_array.clone());
        } else if column_name == &config.coord_columns[0] {
            output_columns.push(Arc::new(coord_a_builder.finish()) as ArrayRef);
        } else if column_name == &config.coord_columns[1] {
            output_columns.push(Arc::new(coord_b_builder.finish()) as ArrayRef);
        } else {
            output_columns.push(batch.column(index).clone());
        }
    }

    RecordBatch::try_new(output_schema, output_columns).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to build restored record batch for {input_path}: {err}"
        ))
    })
}

pub fn restore_parquet_to_parquet_impl(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    print_timing: bool,
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
    let builder = ParquetRecordBatchReaderBuilder::try_new(input_file).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to initialize parquet reader {input_parquet_path}: {err}"
        ))
    })?;
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
    let mut writer = ArrowWriter::try_new(output_file, output_schema.clone(), None).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to initialize parquet writer {output_parquet_path}: {err}"
        ))
    })?;

    let restore_started = Instant::now();
    let mut rows_written = 0usize;
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
        )?;
        rows_written += restored.num_rows();
        writer.write(&restored).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to write restored parquet batch {output_parquet_path}: {err}"
            ))
        })?;
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

    let mut stats = HashMap::new();
    stats.insert("rows_written".to_string(), rows_written as f64);
    stats.insert("reference_load_sec".to_string(), reference_load_sec);
    stats.insert("restore_sec".to_string(), restore_sec);
    stats.insert("parquet_write_sec".to_string(), parquet_write_sec);
    stats.insert("total_sec".to_string(), total_sec);
    Ok(stats)
}
