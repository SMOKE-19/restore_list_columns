use pyo3::prelude::*;
use std::collections::HashMap;

mod converter;
mod parser;
mod reference;

#[pyfunction]
#[pyo3(
    text_signature = "(input_parquet_path, output_parquet_path, lookup_path, schema, config_json, batch_size=None, drop_cache_hint=False, print_timing=False)"
)]
fn restore_parquet_to_parquet(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> PyResult<HashMap<String, f64>> {
    converter::restore_parquet_to_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
    )
}

#[pyfunction]
#[pyo3(
    text_signature = "(input_parquet_path, output_parquet_path, lookup_path, schema, config_json, batch_size=None, drop_cache_hint=False)"
)]
fn restore_parquet_to_parquet_profiled(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
) -> PyResult<HashMap<String, f64>> {
    converter::restore_parquet_to_parquet_profiled_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
    )
}

#[pymodule]
fn restore_list_columns_rs(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "__doc__",
        "Rust module for restoring sparse list columns from parquet into parquet",
    )?;
    module.add_function(wrap_pyfunction!(restore_parquet_to_parquet, module)?)?;
    module.add_function(wrap_pyfunction!(restore_parquet_to_parquet_profiled, module)?)?;
    Ok(())
}
