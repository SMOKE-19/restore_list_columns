use pyo3::prelude::*;
use std::collections::HashMap;

mod converter;
mod parser;
mod reference;

#[pyfunction]
#[pyo3(
    text_signature = "(input_parquet_path, output_parquet_path, lookup_path, schema, config_json, print_timing=False)"
)]
fn restore_parquet_to_parquet(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    print_timing: bool,
) -> PyResult<HashMap<String, f64>> {
    converter::restore_parquet_to_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        print_timing,
    )
}

#[pymodule]
fn restore_list_columns_rs(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "__doc__",
        "Rust module for restoring sparse list columns from parquet into parquet",
    )?;
    module.add_function(wrap_pyfunction!(restore_parquet_to_parquet, module)?)?;
    Ok(())
}
