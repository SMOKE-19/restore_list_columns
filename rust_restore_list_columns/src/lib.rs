use pyo3::prelude::*;
use std::collections::HashMap;

mod converter;
mod coord;
mod parser;
mod planner;
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

#[pyfunction]
#[pyo3(
    text_signature = "(coord_path, output_dir, lookup_path, schema, config_json, writer_config_json=None, batch_size=None, drop_cache_hint=False, print_timing=False)"
)]
fn restore_with_coord_file(
    coord_path: String,
    output_dir: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    writer_config_json: Option<String>,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> PyResult<HashMap<String, f64>> {
    converter::restore_with_coord_file_impl(
        coord_path,
        output_dir,
        lookup_path,
        schema,
        config_json,
        writer_config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
    )
}

#[pyfunction]
#[pyo3(
    text_signature = "(input_parquet_paths, coord_output_dir, filter_config_json=None, planner_config_json=None)"
)]
fn plan_restore_coords(
    input_parquet_paths: Vec<String>,
    coord_output_dir: String,
    filter_config_json: Option<String>,
    planner_config_json: Option<String>,
) -> PyResult<HashMap<String, f64>> {
    planner::plan_restore_coords_impl(
        input_parquet_paths,
        coord_output_dir,
        filter_config_json,
        planner_config_json,
    )
}

#[pymodule]
fn restore_list_columns_rs(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "__doc__",
        "Rust module for restoring sparse list columns from parquet into parquet",
    )?;
    module.add_function(wrap_pyfunction!(restore_parquet_to_parquet, module)?)?;
    module.add_function(wrap_pyfunction!(
        restore_parquet_to_parquet_profiled,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(restore_with_coord_file, module)?)?;
    module.add_function(wrap_pyfunction!(plan_restore_coords, module)?)?;
    Ok(())
}
