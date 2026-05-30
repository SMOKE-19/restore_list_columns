"""Parquet dataset list-column restore wrapper."""

from __future__ import annotations

import concurrent.futures
import json
from pathlib import Path
from typing import Mapping

from .restore_list_columns_rs import (
    plan_restore_coords as _plan_restore_coords_impl,
    restore_parquet_to_parquet as _restore_parquet_to_parquet_impl,
    restore_parquet_to_parquet_profiled as _restore_parquet_to_parquet_profiled_impl,
    restore_with_coord_file as _restore_with_coord_file_impl,
)

__all__ = [
    "__version__",
    "plan_restore_coords",
    "restore_dataset_to_dataset",
    "restore_dataset_to_dataset_profiled",
    "restore_parquet_to_parquet",
    "restore_parquet_to_parquet_profiled",
    "restore_with_coord_file",
]

__version__ = "0.1.1"


def plan_restore_coords(
    input_parquet_paths: list[str],
    coord_output_dir: str,
    filter_config: Mapping[str, object] | None = None,
    planner_config: Mapping[str, object] | None = None,
) -> dict[str, float]:
    return _plan_restore_coords_impl(
        [str(item) for item in input_parquet_paths],
        coord_output_dir,
        json.dumps(dict(filter_config or {}), ensure_ascii=True),
        json.dumps(dict(planner_config or {}), ensure_ascii=True),
    )

def _restore_parquet_to_parquet_worker(args: tuple[str, str, str, dict[str, str], dict[str, object], int | None, bool]) -> dict[str, float]:
    input_parquet_path, output_parquet_path, lookup_path, schema, config, batch_size, drop_cache_hint, print_timing = args
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=print_timing,
        profiled=False,
    )


def _restore_parquet_to_parquet_profiled_worker(
    args: tuple[str, str, str, dict[str, str], dict[str, object], int | None, bool]
) -> dict[str, float]:
    input_parquet_path, output_parquet_path, lookup_path, schema, config, batch_size, drop_cache_hint = args
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=False,
        profiled=True,
    )


def _restore_parquet_impl(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    *,
    batch_size: int | None,
    drop_cache_hint: bool,
    print_timing: bool,
    profiled: bool,
) -> dict[str, float]:
    config_json = json.dumps(config, ensure_ascii=True)
    if profiled:
        return _restore_parquet_to_parquet_profiled_impl(
            input_parquet_path,
            output_parquet_path,
            lookup_path,
            schema,
            config_json,
            batch_size,
            drop_cache_hint,
        )
    return _restore_parquet_to_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
    )


def restore_parquet_to_parquet(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    drop_cache_hint: bool = False,
    print_timing: bool = False,
) -> dict[str, float]:
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=print_timing,
        profiled=False,
    )


def restore_with_coord_file(
    coord_path: str,
    output_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    writer_config: Mapping[str, object] | None = None,
    batch_size: int | None = None,
    drop_cache_hint: bool = False,
    print_timing: bool = False,
) -> dict[str, float]:
    return _restore_with_coord_file_impl(
        coord_path,
        output_dir,
        lookup_path,
        schema,
        json.dumps(config, ensure_ascii=True),
        json.dumps(dict(writer_config or {}), ensure_ascii=True),
        batch_size,
        drop_cache_hint,
        print_timing,
    )


def restore_dataset_to_dataset(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    max_workers: int = 1,
    drop_cache_hint: bool = False,
    print_timing: bool = False,
) -> dict[str, float]:
    input_dir = Path(input_dataset_dir)
    output_dir = Path(output_dataset_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    part_files = sorted(path for path in input_dir.glob("*.parquet") if path.is_file())
    if not part_files:
        raise ValueError(f"restore input dataset has no parquet files: {input_dir}")

    totals: dict[str, float] = {
        "files_written": 0.0,
        "rows_written": 0.0,
        "reference_load_sec": 0.0,
        "restore_sec": 0.0,
        "parquet_write_sec": 0.0,
        "total_sec": 0.0,
    }
    worker_args = [
        (
            str(input_path),
            str(output_dir / input_path.name),
            lookup_path,
            schema,
            dict(config),
            batch_size,
            drop_cache_hint,
            print_timing,
        )
        for input_path in part_files
    ]
    if max_workers <= 1:
        results = [_restore_parquet_to_parquet_worker(args) for args in worker_args]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(_restore_parquet_to_parquet_worker, worker_args))
    for stats in results:
        totals["files_written"] += 1.0
        for key, value in stats.items():
            totals[key] = totals.get(key, 0.0) + value
    return totals


def restore_parquet_to_parquet_profiled(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    drop_cache_hint: bool = False,
) -> dict[str, float]:
    return _restore_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config,
        batch_size=batch_size,
        drop_cache_hint=drop_cache_hint,
        print_timing=False,
        profiled=True,
    )


def restore_dataset_to_dataset_profiled(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = None,
    max_workers: int = 1,
    drop_cache_hint: bool = False,
) -> dict[str, object]:
    input_dir = Path(input_dataset_dir)
    output_dir = Path(output_dataset_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    part_files = sorted(path for path in input_dir.glob("*.parquet") if path.is_file())
    if not part_files:
        raise ValueError(f"restore input dataset has no parquet files: {input_dir}")

    summary: dict[str, float] = {
        "files_written": 0.0,
        "rows_written": 0.0,
        "reference_load_sec": 0.0,
        "restore_sec": 0.0,
        "parquet_write_sec": 0.0,
        "total_sec": 0.0,
        "batches_processed": 0.0,
        "input_rows": 0.0,
        "source_extract_sec": 0.0,
        "dense_restore_sec": 0.0,
        "record_batch_build_sec": 0.0,
        "writer_write_sec": 0.0,
        "cache_hint_sec": 0.0,
        "cache_hint_calls": 0.0,
    }
    avg_accumulators: dict[str, float] = {"avg_restored_batch_array_bytes": 0.0}
    max_keys = {
        "max_batch_rows",
        "max_dense_len",
        "value_column_count",
        "output_file_size_bytes",
        "max_restored_batch_array_bytes",
    }
    files: list[dict[str, object]] = []
    worker_args = [
        (
            str(input_path),
            str(output_dir / input_path.name),
            lookup_path,
            schema,
            dict(config),
            batch_size,
            drop_cache_hint,
        )
        for input_path in part_files
    ]
    if max_workers <= 1:
        results = [_restore_parquet_to_parquet_profiled_worker(args) for args in worker_args]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(_restore_parquet_to_parquet_profiled_worker, worker_args))
    for input_path, output_path, stats in zip(
        [Path(args[0]) for args in worker_args],
        [Path(args[1]) for args in worker_args],
        results,
        strict=False,
    ):
        files.append(
            {
                "input_path": str(input_path),
                "output_path": str(output_path),
                **stats,
            }
        )
        summary["files_written"] += 1.0
        batches_processed = float(stats.get("batches_processed", 0.0))
        for key, value in stats.items():
            numeric_value = float(value)
            if key in max_keys:
                summary[key] = max(summary.get(key, 0.0), numeric_value)
            elif key == "avg_restored_batch_array_bytes":
                avg_accumulators[key] += numeric_value * batches_processed
            else:
                summary[key] = summary.get(key, 0.0) + numeric_value
    total_batches = summary.get("batches_processed", 0.0)
    if total_batches > 0:
        summary["avg_restored_batch_array_bytes"] = avg_accumulators["avg_restored_batch_array_bytes"] / total_batches
    else:
        summary["avg_restored_batch_array_bytes"] = 0.0
    return {"summary": summary, "files": files}
