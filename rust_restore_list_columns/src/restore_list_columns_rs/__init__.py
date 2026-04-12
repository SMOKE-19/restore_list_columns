"""Parquet dataset list-column restore wrapper."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from .restore_list_columns_rs import restore_parquet_to_parquet as _restore_parquet_to_parquet_impl

__all__ = [
    "__version__",
    "restore_dataset_to_dataset",
    "restore_parquet_to_parquet",
]

__version__ = "0.1.0"


def restore_parquet_to_parquet(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    print_timing: bool = False,
) -> dict[str, float]:
    return _restore_parquet_to_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        json.dumps(config, ensure_ascii=True),
        print_timing,
    )


def restore_dataset_to_dataset(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
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
    for input_path in part_files:
        output_path = output_dir / input_path.name
        stats = restore_parquet_to_parquet(
            str(input_path),
            str(output_path),
            lookup_path,
            schema,
            config,
            print_timing=print_timing,
        )
        totals["files_written"] += 1.0
        for key, value in stats.items():
            totals[key] = totals.get(key, 0.0) + value
    return totals
