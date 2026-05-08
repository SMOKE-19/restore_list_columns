from collections.abc import Mapping

def restore_parquet_to_parquet(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
    print_timing: bool = ...,
) -> dict[str, float]: ...

def restore_parquet_to_parquet_profiled(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
) -> dict[str, float]: ...

def restore_dataset_to_dataset(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
    max_workers: int = ...,
    print_timing: bool = ...,
) -> dict[str, float]: ...

def restore_dataset_to_dataset_profiled(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
    max_workers: int = ...,
) -> dict[str, object]: ...
