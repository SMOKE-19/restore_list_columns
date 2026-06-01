from pathlib import Path
from importlib.metadata import version

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

import restore_list_columns_rs


def test_python_package_version_matches_distribution_metadata() -> None:
    assert restore_list_columns_rs.__version__ == version("restore_list_columns_rs")


def _write_coord_file(path: Path, source: Path) -> None:
    table = pa.table(
        {
            "source_file": [str(source), str(source)],
            "row_group_id": pa.array([0, 1], type=pa.int32()),
            "row_index": pa.array([1, 3], type=pa.int64()),
            "row_offset_in_group": pa.array([1, 1], type=pa.int32()),
            "planner_chunk_id": pa.array([0, 0], type=pa.int32()),
        }
    )
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)


def test_restore_parquet_accepts_large_binary_json_columns(tmp_path: Path) -> None:
    source = tmp_path / "source_large_binary.parquet"
    lookup = tmp_path / "lookup.parquet"
    output = tmp_path / "restored.parquet"

    table = pa.table(
        {
            "group": pa.array(["A", "B"], type=pa.string()),
            "value": pa.array([b"[1.5, 2.5]", b"[3.5]"], type=pa.large_binary()),
            "coord_a": pa.array([b"[0, 0]", b"[1]"], type=pa.large_binary()),
            "coord_b": pa.array([b"[0, 1]", b"[0]"], type=pa.large_binary()),
        }
    )
    pq.write_table(table, source)
    pl.DataFrame(
        {
            "group": ["A", "A", "B"],
            "ord": [0, 1, 0],
            "coord_a": [0, 0, 1],
            "coord_b": [0, 1, 0],
        }
    ).write_parquet(lookup)

    restore_list_columns_rs.restore_parquet_to_parquet(
        str(source),
        str(output),
        str(lookup),
        {"group": "TEXT", "value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "coord_columns": ["coord_a", "coord_b"],
        },
        batch_size=16,
    )

    restored = pl.read_parquet(output).sort("group")
    assert restored.get_column("value").to_list() == [[1.5, 2.5], [3.5]]
    assert restored.get_column("coord_a").to_list() == [[0, 0], [1]]
    assert restored.get_column("coord_b").to_list() == [[0, 1], [0]]


def test_restore_parquet_restores_multiple_value_column_types(tmp_path: Path) -> None:
    source = tmp_path / "source_multi_value.parquet"
    lookup = tmp_path / "lookup.parquet"
    output = tmp_path / "restored.parquet"

    pl.DataFrame(
        {
            "group": ["A"],
            "value_float": ["[1.5, 3.5]"],
            "value_int": ["[10, 30]"],
            "value_text": ['["hot", "cold"]'],
            "coord_a": ["[0, 1]"],
            "coord_b": ["[0, 0]"],
        }
    ).write_parquet(source)
    pl.DataFrame(
        {
            "group": ["A", "A", "A"],
            "ord": [0, 1, 2],
            "coord_a": [0, 1, 2],
            "coord_b": [0, 0, 0],
        }
    ).write_parquet(lookup)

    restore_list_columns_rs.restore_parquet_to_parquet(
        str(source),
        str(output),
        str(lookup),
        {
            "value_float": "DOUBLE[]",
            "value_int": "INTEGER[]",
            "value_text": "TEXT[]",
            "coord_a": "INTEGER[]",
            "coord_b": "INTEGER[]",
        },
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value_float", "value_int", "value_text"],
            "coord_columns": ["coord_a", "coord_b"],
        },
    )

    restored = pl.read_parquet(output)
    assert restored.get_column("value_float").to_list() == [[1.5, 3.5, None]]
    assert restored.get_column("value_int").to_list() == [[10, 30, None]]
    assert restored.get_column("value_text").to_list() == [["hot", "cold", None]]


def test_restore_with_coord_file_selects_only_coord_rows(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5, 3.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0, 1]", "[0]", "[1]", "[1]"],
            "coord_b": ["[0, 0]", "[1]", "[1]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A", "A", "A"],
            "ord": [0, 1, 2, 3],
            "coord_a": [0, 0, 1, 1],
            "coord_b": [0, 1, 0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    stats = restore_list_columns_rs.restore_with_coord_file(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "coord_columns": ["coord_a", "coord_b"],
        },
        {"output_file_name": "part-test.parquet"},
        batch_size=16,
    )

    restored = pl.read_parquet(output_dir / "part-test.parquet").sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert restored.get_column("value").to_list() == [
        [None, 2.5, None, None],
        [None, None, 4.5, None],
    ]
    assert stats["rows_written"] == 2.0
    assert stats["source_file_count"] == 1.0
    assert stats["row_group_count"] == 2.0


def test_restore_with_coord_file_can_partition_output(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "partitioned"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "bucket": ["drop", "B", "drop", "C"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5, 3.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0, 1]", "[0]", "[1]", "[1]"],
            "coord_b": ["[0, 0]", "[1]", "[1]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A", "A", "A"],
            "ord": [0, 1, 2, 3],
            "coord_a": [0, 0, 1, 1],
            "coord_b": [0, 1, 0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    stats = restore_list_columns_rs.restore_with_coord_file(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "coord_columns": ["coord_a", "coord_b"],
        },
        {"partition_columns": ["bucket"], "file_name_prefix": "part-test"},
    )

    assert (output_dir / "bucket=B" / "part-test-00000.parquet").exists()
    assert (output_dir / "bucket=C" / "part-test-00000.parquet").exists()
    restored = pl.concat(
        [
            pl.read_parquet(output_dir / "bucket=B" / "part-test-00000.parquet"),
            pl.read_parquet(output_dir / "bucket=C" / "part-test-00000.parquet"),
        ]
    ).sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert stats["rows_written"] == 2.0
    assert stats["output_file_count"] == 2.0


def test_restore_with_coord_file_sanitizes_partition_values_and_nulls(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "partitioned"

    table = pa.table(
        {
            "id": pa.array(["r0", "r1", "r2", "r3"]),
            "bucket": pa.array(["drop", "B/1:bad", "drop", None], type=pa.string()),
            "group": pa.array(["A", "A", "A", "A"]),
            "value": pa.array(["[1.5]", "[2.5]", "[9.5]", "[4.5]"]),
            "coord_a": pa.array(["[0]", "[0]", "[0]", "[0]"]),
            "coord_b": pa.array(["[0]", "[1]", "[0]", "[0]"]),
        }
    )
    pq.write_table(table, source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A"],
            "ord": [0, 1],
            "coord_a": [0, 0],
            "coord_b": [0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    restore_list_columns_rs.restore_with_coord_file(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "coord_columns": ["coord_a", "coord_b"],
        },
        {"partition_columns": ["bucket"], "file_name_prefix": "part-test"},
    )

    assert (output_dir / "bucket=B_1_bad" / "part-test-00000.parquet").exists()
    assert (output_dir / "bucket=__null__" / "part-test-00000.parquet").exists()


def test_restore_with_coord_file_applies_reference_replace(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    replace_ref = tmp_path / "replace_ref.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "status": ["old", "old", "keep", "keep"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5, 3.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0, 1]", "[0]", "[1]", "[1]"],
            "coord_b": ["[0, 0]", "[1]", "[1]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A", "A", "A"],
            "ord": [0, 1, 2, 3],
            "coord_a": [0, 0, 1, 1],
            "coord_b": [0, 1, 0, 1],
        }
    ).write_parquet(lookup)
    pl.DataFrame({"from_status": ["old"], "to_status": ["new"]}).write_parquet(replace_ref)
    _write_coord_file(coord, source)

    restore_list_columns_rs.restore_with_coord_file(
        str(coord),
        str(output_dir),
        str(lookup),
        {"status": "TEXT", "value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "coord_columns": ["coord_a", "coord_b"],
        },
        {
            "output_file_name": "part-test.parquet",
            "reference_replace": {
                "reference_parquet": str(replace_ref),
                "source_column": "status",
                "reference_input_column": "from_status",
                "reference_output_column": "to_status",
            },
        },
    )

    restored = pl.read_parquet(output_dir / "part-test.parquet").sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert restored.get_column("status").to_list() == ["new", "keep"]


def test_restore_with_coord_file_does_not_publish_partial_parquet_on_error(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "id": ["r0", "r1"],
            "group": ["A", "B"],
            "value": ["[1.5]", "[2.5]"],
            "coord_a": ["[0]", "[0]"],
            "coord_b": ["[0]", "[0]"],
        }
    ).write_parquet(source, row_group_size=1)
    pl.DataFrame(
        {
            "group": ["A"],
            "ord": [0],
            "coord_a": [0],
            "coord_b": [0],
        }
    ).write_parquet(lookup)
    table = pa.table(
        {
            "source_file": [str(source), str(source)],
            "row_group_id": pa.array([0, 1], type=pa.int32()),
            "row_index": pa.array([0, 1], type=pa.int64()),
            "row_offset_in_group": pa.array([0, 0], type=pa.int32()),
            "planner_chunk_id": pa.array([0, 0], type=pa.int32()),
        }
    )
    with ipc.new_file(coord, table.schema) as writer:
        writer.write_table(table)

    try:
        restore_list_columns_rs.restore_with_coord_file(
            str(coord),
            str(output_dir),
            str(lookup),
            {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
            {
                "key_column": "group",
                "order_column": "ord",
                "value_columns": ["value"],
                "coord_columns": ["coord_a", "coord_b"],
            },
            {"output_file_name": "part-test.parquet"},
        )
    except ValueError as exc:
        assert "unknown lookup key" in str(exc)
    else:
        raise AssertionError("restore_with_coord_file should fail on missing lookup key")

    assert not list(output_dir.rglob("*.parquet"))
    assert all(path.stat().st_size > 0 for path in output_dir.rglob("*") if path.is_file())


def test_restore_with_coord_file_replaces_existing_output_and_cleans_temp(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    final_output = output_dir / "part-test.parquet"
    final_output.write_bytes(b"stale")

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0]", "[0]", "[0]", "[0]"],
            "coord_b": ["[0]", "[1]", "[0]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A"],
            "ord": [0, 1],
            "coord_a": [0, 0],
            "coord_b": [0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    restore_list_columns_rs.restore_with_coord_file(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "coord_columns": ["coord_a", "coord_b"],
        },
        {"output_file_name": final_output.name},
    )

    restored = pl.read_parquet(final_output).sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert not list(output_dir.glob("*.tmp"))


def test_restore_with_coord_file_rejects_invalid_binary_json_without_output(tmp_path: Path) -> None:
    source = tmp_path / "source_invalid_binary.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    table = pa.table(
        {
            "id": pa.array(["r0"]),
            "group": pa.array(["A"]),
            "value": pa.array([b"\xff\xfe"], type=pa.binary()),
            "coord_a": pa.array([b"[0]"], type=pa.binary()),
            "coord_b": pa.array([b"[0]"], type=pa.binary()),
        }
    )
    pq.write_table(table, source)
    pl.DataFrame({"group": ["A"], "ord": [0], "coord_a": [0], "coord_b": [0]}).write_parquet(lookup)
    coord_table = pa.table(
        {
            "source_file": [str(source)],
            "row_group_id": pa.array([0], type=pa.int32()),
            "row_index": pa.array([0], type=pa.int64()),
            "row_offset_in_group": pa.array([0], type=pa.int32()),
            "planner_chunk_id": pa.array([0], type=pa.int32()),
        }
    )
    with ipc.new_file(coord, coord_table.schema) as writer:
        writer.write_table(coord_table)

    try:
        restore_list_columns_rs.restore_with_coord_file(
            str(coord),
            str(output_dir),
            str(lookup),
            {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
            {
                "key_column": "group",
                "order_column": "ord",
                "value_columns": ["value"],
                "coord_columns": ["coord_a", "coord_b"],
            },
            {"output_file_name": "part-test.parquet"},
        )
    except ValueError as exc:
        assert "invalid utf-8 bytes" in str(exc)
    else:
        raise AssertionError("restore_with_coord_file should fail on invalid binary json")

    assert not list(output_dir.rglob("*.parquet"))


def test_plan_restore_coords_writes_row_count_chunks(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    coord_dir = tmp_path / "coords"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3", "r4"],
            "group": ["A", "A", "A", "A", "A"],
        }
    ).write_parquet(source, row_group_size=2)

    stats = restore_list_columns_rs.plan_restore_coords(
        [str(source)],
        str(coord_dir),
        planner_config={"row_count": 2},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    assert [path.name for path in coord_files] == [
        "chunk-000000.arrow",
        "chunk-000001.arrow",
        "chunk-000002.arrow",
    ]
    assert stats["input_file_count"] == 1.0
    assert stats["input_row_group_count"] >= 1.0
    assert stats["selected_row_count"] == 5.0
    assert stats["coord_chunk_count"] == 3.0

    first = pl.read_ipc(coord_files[0])
    assert first.get_column("row_index").to_list() == [0, 1]
    assert first.get_column("row_offset_in_group").to_list() == [0, 1]


def test_plan_restore_coords_applies_thin_filters_and_dedupe(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    coord_dir = tmp_path / "coords"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3", "r4"],
            "biz": ["keep", "drop", None, "keep", "keep"],
            "group_key": ["A", "A", "B", "A", "B"],
            "updated_at": ["2025-01-01T00:00:00", "2025-01-03T00:00:00", "2025-01-04T00:00:00", "2025-01-02T00:00:00", "2025-01-05T00:00:00"],
        }
    ).write_parquet(source, row_group_size=2)

    stats = restore_list_columns_rs.plan_restore_coords(
        [str(source)],
        str(coord_dir),
        filter_config={
            "source_filters": [
                {"column": "biz", "op": "is_not_null"},
                {"column": "biz", "op": "ne", "value": "drop"},
            ],
            "dedupe": {
                "enabled": True,
                "group_keys": ["group_key"],
                "sort": [{"column": "updated_at", "direction": "desc"}],
            },
        },
        planner_config={"row_count": 10},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    assert len(coord_files) == 1
    coords = pl.read_ipc(coord_files[0]).sort("row_index")
    assert coords.get_column("row_index").to_list() == [3, 4]
    assert stats["selected_row_count"] == 2.0


def test_plan_restore_coords_dedupe_supports_timestamp_nanosecond_sort(tmp_path: Path) -> None:
    source = tmp_path / "source_timestamp_ns.parquet"
    coord_dir = tmp_path / "coords"

    table = pa.table(
        {
            "id": pa.array(["old_a", "new_a", "old_b", "new_b"]),
            "group_key": pa.array(["A", "A", "B", "B"]),
            "updated_at": pa.array(
                [
                    1_700_000_000_000_000_001,
                    1_700_000_000_000_000_010,
                    1_700_000_000_000_000_003,
                    1_700_000_000_000_000_020,
                ],
                type=pa.timestamp("ns"),
            ),
        }
    )
    pq.write_table(table, source, row_group_size=2)

    stats = restore_list_columns_rs.plan_restore_coords(
        [str(source)],
        str(coord_dir),
        filter_config={
            "dedupe": {
                "enabled": True,
                "group_keys": ["group_key"],
                "sort": [{"column": "updated_at", "direction": "desc"}],
            },
        },
        planner_config={"row_count": 10},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    assert len(coord_files) == 1
    coords = pl.read_ipc(coord_files[0]).sort("row_index")
    assert coords.get_column("row_index").to_list() == [1, 3]
    assert stats["selected_row_count"] == 2.0


def test_plan_restore_coords_supports_in_like_and_or_filters(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    coord_dir = tmp_path / "coords"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3", "r4"],
            "biz": ["A100", "A200", "B100", "C100", None],
            "status": ["keep", "drop", "hold", "keep", "keep"],
        }
    ).write_parquet(source, row_group_size=2)

    stats = restore_list_columns_rs.plan_restore_coords(
        [str(source)],
        str(coord_dir),
        filter_config={
            "source_filters": [
                {
                    "op": "and",
                    "filters": [
                        {"column": "biz", "op": "like", "value": "A%"},
                        {
                            "op": "or",
                            "filters": [
                                {"column": "status", "op": "in", "values": ["keep", "hold"]},
                                {"column": "biz", "op": "eq", "value": "C100"},
                            ],
                        },
                    ],
                }
            ],
        },
        planner_config={"row_count": 10},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    coords = pl.read_ipc(coord_files[0]).sort("row_index")
    assert coords.get_column("row_index").to_list() == [0]
    assert stats["selected_row_count"] == 1.0
