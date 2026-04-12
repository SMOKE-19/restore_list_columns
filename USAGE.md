# USAGE

## 설치 후 import

이 프로젝트는 `maturin develop` 또는 `pip install -e` 형태로 설치한 뒤 아래처럼 사용합니다.

```python
from restore_list_columns_rs import (
    restore_dataset_to_dataset,
    restore_parquet_to_parquet,
)
```

## 1. parquet file -> parquet file

```python
from restore_list_columns_rs import restore_parquet_to_parquet

stats = restore_parquet_to_parquet(
    input_parquet_path="input.parquet",
    output_parquet_path="output.parquet",
    lookup_path="lookup.parquet",
    schema={
        "group_key": "TEXT",
        "val": "DOUBLE[]",
        "id_x": "INTEGER[]",
        "id_y": "INTEGER[]",
    },
    config={
        "key_column": "group_key",
        "order_column": "order_idx",
        "value_column": "val",
        "coord_columns": ["id_x", "id_y"],
    },
    print_timing=False,
)
```

반환값은 timing/row 수 요약 dict 입니다.

## 2. dataset -> dataset

```python
from restore_list_columns_rs import restore_dataset_to_dataset

stats = restore_dataset_to_dataset(
    input_dataset_dir="input_dataset",
    output_dataset_dir="output_dataset",
    lookup_path="lookup.parquet",
    schema={
        "group_key": "TEXT",
        "val": "DOUBLE[]",
        "id_x": "INTEGER[]",
        "id_y": "INTEGER[]",
        "biz_date": "TEXT",
    },
    config={
        "key_column": "group_key",
        "order_column": "order_idx",
        "value_column": "val",
        "coord_columns": ["id_x", "id_y"],
    },
    print_timing=True,
)
```

이 경우:

- 입력 디렉터리 안 `*.parquet` 파일을 찾습니다.
- 각 파일을 같은 이름으로 output dataset 디렉터리에 저장합니다.
- 출력은 단일 파일이 아니라 dataset 디렉터리입니다.

## schema 규약

현재 구현에서 중요한 건 호출자가 목표 타입을 명시해 주는 것입니다.

- `INTEGER[]`
- `DOUBLE[]`
- 일반 pass-through scalar 타입
  - `TEXT`
  - `INTEGER`
  - `DOUBLE`
  - `DATE`
  - `TIMESTAMP`

리스트 타입은 내부 값으로 타입을 추론하지 않고, `schema` 기준으로 바로 해당 parser를 탑니다.

## 입력 리스트 문자열 규약

- `INTEGER[]`: `[1,2,3]`
- `DOUBLE[]`: `[3.14,2.333,9.9999]`
- `DOUBLE[]` quoted numeric도 허용: `["3.14","2.333","9.9999"]`
- `TEXT[]`: `["N","Y","Y"]`

## 주요 에러

- lookup key가 없으면 `unknown lookup key ...`
- coord column 개수가 2개가 아니면 `coord_columns must contain exactly 2 items`
- 입력 dataset에 parquet file이 없으면 `restore input dataset has no parquet files`
- 숫자 파싱이 안 되면 `invalid integer ...`, `invalid float ...`
