# restore_list_columns

Parquet dataset 안에 문자열로 저장된 리스트 칼럼을 Rust 기반으로 복원해 다시 parquet로 저장하는 패키지입니다.

현재 구현 범위:

- 입력: parquet file 또는 parquet dataset 디렉터리
- 출력: parquet file 또는 입력 part와 1:1 대응하는 output dataset 디렉터리
- 복원 대상:
  - `INTEGER[]`
  - `DOUBLE[]`
- 입력 리스트 문자열 규약:
  - `INTEGER[]`: `[1,2,3]`
  - `TEXT[]`: `["N","Y","Y"]`
  - `DOUBLE[]`: `[3.14,2.333,9.9999]` 또는 `["3.14","2.333","9.9999"]`

`TEXT[]` 전용 parser도 포함되어 있지만, 현재 핵심 restore 흐름은 sparse numeric list 복원에 맞춰져 있습니다.

## 구조

```text
restore_list_columns/
  README.md
  USAGE.md
  BUILD.md
  .gitignore
  rust_restore_list_columns/
    Cargo.toml
    pyproject.toml
    src/
      lib.rs
      converter.rs
      reference.rs
      parser.rs
      restore_list_columns_rs/
```

## 핵심 동작

1. 입력 parquet batch를 읽습니다.
2. sparse list 문자열 컬럼을 YAML/호출자가 넘긴 schema 기준으로 바로 파싱합니다.
3. lookup parquet를 읽어 dense 좌표 기준을 만듭니다.
4. row별로 sparse 값을 dense list로 복원합니다.
5. 복원된 batch를 parquet로 다시 기록합니다.

dataset 입력일 때는 각 `*.parquet` part를 같은 파일명으로 output dataset에 1:1로 저장합니다.

## 공개 API

대표 Python entrypoint는 두 개입니다.

- `restore_list_columns_rs.restore_parquet_to_parquet`
- `restore_list_columns_rs.restore_dataset_to_dataset`

자세한 호출 예시는 [USAGE.md](USAGE.md), 빌드 절차는 [BUILD.md](BUILD.md)를 보면 됩니다.
