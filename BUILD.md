# BUILD

## 전제 조건

- Python 3.10+
- Rust toolchain
- `maturin`

## 가상환경 준비

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip maturin
```

## 개발 설치

프로젝트 루트가 아니라 Rust 패키지 디렉터리에서 `maturin develop`을 실행합니다.

```bash
cd rust_restore_list_columns
maturin develop
```

설치가 끝나면 Python에서 `restore_list_columns_rs` import가 가능해야 합니다.

## 빠른 확인

```bash
python - <<'PY'
import restore_list_columns_rs
print(restore_list_columns_rs.__version__)
PY
```

## 메모

- 이 프로젝트는 mixed Python/Rust 패키지입니다.
- wheel 산출물은 `maturin`이 임시 디렉터리에 만들고 editable 설치를 수행합니다.
- `target/` 디렉터리는 빌드 산출물이라 Git에는 포함하지 않습니다.
