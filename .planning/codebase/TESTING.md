# Testing Patterns

**Analysis Date:** 2026-05-31

## Test Framework

- **Runner:** pytest (7.0.0–9.0.0 for Python <3.11; 7.0.0+ for Python >=3.11)
- **Coverage:** pytest-cov; reports uploaded to CodeCov per OS/Python matrix
- **Config:** `.github/workflows/actions/pytest/action.yml`

**Run Commands:**
```bash
pytest ./tests --cov=pylegend --cov=tests -o log_cli=true
pytest ./tests
pytest -k test_name  # Run specific test
```

## Test File Organization

Tests mirror source structure: `tests/` parallels `pylegend/`.

```
tests/
├── conftest.py                     # Session fixtures (Legend test server)
├── test_legacy_api_tds_client.py
├── core/
│   ├── test_project_coorindates.py
│   ├── language/
│   │   └── test_literal_expressions.py
│   ├── tds/
│   │   ├── test_tds_column.py
│   │   └── test_tds_frame_cast.py
│   └── database/
│       ├── test_sql_gen_e2e.py
│       └── test_sql_to_string.py
├── extensions/
└── utils/
    └── test_class_utils.py
```

**Naming:**
- Files: `test_*.py`
- Classes: `Test*` (e.g., `TestTdsColumn`, `TestLiteralExpressions`)
- Methods: `test_*()` (e.g., `test_primitive_tds_column_creation`)

## Test Structure

```python
class TestTdsColumn:
    def test_primitive_tds_column_creation(self) -> None:
        c1 = PrimitiveTdsColumn('C1', PrimitiveType.Integer)
        assert "TdsColumn(Name: C1, Type: Integer)" == str(c1)
```

- Type hints on test methods: `-> None`
- Fixture injection via method parameters with type hints

## Fixtures

**Session-Level (in `tests/conftest.py`):**
- `legend_test_server` (scope="session") — starts Java-based Legend SQL Server; yields `PyLegendDict` with engine/metadata ports; retry logic with 15 attempts, 4-second intervals

**Autouse:**
- `init_legend` (autouse=True) — initializes Legend client per test

**Usage:**
```python
def test_legacy_api_tds_client(
    self,
    legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
) -> None:
    port = legend_test_server["engine_port"]
```

## Mocking

- **Framework:** `mockito` (dev dependency)
- Minimal usage; preference is integration tests against real services
- HTTP calls to Legend Server use live integration tests (no mocking)
- Database operations use testcontainers for real containers

## Test Data

- Metadata JSON files: `tests/resources/legend/*.json`
- Legend SQL Server JAR: `tests/resources/legend/server/`

## Test Types

**Unit Tests:**
- Scope: Individual classes and methods, no external dependencies
- Example: `TestTdsColumn.test_primitive_tds_column_creation()`

**Integration Tests:**
- Scope: Multi-component interaction with live Legend Server
- Example: `TestLegacyApiTdsClient` (uses `legend_test_server` fixture)

**E2E Tests:**
- Full query execution with real database backends (PostgreSQL/MySQL via testcontainers)
- Example: `tests/core/database/test_sql_gen_e2e.py`

## Common Patterns

**Error testing:**
```python
with pytest.raises(RuntimeError) as r:
    tds_columns_from_json(" --- ")
assert "Unable to parse tds columns from schema" in r.value.args[0]
```

**DataFrame comparison:**
```python
df = frame.execute_frame_to_pandas_df()
expected = pd.DataFrame(columns=["Age"], data=[[23]]).astype({"Age": "Int64"})
pd.testing.assert_frame_equal(expected, df)
```

## CI/CD Pipeline

**Workflow:** `.github/workflows/build-ci.yml`

**Matrix:** Python 3.9–3.14, Ubuntu + Windows

**Steps:**
1. Lint (flake8) + type check (mypy)
2. PyTest with coverage
3. Build (uv build)
4. Docker build + push
5. Sphinx docs

**Requirements:** Java JDK 11 (Legend Server), Maven (builds JAR), Docker (testcontainers)
