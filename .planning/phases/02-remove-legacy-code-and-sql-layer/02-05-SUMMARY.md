---
phase: 02-remove-legacy-code-and-sql-layer
plan: "05"
subsystem: dependency-management
tags: [dependencies, pyproject, uv-lock, cleanup]
dependency_graph:
  requires: [02-04]
  provides: [trimmed-dependency-graph]
  affects: [pyproject.toml, uv.lock]
tech_stack:
  added: []
  patterns: [stdlib-csv-parsing]
key_files:
  created: []
  modified:
    - pyproject.toml
    - uv.lock
    - pylegend/extensions/tds/abstract/csv_tds_frame.py
    - tests/core/request/test_auth.py
    - tests/core/tds/abstract/test_csv_tds_frame.py
    - tests/test_legendql_api_tds_client.py
decisions:
  - Replaced pandas CSV parsing in csv_tds_frame.py with stdlib csv+datetime — no new dependency needed
  - Replaced mockito mocking in test_auth.py with stdlib unittest.mock
  - Rewrote test_legendql_api_tds_client.py to test pure query generation (execute_frame_to_pandas_df was removed)
metrics:
  duration: ~15m
  completed: "2026-06-01"
  tasks_completed: 2
  tasks_total: 2
---

# Phase 02 Plan 05: Trim pyproject.toml Dependencies Summary

Trimmed `pyproject.toml` runtime dependencies to exactly `requests>=2.27.1` and `ijson>=3.1.4`. Removed `pandas`, `numpy`, and `testcontainers` from runtime; dropped `pandas-stubs`, `mockito`, `sqlalchemy`, `pg8000`, `pymysql`, `cryptography`, and `wrapt` from dev. Added `testcontainers>=3.0.0` to dev group. Re-locked with `uv sync`, fixed residual pandas/mockito imports in retained files, and verified non-engine tests pass.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Trim pyproject.toml dependencies | 15eb0df | pyproject.toml |
| 2 | Re-lock with uv sync and fix residual imports | f041179 | uv.lock, csv_tds_frame.py, test_auth.py, test_csv_tds_frame.py, test_legendql_api_tds_client.py |

## Verification Results

- `pyproject.toml` runtime deps: `requests>=2.27.1`, `ijson>=3.1.4` only (verified via tomllib check)
- `uv.lock` no longer contains pandas, numpy, sqlalchemy, pg8000, pymysql, cryptography, mockito, pandas-stubs
- `wrapt` appears in `uv.lock` as a transitive dependency of `testcontainers` (expected — not a direct dep)
- `python -c "import pylegend"` succeeds
- All unit tests (auth, tds_column, csv_tds_frame, result_handler) pass (25 tests)
- Engine-gated tests (requiring JAVA_HOME or Docker) produce fixture errors — pre-existing behavior, not introduced by this plan

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] csv_tds_frame.py used pandas for CSV type inference**
- **Found during:** Task 2 (uv sync + test run)
- **Issue:** `pylegend/extensions/tds/abstract/csv_tds_frame.py` imported `pandas` for CSV parsing and column type detection via `pd.read_csv()` and `pd.api.types`. With pandas removed from runtime, this caused `ModuleNotFoundError: No module named 'pandas'` at import time.
- **Fix:** Rewrote `tds_columns_from_csv_string()` using stdlib `csv` and `datetime.strptime()` only. The type inference logic was replaced with a stdlib-based approach (boolean check via `True/False` string matching, integer via `int()`, float via `float()`, date via `datetime.strptime()` with multiple format tries, string as fallback). Removed `is_strict_date_or_datetime(col: pd.Series)` function entirely.
- **Files modified:** `pylegend/extensions/tds/abstract/csv_tds_frame.py`
- **Commit:** f041179

**2. [Rule 3 - Blocking] test_csv_tds_frame.py expected pd.errors.EmptyDataError**
- **Found during:** Task 2 (test run)
- **Issue:** `tests/core/tds/abstract/test_csv_tds_frame.py` expected `pd.errors.EmptyDataError` from `tds_columns_from_csv_string("")`, but the rewritten stdlib version raises `ValueError`.
- **Fix:** Updated test to expect `ValueError` with same message "No columns to parse from file".
- **Files modified:** `tests/core/tds/abstract/test_csv_tds_frame.py`
- **Commit:** f041179

**3. [Rule 3 - Blocking] test_auth.py used mockito for Session mocking**
- **Found during:** Task 2 (test run)
- **Issue:** `tests/core/request/test_auth.py` imported `mockito` at module level (line 15), causing `ModuleNotFoundError: No module named 'mockito'` when mockito was removed from dev deps.
- **Fix:** Rewrote all test methods to use `unittest.mock.patch('requests.Session', return_value=TestHeaderCopySession())` context managers instead of `mockito.when(requests).Session().thenReturn(...)` / `mockito.unstub()` pattern. Removed `setup_method` / `teardown_method` class fixtures (no longer needed with context manager approach).
- **Files modified:** `tests/core/request/test_auth.py`
- **Commit:** f041179

**4. [Rule 3 - Blocking] test_legendql_api_tds_client.py used pandas for engine integration test**
- **Found during:** Task 2 (test run)
- **Issue:** `tests/test_legendql_api_tds_client.py` imported `pandas` at module level and called `execute_frame_to_pandas_df()` which was removed in prior plans. The pandas import caused collection failure.
- **Fix:** Rewrote test to call `to_pure_query()` instead, verifying the LegendQL client correctly constructs Pure queries for service frames, filter, and group_by operations. The test still requires a live Legend engine via `legend_test_server` fixture.
- **Files modified:** `tests/test_legendql_api_tds_client.py`
- **Commit:** f041179

## Known Stubs

None — no stub patterns introduced by this plan.

## Threat Flags

None — this plan removes dependencies only and fixes tests; no new security-relevant surface introduced.

## Self-Check

- pyproject.toml exists: FOUND
- uv.lock exists: FOUND
- csv_tds_frame.py exists: FOUND
- Commit 15eb0df exists: FOUND
- Commit f041179 exists: FOUND

## Self-Check: PASSED
