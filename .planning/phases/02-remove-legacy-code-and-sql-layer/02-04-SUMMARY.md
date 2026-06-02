---
phase: 02-remove-legacy-code-and-sql-layer
plan: "04"
subsystem: tests
tags: [test-cleanup, sql-removal, pure-assertions]
dependency_graph:
  requires: ["02-03"]
  provides: ["REMV-03-test-cleanup"]
  affects: ["full-test-suite-collection"]
tech_stack:
  added: []
  patterns: ["sql-to-pure-test-migration"]
key_files:
  modified:
    - tests/core/tds/legendql_api/frames/functions/ (17 files)
    - tests/core/request/test_legend_client.py
    - tests/core/request/test_legend_client_e2e.py
    - tests/core/language/legendql_api/test_legendql_api_tds_row.py
    - tests/core/language/shared/__init__.py
    - tests/core/language/shared/test_tds_row.py
    - tests/core/language/shared/primitives/ (10 files)
    - tests/core/language/test_literal_expressions.py
    - tests/core/tds/result_handler/ (3 files)
    - tests/core/tds/test_tds_frame_cast.py
    - tests/core/test_project_coorindates.py
    - tests/extensions/tds/abstract/ (2 files)
    - tests/extensions/tds/frames/legendql_api/ (3 files)
    - tests/extensions/tds/result_handler/test_to_pandas_df_result_handler.py
    - tests/test_helpers/test_legend_service_frames.py
decisions:
  - "Removed TestPrecisePrimitiveDirectInstantiation class entirely (SQL-only tests)"
  - "Replaced legacy/pandas frame factories in extension/result-handler tests with LegendQL API equivalents"
  - "Rewrote test_project_coorindates.py to test accessor methods instead of deleted sql_params()"
metrics:
  duration: "~2 hours"
  completed: "2026-06-02"
  tasks_completed: 2
  files_modified: 37
---

# Phase 02 Plan 04: Test SQL Cleanup Summary

**One-liner:** Removed all SQL assertions from 37 test files, replacing with Pure-only coverage; full pytest suite now collects 678 tests with zero errors.

## What Was Done

### Task 1: Clean LegendQL function tests and client tests

Cleaned SQL assertions from 17 LegendQL function test files under `tests/core/tds/legendql_api/frames/functions/`:
- Removed `from pylegend.core.tds.tds_frame import FrameToSqlConfig` imports
- Removed inline `assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)` assertions and preceding `expected = '''...'''` blocks
- Handled the window_extend function test which used `sql_expression` as a parametrize parameter (removed sql_expression from parametrize tuples and function signature)
- Kept all `generate_pure_query_and_compile(frame, FrameToPureConfig(), ...)` assertions

Cleaned `test_legend_client.py`: Removed the SQL mock server class with `get_sql_string_schema` tests; added a mock-based `execute_pure_string` test to satisfy the Pure coverage requirement.

Cleaned `test_legend_client_e2e.py`: Removed `test_e2e_schema_string_api` (get_sql_string_schema) and `test_e2e_execute_string_api` (execute_sql_string); kept Pure, parse_model, and compile_model tests.

Cleaned `test_legendql_api_tds_row.py`: Removed `get_frame_name_to_base_query_map` abstract method and SQL assertions; updated `AbstractTestTdsRow` in `shared/test_tds_row.py` to Pure-only assertions.

**Rule 3 auto-fixes (blocking issues):**
- Fixed `tests/core/language/shared/__init__.py` which imported `FrameToSqlConfig`, `R` (TypeVar), and `PandasDfReadConfig` (all deleted); also removed `to_sql_query`, `execute_frame`, `execute_frame_to_string`, and `execute_frame_to_pandas_df` methods from `TestTableSpecInputFrame`
- Fixed `tests/core/language/shared/test_tds_row.py` which imported from deleted `pylegend.core.database` module
- Fixed `tests/test_helpers/test_legend_service_frames.py` which imported deleted legacy/pandas API frame classes

### Task 2: Clean shared-language tests and verify full suite collection

Cleaned 10 shared primitive test files under `tests/core/language/shared/primitives/`:
- Removed SQL import blocks (`from pylegend.core.database.sql_to_string import ...`, `FrameToSqlConfig`)
- Removed class-level SQL fixtures (`frame_to_sql_config`, `db_extension`, `sql_to_string_config`, `base_query`)
- Removed `__generate_sql_string` and `__generate_sql_string_no_X_assert` helper methods
- Removed all `assert self.__generate_sql_string(...)` and `assert self.db_extension.process_expression(...)` assertions
- Kept all `__generate_pure_string` helpers and `assert self.__generate_pure_string(...)` assertions

Special handling for `test_precise_primitives.py`:
- Updated `TestPreciseIntegerTypes` and `TestPreciseFloatTypes` to use 2-tuple `(frame, row)` from `__make_frame` (dropped `base_query`); updated `__pure` helper signature; removed `__sql` helper
- Removed `TestPrecisePrimitiveDirectInstantiation` class entirely (SQL-only, tested `to_sql_expression` directly)

**Rule 3 auto-fixes for full-suite collection (12 additional files):**
- `tests/core/language/test_literal_expressions.py`: removed deleted SQL imports
- `tests/core/tds/test_tds_frame_cast.py`: removed pandas/legacy API imports, fixed SQL assertion, updated frame factories to LegendQL-only
- `tests/core/test_project_coorindates.py`: rewrote to test accessor methods instead of deleted `sql_params()`
- `tests/core/tds/result_handler/` (3 files): updated from legacy to LegendQL API frames
- `tests/extensions/tds/abstract/` (2 files): removed deleted `FrameToSqlConfig`, `QuerySpecification` imports and SQL methods
- `tests/extensions/tds/frames/legendql_api/` (3 files): removed SQL-only test methods
- `tests/extensions/tds/result_handler/test_to_pandas_df_result_handler.py`: emptied (imports deleted `to_pandas_df_result_handler.py` module)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed shared/__init__.py importing deleted symbols**
- **Found during:** Task 1 pytest collection
- **Issue:** `tests/core/language/shared/__init__.py` imported `FrameToSqlConfig`, `R`, `PandasDfReadConfig` - all deleted in prior waves; also had SQL frame methods
- **Fix:** Cleaned imports and removed SQL methods from `TestTableSpecInputFrame`
- **Files modified:** `tests/core/language/shared/__init__.py`

**2. [Rule 3 - Blocking] Fixed shared/test_tds_row.py importing deleted module**
- **Found during:** Task 1 pytest collection
- **Issue:** `tests/core/language/shared/test_tds_row.py` (`AbstractTestTdsRow`) imported from `pylegend.core.database` (deleted)
- **Fix:** Rewrote to Pure-only `AbstractTestTdsRow` (no SQL methods)
- **Files modified:** `tests/core/language/shared/test_tds_row.py`

**3. [Rule 3 - Blocking] Fixed test_legend_service_frames.py importing deleted classes**
- **Found during:** Task 1 pytest collection
- **Issue:** `tests/test_helpers/test_legend_service_frames.py` imported `LegacyApiLegendServiceInputFrame`, `PandasApiLegendServiceInputFrame` (deleted)
- **Fix:** Rewrote to export only LegendQL API frame factories
- **Files modified:** `tests/test_helpers/test_legend_service_frames.py`

**4. [Rule 3 - Blocking] Fixed 12 additional test files for full-suite collection**
- **Found during:** Task 2 full-suite pytest --co check
- **Issue:** Pre-existing failures from prior-wave deletions (pylegend.core.database, legacy/pandas API) in files outside the plan scope; some caused by our test_helper change
- **Fix:** Per-file targeted cleanup of deleted imports and SQL-only tests
- **Files modified:** 12 files across result_handler, extensions, core/language

## Verification Results

- Zero SQL refs in `tests/core/tds/legendql_api/`, client tests, and tds_row test: **PASS**
- `execute_pure_string` present in `test_legend_client.py`: **PASS**
- Zero SQL refs in `tests/core/language/shared/`: **PASS**
- 12+ files retain `to_pure_expression` coverage: **PASS (12 files)**
- `pytest --co -q tests/` exit 0: **PASS (678 tests collected, 0 errors)**
- flake8 passes on `tests/core/language/shared/`: **PASS**

## Known Stubs

None - no placeholder or stub values introduced.

## Threat Flags

None - test-only changes with no security surface.

## Self-Check: PASSED

Commits verified:
- `031c55c` - Task 1: Clean LegendQL function tests, client tests, and tds_row test
- `4f5d5a9` - Task 2: Clean shared-language tests and fix full suite collection

Both commits exist and contain the expected test file modifications.
