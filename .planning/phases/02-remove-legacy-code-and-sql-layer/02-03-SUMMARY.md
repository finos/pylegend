---
phase: 02-remove-legacy-code-and-sql-layer
plan: "03"
subsystem: frame-abstractions-and-public-api
tags: [sql-removal, pure-paths, public-api-cleanup, import-hygiene]
dependency_graph:
  requires: ["02-01", "02-02"]
  provides: ["import-pylegend-green", "sql-free-frame-layer"]
  affects: ["legendql-api", "frame-abstractions", "extension-bases", "public-init"]
tech_stack:
  added: []
  patterns: ["Pure-only execution paths", "SQL-free frame hierarchy"]
key_files:
  modified:
    - pylegend/core/tds/tds_frame.py
    - pylegend/core/tds/abstract/frames/base_tds_frame.py
    - pylegend/core/tds/abstract/frames/applied_function_tds_frame.py
    - pylegend/core/request/legend_client.py
    - pylegend/extensions/tds/result_handler/__init__.py
    - pylegend/extensions/tds/abstract/legend_service_input_frame.py
    - pylegend/extensions/tds/abstract/legend_function_input_frame.py
    - pylegend/extensions/tds/abstract/csv_tds_frame.py
    - pylegend/extensions/tds/abstract/table_spec_input_frame.py
    - pylegend/extensions/tds/legendql_api/frames/legendql_api_table_spec_input_frame.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_head_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_filter_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_drop_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_slice_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_select_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_distinct_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_rename_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_concatenate_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_sort_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_join_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_extend_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_window_extend_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_groupby_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_aggregate_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_project_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_cast_function.py
    - pylegend/core/tds/legendql_api/frames/functions/legendql_api_asofjoin_function.py
    - pylegend/__init__.py
    - pylegend/samples/__init__.py
    - pylegend/core/language/__init__.py
decisions:
  - "Replaced QualifiedName (from core.sql.metamodel) in TableSpecInputFrameAbstract with plain PyLegendList[str] to eliminate last sql.metamodel dependency in extension abstractions"
  - "Removed execute_frame and execute_frame_to_string from PyLegendTdsFrame/BaseTdsFrame abstractions since Pure-path subclasses implement execute_frame directly"
metrics:
  duration: "~25 minutes"
  completed: "2026-06-01"
  tasks_completed: 3
  tasks_total: 3
  files_modified: 30
---

# Phase 02 Plan 03: SQL/Pandas Surface Removal and `import pylegend` Fix Summary

Surgical removal of all SQL/pandas surface from the retained frame, client, and extension layers, restoring `import pylegend` to a working state with only Pure paths remaining.

## What Was Built

Complete removal of SQL generation and pandas execution surface from the LegendQL frame hierarchy, extension bases, and public API. After this plan, `python -c "import pylegend"` succeeds with no ImportError, and the codebase retains only the Pure compilation path.

## Tasks Completed

### Task 1: Remove SQL/pandas from frame abstractions and LegendClient (f425d2f)

- **tds_frame.py**: Removed `FrameToSqlConfig` class, `to_sql_query` abstract, `execute_frame`/`execute_frame_to_string` abstracts, `to_pandas_df`/`to_pandas` convenience methods. Removed `import importlib`, `import pandas`, `SqlToStringGenerator`, `PandasDfReadConfig` imports. Retained `FrameToPureConfig` and `to_pure_query` abstract.
- **base_tds_frame.py**: Removed `to_sql_query_object` abstract, `to_sql_query` concrete, `execute_frame`/`execute_frame_to_string`/`execute_frame_to_pandas_df` concrete methods. Removed all SQL/pandas imports. Retained `to_pure`, `to_pure_query`, `get_legend_client`.
- **applied_function_tds_frame.py**: Removed `to_sql` abstract from `AppliedFunction`, `to_sql_query_object` concrete from `AppliedFunctionTdsFrame`. Removed `core.sql.metamodel`/`FrameToSqlConfig` imports.
- **legend_client.py**: Removed `get_sql_string_schema` and `execute_sql_string` methods. Removed unused `tds_columns_from_json` import. Retained `get_pure_string_schema` and `execute_pure_string`.
- **extensions/tds/result_handler/__init__.py**: Removed `ToPandasDfResultHandler`/`PandasDfReadConfig` exports; `__all__` is now `[]`.

### Task 2: Remove SQL from extension bases, LegendQL extension frames, and 17 function files (9ad5343)

- **4 abstract extension bases**: Removed all `core.sql.metamodel` imports, `FrameToSqlConfig` imports, and `to_sql_query_object` methods from `legend_service_input_frame.py`, `legend_function_input_frame.py`, `csv_tds_frame.py`.
- **table_spec_input_frame.py**: Replaced `QualifiedName` (from `core.sql.metamodel`) with `PyLegendList[str]` for the `table` attribute, removing the last `core.sql` dependency in this file.
- **legendql_api_table_spec_input_frame.py**: Updated `__str__` to use `self.table` directly (no longer `self.table.parts`).
- **17 LegendQL function files**: Removed all `sql_query_helpers`, `core.sql.metamodel`, `core.sql.metamodel_extension`, and `FrameToSqlConfig` imports; removed all `to_sql()` methods. Pure paths (`to_pure`) preserved on all 17 files.

### Task 3: Clean public __init__.py files and smoke-test (0d4a5ce)

- **pylegend/__init__.py**: Removed `LegacyApiTdsClient`, `legacy_api_tds_client`, `agg`, `olap_rank`, `olap_agg`. Kept `now`, `today`, `current_user`, all LegendQL/request/coordinates symbols.
- **pylegend/samples/__init__.py**: Removed `pandas_api` import and `__all__` entry. Retained `legendql_api`.
- **pylegend/core/language/__init__.py**: Removed `LegacyApiTdsRow`, `LegacyApiAggregateSpecification`, `agg`, `LegacyApiOLAPGroupByOperation`, `LegacyApiOLAPAggregation`, `LegacyApiOLAPRank`, `olap_agg`, `olap_rank` from imports and `__all__`. Retained all shared/LegendQL primitives, collections, and functions.
- **Smoke test**: `python -c "import pylegend; print('import ok')"` passes.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical] Replaced QualifiedName with PyLegendList in TableSpecInputFrameAbstract**
- **Found during:** Task 2 when removing `core.sql` imports
- **Issue:** `TableSpecInputFrameAbstract` typed `table` as `QualifiedName` (from `core.sql.metamodel`). Keeping this import would have failed the verification check for zero `core.sql` references in extension bases.
- **Fix:** Replaced `QualifiedName` with `PyLegendList[str]` for the attribute. Updated `to_pure` to use `'.'.join(self.table)` directly. Updated `legendql_api_table_spec_input_frame.py` `__str__` to remove `.parts` access.
- **Files modified:** `table_spec_input_frame.py`, `legendql_api_table_spec_input_frame.py`
- **Commit:** 9ad5343

**2. [Rule 1 - Bug] Removed unused imports after SQL method deletion**
- **Found during:** Tasks 1 and 2
- **Issue:** After removing `to_sql` methods, several files had unused imports (`PythonDecimal`, `PyLegendBooleanLiteralExpression`, `PyLegendTuple`, `tds_columns_from_json`) that would cause flake8 failures.
- **Fix:** Removed all now-unused imports from affected files.
- **Files modified:** Multiple function files, `legend_client.py`
- **Commit:** 9ad5343

## Known Stubs

None - all changes are deletions of SQL/pandas code; no placeholder values introduced.

## Threat Flags

None - this plan only removes code (SQL generation and pandas execution paths). No new network endpoints, auth paths, or schema changes introduced.

## Self-Check: PASSED

- pylegend/__init__.py: modified and committed (0d4a5ce)
- pylegend/core/tds/tds_frame.py: modified and committed (f425d2f)
- pylegend/core/tds/abstract/frames/base_tds_frame.py: modified and committed (f425d2f)
- pylegend/core/request/legend_client.py: modified and committed (f425d2f)
- python -c "import pylegend" exits 0 - VERIFIED
- All 3 task commits present in git log - VERIFIED
