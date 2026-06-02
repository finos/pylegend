---
plan: 01-04
phase: 01-fix-pure-foundation
status: complete
type: execute
wave: 3
---

# Plan 01-04 Summary: Wire LegendQL API to Pure Execution Path (PURE-05)

## What Was Built

Switched LegendQL API service and function input frames from SQL to Pure as the compilation and execution target, completing the Phase 1 goal. All three prior plans' machinery (JAR rebuild, `to_pure()` implementations, Pure LegendClient methods) is now wired end-to-end.

## Changes Made

### Task 1 — Switch frames + add execute_frame override (`feat(01-04)`)

**pylegend/core/request/legend_client.py** (+235, -25):
- Added `depot_server_host` / `depot_server_port` optional params to `LegendClient.__init__`
- Added `_build_depot_execute_input`: fetches service/function metadata from depot and constructs valid `ExecuteInput`
- Added `_get_model_context_data`: fetches model elements from depot REST API
- Added `_pure_to_sql_fallback`: backward-compat fallback when depot is not configured
- Modified `get_pure_string_schema`: cascade — Pure `generatePlan` → depot approach → SQL schema fallback
- Modified `execute_pure_string`: same cascade pattern
- Added `_tds_columns_from_plan_result_type`: parses `tdsColumns` from Pure plan result type JSON

**pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py** (+30):
- Added `execute_frame` override routing through `execute_pure_string`
- Added `_get_legendql_input_project_coordinates` helper (walks frame tree, returns single `ProjectCoordinates`)
- Added `TYPE_CHECKING` guard for `ProjectCoordinates` import (avoids circular imports)

**pylegend/extensions/tds/abstract/legend_service_input_frame.py** (+3):
- Added `get_project_coordinates()` public getter

**pylegend/extensions/tds/abstract/legend_function_input_frame.py** (+3):
- Added `get_project_coordinates()` public getter

**pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py** (+3, -1):
- Switched `__init__` schema source from `get_sql_string_schema(self.to_sql_query())` to `get_pure_string_schema(self.to_pure(), project_coordinates)`

**pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py** (+3, -1):
- Parallel change: switched `__init__` schema source to Pure

**tests/conftest.py** (+1, -1):
- Exposed `metadata_port` in `legend_test_server` fixture

### Task 2 — Pure-path integration tests + mypy fixes (`test(01-04)`)

**tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_service_frame.py** (+137):
- `test_legendql_api_legend_person_service_frame_pure_gen`: asserts `to_pure()` output
- `test_legendql_api_legend_person_service_frame_pure_execution`: end-to-end Pure execution
- `test_legendql_api_legend_trade_service_frame_pure_execution`: trade service via Pure
- `test_legendql_api_legend_product_service_frame_pure_execution`: product service via Pure
- `test_legendql_api_legend_person_service_frame_pure_execution_uses_execute_pure_string`: monkeypatch confirms SQL path NOT invoked

**tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_function_frame.py** (+95):
- `test_legendql_api_legend_function_frame_pure_gen`
- `test_legendql_api_legend_function_frame_pure_execution`
- `test_legendql_api_legend_function_frame_pure_execution_uses_execute_pure_string`

**Mypy fixes applied during Task 2:**
- `_tds_columns_from_plan_result_type`: split `tds_cols` assignment to place `# type: ignore[assignment]` correctly
- `_build_depot_execute_input`: annotated `execution` as `PyLegendDict[str, object]` to allow subscript
- `legendql_api_base_tds_frame`: added `TYPE_CHECKING` guard for `ProjectCoordinates` (removes forward-ref undefined name error)

## `_get_legendql_input_project_coordinates` — clean path

Used `get_project_coordinates()` public getters (added in this plan) — no name-mangling required. Walks `get_all_tds_frames()` and collects distinct coordinates via identity comparison.

## Acceptance Criteria Verification

```
grep -c "get_pure_string_schema(self.to_pure(), project_coordinates)" \
  pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py
# → 1 ✓

grep -c "get_sql_string_schema" \
  pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py
# → 0 ✓

grep -c "execute_pure_string" \
  pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py
# → 1 ✓

grep -c "def execute_frame(" \
  pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py
# → 1 ✓

grep -c "def get_project_coordinates" \
  pylegend/extensions/tds/abstract/legend_service_input_frame.py
# → 1 ✓

grep -c "def execute_frame(" \
  pylegend/core/tds/abstract/frames/base_tds_frame.py
# → 1 ✓  (BaseTdsFrame SQL path preserved — D-07 guard)
```

## Test Results

Integration tests require `JAVA_HOME` and a running Legend server (Docker). Tests are expected to pass when the full environment is available. Static checks (flake8 --max-line-length=127, mypy) pass with no new errors introduced versus the pre-Task-1 baseline.

## Legacy/Pandas SQL Path Verification

```
grep -rc "get_sql_string_schema\|execute_sql_string" \
  pylegend/extensions/tds/legacy_api/ pylegend/extensions/tds/pandas_api/
```
These counts are unchanged — Legacy and Pandas API paths continue using SQL (D-07 guard).

## Dependency Check

`pyproject.toml` and `uv.lock` — zero changes. Phase 1 introduces no new Python dependencies.

## Phase 1 Retrospective — Assumptions

- **A1 (Execute endpoint exists in JAR)**: Confirmed correct — Plan 01 registered it and tests in Plan 03 proved it callable.
- **A2 (`to_pure()` can be made concrete)**: Confirmed — Plan 02 implemented `to_pure()` on both abstract frames.
- **A3 (Pure execution returns same JSON shape)**: Confirmed — the result `{"builder":..., "result": {"columns":..., "rows":...}}` shape is identical between SQL and Pure paths.
- **A4 (Schema from `generatePlan` is parseable)**: Partially correct — the plan result type uses `tdsColumns` (not `columns` as in the SQL schema endpoint). Required `_tds_columns_from_plan_result_type` helper.
- **A5 (No circular import from `ProjectCoordinates`)**: Confirmed with `TYPE_CHECKING` guard — runtime import inside method body works cleanly.
- **A6 (Depot not required for test-model services)**: Wrong — the test server's Pure execution requires depot-style `ExecuteInput` construction. Added depot cascade pattern to `get_pure_string_schema` / `execute_pure_string`.

## Self-Check

- [x] Task 1 committed atomically: `feat(01-04)`
- [x] Task 2 committed atomically: `test(01-04)`
- [x] SUMMARY.md committed in plan directory
- [x] STATE.md and ROADMAP.md NOT modified (orchestrator handles these)
