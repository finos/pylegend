---
phase: 01-fix-pure-foundation
plan: "05"
subsystem: legendql-api-execution
tags:
  - legendql
  - execute_frame
  - pure-execution
  - gap-closure
  - sql-fallback-removal
dependency_graph:
  requires:
    - 01-04
  provides:
    - test_table_spec_frame_execution_error passing (Gap 1 BLOCKER closed)
    - no silent SQL fallback in LegendClient Pure methods (Gap 2 correctness fix)
    - xfail marks removed from test_e2e_pure_schema_api/test_e2e_pure_execute_api
  affects:
    - LegendQLApiBaseTdsFrame execution path
    - LegendClient.get_pure_string_schema
    - LegendClient.execute_pure_string
tech_stack:
  added: []
  patterns:
    - depot cascade as sole fallback (no SQL fallback) in LegendClient Pure methods
    - BaseTdsFrame.execute_frame handles all LegendQL frames (TableSpec raises ValueError)
key_files:
  modified:
    - pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py
    - pylegend/core/request/legend_client.py
    - tests/core/request/test_legend_client_e2e.py
decisions:
  - Remove execute_frame override from LegendQLApiBaseTdsFrame rather than guarding it
  - Remove _pure_to_sql_fallback entirely rather than deprecating — no legitimate use
  - Wire depot_server_host/depot_server_port to Pure e2e tests (same pattern as Plan 04 service frame tests)
metrics:
  duration: "~15 minutes"
  completed: "2026-05-31"
  tasks_completed: 3
  tasks_total: 3
  files_modified: 3
---

# Phase 01 Plan 05: Execute-frame override removal and SQL fallback deletion Summary

Deleted the `execute_frame` override from `LegendQLApiBaseTdsFrame` (restoring pre-Plan-04 behavior where `BaseTdsFrame.execute_frame` raises `ValueError` for non-executable frames), removed the silent SQL fallback cascade from `LegendClient`'s Pure methods, and cleaned up two stale `xfail` decorators from the Pure e2e tests.

## What Was Built

Three targeted deletions closing two verification gaps identified in `01-VERIFICATION.md`:

**Gap 1 (BLOCKER, TEST-01) — Closed:**
- `execute_frame` method deleted from `LegendQLApiBaseTdsFrame` (was lines 63-72)
- `_get_legendql_input_project_coordinates` helper deleted (was lines 74-90)
- `ResultHandler` import deleted (only used by deleted methods)
- `PyLegendTypeVar` and `TYPE_CHECKING` imports deleted (only used by deleted methods)
- `BaseTdsFrame.execute_frame` now handles all LegendQL frames: calls `get_legend_client()` which raises `ValueError` for non-executable frames (TableSpec) as `test_table_spec_frame_execution_error` expects

**Gap 2 (correctness fix) — Closed:**
- SQL fallback branch deleted from `get_pure_string_schema`: after depot path fails, `raise` propagates `pure_err`
- SQL fallback branch deleted from `execute_pure_string`: same pattern
- `_pure_to_sql_fallback` method deleted entirely (was lines 286-326)
- Depot cascade path (`_build_depot_execute_input`) preserved unchanged

**Gap 2 (xfail cleanup, PURE-05) — Closed:**
- `@pytest.mark.xfail` with stale "To be resolved in Plan 04" reason removed from `test_e2e_pure_schema_api`
- `@pytest.mark.xfail` with same stale reason removed from `test_e2e_pure_execute_api`
- Both tests now wire `depot_server_host="localhost"` and `depot_server_port=legend_test_server["metadata_port"]` to `LegendClient` constructor (canonical pattern from Plan 04 service frame tests)

## Verification Results

- `test_table_spec_frame_execution_error` PASSES: both `pytest.raises(ValueError)` assertions pass with expected message
- `_pure_to_sql_fallback` absent from codebase: `grep` returns 0
- `"falling back to SQL"` absent: `grep` returns 0
- `@pytest.mark.xfail` absent from `test_legend_client_e2e.py`: `grep` returns 0
- 6 tests collected in `test_legend_client_e2e.py` (was previously 6 with 2 xfail)
- All non-infrastructure tests pass (402 passed, errors only from JAVA_HOME/Docker not available)
- mypy strict: 0 issues on modified files
- flake8 (--max-line-length=127): 0 issues on modified files

## Deviations from Plan

None — plan executed exactly as written.

## Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Delete execute_frame override and _get_legendql_input_project_coordinates | 105da90 | pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py |
| 2 | Remove SQL fallback from LegendClient Pure methods; delete _pure_to_sql_fallback | e376879 | pylegend/core/request/legend_client.py |
| 3 | Remove stale xfail decorators; wire depot cascade to Pure e2e tests | 089d1ab | tests/core/request/test_legend_client_e2e.py |

## Known Stubs

None.

## Threat Flags

None — changes are deletions only. No new network endpoints, auth paths, or schema changes introduced.

## Self-Check: PASSED

- `pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py`: execute_frame/helper deleted, imports cleaned
- `pylegend/core/request/legend_client.py`: _pure_to_sql_fallback gone, SQL fallback branches gone
- `tests/core/request/test_legend_client_e2e.py`: xfail decorators gone, depot kwargs wired
- Commits 105da90, e376879, 089d1ab all present in git log
