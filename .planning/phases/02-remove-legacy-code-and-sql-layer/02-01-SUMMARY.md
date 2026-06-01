---
phase: 02-remove-legacy-code-and-sql-layer
plan: 01
subsystem: codebase-cleanup
tags: [deletion, refactor, grammar-method, legacy-api, pandas-api, sql-layer]
dependency_graph:
  requires: []
  provides: [pylegend.utils.grammar_method, legacy-api-deleted, pandas-api-deleted, sql-layer-deleted]
  affects: [pylegend.core.language.shared.primitives]
tech_stack:
  added: []
  patterns: [module-relocation, wholesale-deletion]
key_files:
  created:
    - pylegend/utils/grammar_method.py
  modified:
    - pylegend/core/language/shared/primitives/primitive.py
    - pylegend/core/language/shared/primitives/boolean.py
    - pylegend/core/language/shared/primitives/integer.py
    - pylegend/core/language/shared/primitives/float.py
    - pylegend/core/language/shared/primitives/decimal.py
    - pylegend/core/language/shared/primitives/number.py
    - pylegend/core/language/shared/primitives/string.py
    - pylegend/core/language/shared/primitives/date.py
    - pylegend/core/language/shared/primitives/datetime.py
    - pylegend/core/language/shared/primitives/strictdate.py
  deleted:
    - pylegend/core/tds/legacy_api/ (entire tree)
    - pylegend/core/tds/pandas_api/ (entire tree)
    - pylegend/core/language/legacy_api/ (entire tree)
    - pylegend/core/language/pandas_api/ (entire tree)
    - pylegend/core/sql/ (entire tree)
    - pylegend/core/database/ (entire tree)
    - pylegend/extensions/tds/legacy_api/ (entire tree)
    - pylegend/extensions/tds/pandas_api/ (entire tree)
    - pylegend/extensions/database/ (entire tree)
    - pylegend/samples/pandas_api/ (entire tree)
    - pylegend/legacy_api_tds_client.py
    - pylegend/core/tds/sql_query_helpers.py
    - pylegend/extensions/tds/result_handler/to_pandas_df_result_handler.py
    - tests/core/tds/legacy_api/ (entire tree)
    - tests/core/tds/pandas_api/ (entire tree)
    - tests/core/language/legacy_api/ (entire tree)
    - tests/core/database/ (entire tree)
    - tests/extensions/database/ (entire tree)
    - tests/extensions/tds/frames/legacy_api/ (entire tree)
    - tests/extensions/tds/frames/pandas_api/ (entire tree)
    - tests/samples/pandas_api/ (entire tree)
    - tests/test_legacy_api_tds_client.py
decisions:
  - grammar_method decorator re-homed to pylegend/utils/grammar_method.py using TypeVar-bound Callable with type: ignore[type-arg] to stay consistent with existing patterns in the codebase
metrics:
  duration: ~10 minutes
  completed: 2026-06-01T19:01:58Z
  tasks_completed: 2
  files_changed: 185
---

# Phase 02 Plan 01: Delete Legacy/Pandas/SQL Trees and Re-home grammar_method Summary

**One-liner:** grammar_method decorator relocated from pandas_api series_helper to pylegend/utils; 10 production trees + 8 test trees + 3 orphaned files removed via git rm in the foundational subtractive wave.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Create pylegend/utils/grammar_method.py and update 10 primitive imports | b2f76c8 | pylegend/utils/grammar_method.py (new), 10 primitive files (import updated) |
| 2 | Delete Legacy/Pandas/SQL directory trees, test mirrors, and orphaned files | 781b912 | 174 files deleted across 10 production trees, 8 test trees, 3 individual files |

## What Was Built

### Task 1: grammar_method Re-home

Created `pylegend/utils/grammar_method.py` with:
- Full Apache 2.0 header (copyright 2026)
- `F = TypeVar('F', bound=Callable)` type variable with `# type: ignore[type-arg]`
- `grammar_method(func: F) -> F` identity decorator that sets `func._is_grammar_method = True`
- `__all__: PyLegendSequence[str] = ["grammar_method"]`

Updated 10 shared primitive files to import from `pylegend.utils.grammar_method` instead of `pylegend.core.tds.pandas_api.frames.helpers.series_helper`.

### Task 2: Deletion

Removed 185 files across:
- 10 production directory trees (legacy_api, pandas_api, sql, database, etc.)
- 3 individual production files
- 8 test mirror directory trees
- 1 test file

Retained: `pylegend/samples/legendql_api/`, `pylegend/samples/local_legend_env.py`, `pylegend/core/tds/result_handler/` (CSV/string/JSON handlers).

## Verification Results

- `pylegend/utils/grammar_method.py` exists with correct decorator implementation
- Zero primitive files still import from `pandas_api.frames.helpers.series_helper`
- Exactly 10 primitive files import from `pylegend.utils.grammar_method`
- All 10 production directory trees confirmed absent
- All 3 orphaned production files confirmed absent
- All 8 test mirror trees confirmed absent
- LegendQL tree, samples/legendql_api, local_legend_env.py, result_handler remain intact
- NOTE: `import pylegend` is intentionally broken at this point (retained files still import from core.sql); this is expected and will be fixed by Plan 02-03

## Requirements Satisfied

- **REMV-01:** Legacy API (`LegacyApiTdsClient`, all `legacy_api/` modules) removed from codebase
- **REMV-02:** Pandas API (`PandasApiTdsClient`, all `pandas_api/` modules) removed from codebase
- **REMV-03:** SQL metamodel layer (`core/sql/`, `core/database/`, `extensions/database/vendors/`) removed from codebase (directory-level; cross-cutting cleanup in Plans 02-02/02-03)

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None - this plan performed deletions and a decorator re-home with no new feature code.

## Threat Flags

None - this plan performs only deletions and a simple decorator extraction; no new network endpoints, auth paths, or trust boundaries introduced.

## Self-Check: PASSED

- [x] `pylegend/utils/grammar_method.py` exists
- [x] Commit b2f76c8 exists (Task 1)
- [x] Commit 781b912 exists (Task 2)
- [x] All 10 production trees absent
- [x] All retained directories present
