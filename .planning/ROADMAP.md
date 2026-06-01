# Roadmap: PyLegend 2.0

## Overview

PyLegend 2.0 replaces the three-layer architecture (LegendQL + Legacy + Pandas APIs all compiling to a SQL metamodel) with a single Ibis backend that compiles directly to Pure. The rewrite proceeds in four coarse phases: first fix the broken Pure foundation and verify the test suite is green, then strip out the legacy code, then build the Ibis backend and compiler, and finally rewire the LegendQL API as a thin Ibis wrapper and delete dead code.

## Phases

**Phase Numbering:**

- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Fix Pure Foundation** - Fix broken `to_pure()` roots, wire `LegendClient` Pure execution, verify PCT is green (completed 2026-05-31)
- [ ] **Phase 2: Remove Legacy Code** - Delete Legacy API, Pandas API, SQL metamodel layer, and obsolete dependencies
- [ ] **Phase 3: Ibis Backend** - Build backend skeleton, type mapping, and full Pure compiler covering all TdsFrame operations
- [ ] **Phase 4: Rewire LegendQL + Cleanup** - Rewire LegendQL as Ibis wrapper, deprecate SQL surface, delete dead code, validate CI

## Phase Details

### Phase 1: Fix Pure Foundation

**Goal**: `LegendClient` can execute queries end-to-end using Pure (not SQL) as the compilation target, with the PCT test matrix green
**Depends on**: Nothing (first phase)
**Requirements**: PURE-01, PURE-02, PURE-03, PURE-04, PURE-05, TEST-01, TEST-02
**Success Criteria** (what must be TRUE):

  1. `LegendServiceInputFrame.to_pure()` and `LegendFunctionInputFrame.to_pure()` return valid Pure root expressions without raising `RuntimeError`
  2. `LegendClient` exposes `execute_pure_string()` and `get_pure_string_schema()` methods that communicate with the Legend engine over the correct HTTP endpoint
  3. Running the existing LegendQL integration tests against a Legend engine produces results via Pure execution (no SQL path invoked)
  4. The Legend PCT matrix remains green after these changes

**Plans**: 5 plansPlans:
**Wave 1**

- [x] 01-01-PLAN.md — Register Execute JAX-RS resource in PyLegendSqlServer.java; rebuild test server JAR via Maven (Wave 1)
- [x] 01-02-PLAN.md — Implement to_pure() bodies for LegendServiceInputFrameAbstract and LegendFunctionInputFrameAbstract with unit + integration tests (Wave 1)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-03-PLAN.md — Add LegendClient.execute_pure_string and get_pure_string_schema (plus _build_execute_input helper) and Pure e2e tests (Wave 2)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01-04-PLAN.md — Switch LegendQL service/function input frames to Pure schema fetch; override execute_frame on LegendQLApiBaseTdsFrame to route through execute_pure_string; integration tests + end-of-phase verification (Wave 3)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 01-05-PLAN.md — GAP CLOSURE: Delete execute_frame override and _get_legendql_input_project_coordinates from LegendQLApiBaseTdsFrame (fix test_table_spec_frame_execution_error by removing the root cause); remove SQL fallback from LegendClient.execute_pure_string and get_pure_string_schema; remove stale xfail decorators from test_e2e_pure_* (Wave 4)

### Phase 2: Remove Legacy Code and SQL Layer

**Goal**: The codebase contains only the LegendQL API and the Pure HTTP methods on LegendClient; Legacy API, Pandas API, the entire SQL metamodel, SQL execution paths, and associated dependencies are deleted
**Depends on**: Phase 1
**Requirements**: REMV-01, REMV-02, REMV-03, REMV-04, REMV-05
**Rationale**: The only real PyLegend consumer uses `to_pure()` for Pure expression generation (reverse PCT tests), not SQL execution. Nobody executes queries via SQL. Deleting the SQL layer now removes substantial dead weight and clarifies the codebase before building the Ibis backend. Legacy and Pandas API deletion is safe immediately; the SQL metamodel can go at the same time since LegendQL's SQL fallback was already removed in Phase 1 (Plan 05).
**Success Criteria** (what must be TRUE):

  1. `legacy_api/`, `pandas_api/`, `core/sql/`, `core/database/`, and `extensions/database/vendors/` directories do not exist in the repository
  2. `BaseTdsFrame.execute_frame`, `BaseTdsFrame.to_sql_query()`, `execute_sql_string`, and `get_sql_string_schema` on `LegendClient` do not exist
  3. `pyproject.toml` runtime deps are reduced to `requests`, `ijson`, and `testcontainers` (moved to dev); `pandas`, `numpy`, `sqlalchemy`, `pg8000`, `pymysql`, `cryptography`, `mockito` are removed entirely
  4. `import pylegend` succeeds and the LegendQL + Pure test suite passes with no import errors

**Plans**: 5 plans

**Wave 1**

- [x] 02-01-PLAN.md — Re-home grammar_method to pylegend/utils; update 10 primitive imports; delete all Legacy/Pandas/SQL directory trees, test mirrors, and 3 orphaned files (Wave 1)

**Wave 2** *(blocked on Wave 1)*

- [ ] 02-02-PLAN.md — Remove SQL metamodel imports and to_sql_expression from shared language layer (primitives, operations, expressions, project_cooridnates, legendql language files) (Wave 2)

**Wave 3** *(blocked on Wave 2)*

- [ ] 02-03-PLAN.md — Remove SQL/pandas surface from frame abstractions, LegendClient, extension bases, 17 LegendQL function files, and public __init__.py files; restore import pylegend (Wave 3)

**Wave 4** *(blocked on Wave 3)*

- [ ] 02-04-PLAN.md — Remove SQL assertions/fixtures from retained test files (LegendQL function tests, shared-language tests, client tests, tds_row test); confirm full pytest collection (Wave 4)

**Wave 5** *(blocked on Wave 4)*

- [ ] 02-05-PLAN.md — Trim pyproject.toml runtime deps to requests+ijson, move testcontainers to dev, remove SQL/pandas/mocking deps; uv sync + full test suite as final acceptance gate (Wave 5)

### Phase 3: Ibis Backend

**Goal**: `ibis.legend` is a fully functional registered Ibis backend that can connect to a Legend engine, construct table expressions with correct schema, and compile all TdsFrame operations to Pure via `Backend.execute()`
**Depends on**: Phase 2
**Requirements**: IBIS-01, IBIS-02, IBIS-03, IBIS-04, IBIS-05, IBIS-06, QLOP-01, QLOP-02, QLOP-03, QLOP-04, QLOP-05, QLOP-06, QLOP-07, QLOP-08, QLOP-09, QLOP-10, QLOP-11, QLOP-12, QLOP-13, QLOP-14, QLOP-15, QLOP-16, QLOP-17, QLOP-18
**Success Criteria** (what must be TRUE):

  1. `import ibis; ibis.legend.connect(host, port, auth_scheme)` returns a connected backend instance without error
  2. `backend.table(pattern, project_coordinates)` returns an `ibis.Table` with the correct column names and types populated from the Legend engine schema
  3. `backend.execute(ibis_expr)` returns a `pandas.DataFrame` for Ibis expressions covering all 18 TdsFrame operation types (filter, limit, drop, slice, select, rename, extend, project, cast, distinct, group_by, aggregate, sort, concatenate, all join types, as_of_join, window functions, and duration-unit window frames)
  4. `ibis-framework>=9.0` is declared as a runtime dependency in `pyproject.toml` and the backend entry point is registered under `[project.entry-points."ibis.backends"]`

**Plans**: TBD

### Phase 4: Rewire LegendQL + Cleanup

**Goal**: `LegendQLApiTdsFrame` operations build Ibis expressions internally and route through the Ibis backend; the CI matrix is confirmed green across the full Python version range
**Depends on**: Phase 3
**Requirements**: COMPAT-01, COMPAT-02, COMPAT-03, COMPAT-04, COMPAT-05, COMPAT-06, COMPAT-07, COMPAT-08, TEST-03, TEST-04
**Success Criteria** (what must be TRUE):

  1. All `LegendQLApiTdsFrame` operation method signatures and return types are unchanged from 1.x — existing callers using `legend_service_frame()`, `legend_function_frame()`, and all frame operations require no code changes
  2. `to_pure_query()` on any `LegendQLApiTdsFrame` produces the same Pure string as in 1.x; `to_sql_query()` no longer exists
  3. `FrameToSqlConfig`, `to_sql_query_object()`, `AppliedFunction.to_sql()`, and `AppliedFunction.to_pure()` recursive-descent code are absent from the codebase
  4. The CI matrix passes on Python 3.9–3.14 across Ubuntu and Windows; `as_of_join` has at least one integration test or is marked `xfail` with a tracking comment

**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Fix Pure Foundation | 5/5 | Complete   | 2026-06-01 |
| 2. Remove Legacy Code | 1/5 | In Progress|  |
| 3. Ibis Backend | 0/TBD | Not started | - |
| 4. Rewire LegendQL + Cleanup | 0/TBD | Not started | - |
