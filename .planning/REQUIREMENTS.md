# Requirements: PyLegend 2.0

**Defined:** 2026-05-31
**Core Value:** Internal library teams and Legend PCT tests can build and execute TDS queries against a Legend engine using familiar Python APIs without maintaining knowledge of the underlying Pure or engine protocol.

## v1 Requirements

### Pure Foundation

- [ ] **PURE-01**: `LegendServiceInputFrame.to_pure()` generates a valid Pure root expression for a Legend service
- [ ] **PURE-02**: `LegendFunctionInputFrame.to_pure()` generates a valid Pure root expression for a Legend function
- [ ] **PURE-03**: `LegendClient` can execute a Pure TDS query string against the Legend engine and return a streaming response
- [ ] **PURE-04**: `LegendClient` can retrieve TDS column schema from the Legend engine using a Pure expression (without SQL)
- [ ] **PURE-05**: End-to-end query execution via the existing LegendQL API produces results using Pure (not SQL) as the compilation target

### Removals

- [ ] **REMV-01**: Legacy API (`LegacyApiTdsClient`, all `legacy_api/` modules) is removed from the codebase
- [ ] **REMV-02**: Pandas API (`PandasApiTdsClient`, all `pandas_api/` modules) is removed from the codebase
- [ ] **REMV-03**: SQL metamodel layer (`core/sql/`, `core/database/`, `extensions/database/vendors/`) is removed from the codebase
- [ ] **REMV-04**: SQL-related dev dependencies (`sqlalchemy`, `pg8000`, `pymysql`, `cryptography`, `mockito`) are removed from `pyproject.toml`
- [ ] **REMV-05**: `testcontainers` is moved from runtime dependency to dev-only dependency

### Ibis Backend

- [ ] **IBIS-01**: `ibis.legend` is importable as a registered Ibis backend (entry point `[project.entry-points."ibis.backends"] legend = "pylegend.ibis_backend:Backend"`)
- [ ] **IBIS-02**: `ibis.legend.connect(host, port, auth_scheme, ...)` returns a connected Ibis backend instance using `LegendClient` internally
- [ ] **IBIS-03**: `backend.table(pattern, project_coordinates)` returns an `ibis.Table` expression with the correct schema populated at construction time
- [ ] **IBIS-04**: `backend.execute(ibis_expr)` compiles the Ibis expression to Pure, sends it to the Legend engine, and returns a `pandas.DataFrame`
- [ ] **IBIS-05**: The Ibis backend's Pure compiler correctly generates Pure for all TdsFrame operations currently supported in PyLegend 1.x (see LegendQL Operations below)
- [ ] **IBIS-06**: `ibis-framework>=9.0` (verify version) is added as a runtime dependency in `pyproject.toml`

### LegendQL Operations (Ibis compiler must cover all)

- [ ] **QLOP-01**: `filter(predicate)` — row filtering via boolean lambda over typed columns
- [ ] **QLOP-02**: `head(n)` / `limit(n)` — take first N rows
- [ ] **QLOP-03**: `drop(n)` — skip first N rows
- [ ] **QLOP-04**: `slice(start, end_exclusive)` — range of rows
- [ ] **QLOP-05**: `select(cols)` / `restrict(cols)` — column subset
- [ ] **QLOP-06**: `rename([(old, new), ...])` — column renaming
- [ ] **QLOP-07**: `extend([(name, lambda), ...])` — add computed columns
- [ ] **QLOP-08**: `project([(name, lambda), ...])` — replace output schema with computed columns
- [ ] **QLOP-09**: `cast({col: type})` — change declared column type
- [ ] **QLOP-10**: `distinct(cols=None)` — deduplication
- [ ] **QLOP-11**: `group_by(cols, agg_specs)` — groupBy + aggregate with triplet-lambda specs
- [ ] **QLOP-12**: `aggregate(agg_specs)` — whole-table aggregate (no grouping key)
- [ ] **QLOP-13**: `sort(cols_or_lambda)` — sort by columns ascending/descending
- [ ] **QLOP-14**: `concatenate(other)` — vertical union of two compatible frames
- [ ] **QLOP-15**: `inner_join / left_join / right_join / full_join` — all standard join types
- [ ] **QLOP-16**: `as_of_join(other, match_fn, join_cond)` — Legend-native as-of join (Pure-only; no SQL equivalent)
- [ ] **QLOP-17**: `window(partition_by, order_by, frame)` + `window_extend(window, extend_cols)` — window functions including `row_number`, `rank`, `dense_rank`, `lead`, `lag`, and windowed aggregates
- [ ] **QLOP-18**: Row-based and range-based window frames including duration-unit range bounds (`DAYS`, `HOURS`, etc.)

### LegendQL API Backwards Compatibility

- [ ] **COMPAT-01**: `LegendQLApiTdsClient` public interface is unchanged — `legend_service_frame()` and `legend_function_frame()` have the same signatures and return `LegendQLApiTdsFrame` instances
- [ ] **COMPAT-02**: All `LegendQLApiTdsFrame` operation method signatures are unchanged from 1.x
- [ ] **COMPAT-03**: All frame operations return instances that are still `LegendQLApiTdsFrame` subclasses (preserves `isinstance` checks in internal library)
- [ ] **COMPAT-04**: `TdsColumn` names and types returned by all operations are unchanged
- [ ] **COMPAT-05**: `to_pure_query()` on `LegendQLApiTdsFrame` produces the same Pure string as in 1.x
- [ ] **COMPAT-06**: `to_pandas()` / `execute_frame_to_pandas_df()` convenience methods remain on `TdsFrame`
- [ ] **COMPAT-07**: `HeaderTokenAuthScheme`, `CookieAuthScheme`, and `LocalhostEmptyAuthScheme` remain available with unchanged interfaces
- [ ] **COMPAT-08**: `ResultHandler` interface and all three built-in handlers (CSV, Pandas DataFrame, string) remain available

### Testing and CI

- [ ] **TEST-01**: Legend PCT matrix remains green after the full rewrite
- [ ] **TEST-02**: All existing LegendQL integration tests pass against the LegendQL API backed by Pure execution
- [ ] **TEST-03**: CI matrix continues to cover Python 3.9–3.14 on Ubuntu and Windows
- [ ] **TEST-04**: `as_of_join` has at least one integration test against a real Legend engine (or is marked `xfail` with a tracking comment if no engine is available in CI)

## v2 Requirements

### Ibis Ecosystem

- **IBIS-V2-01**: Full `ibis-backends` test suite compliance (currently blocked by Legend/Pure being more restricted than general SQL backends)
- **IBIS-V2-02**: `pylegend[pandas]` optional extra (keep pandas and numpy as optional deps for users who only need Pure generation without DataFrame output)

### New Features

- **FEAT-V2-01**: Async execution path (`async def execute(...)`)
- **FEAT-V2-02**: Apache Arrow streaming result handler
- **FEAT-V2-03**: JSON streaming result handler
- **FEAT-V2-04**: New TdsFrame operations beyond 1.x scope (deferred pending v2 scoping)

## Out of Scope

| Feature | Reason |
|---------|--------|
| SQL generation from any API or backend | Pure is the only compilation target in 2.0; SQL compilation was never needed |
| Pure output formatting / pretty-print config | Keep generation simple; no indent/separator config |
| `project_cooridnates.py` typo rename | Public import surface; rename would break internal library; deferred to a separate minor release |
| Expansion of supported operations beyond 1.x | Out of scope per PROJECT.md; defer to future milestone |
| Local/in-memory expression evaluation | Legend requires a live engine; read-only query backend only |
| DML operations (INSERT/UPDATE/DELETE) | Legend Python client is query-only |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| PURE-01 | Phase 1 | Pending |
| PURE-02 | Phase 1 | Pending |
| PURE-03 | Phase 1 | Pending |
| PURE-04 | Phase 1 | Pending |
| PURE-05 | Phase 1 | Pending |
| TEST-01 | Phase 1 | Pending |
| TEST-02 | Phase 1 | Pending |
| REMV-01 | Phase 2 | Pending |
| REMV-02 | Phase 2 | Pending |
| REMV-03 | Phase 2 | Pending |
| REMV-04 | Phase 2 | Pending |
| REMV-05 | Phase 2 | Pending |
| IBIS-01 | Phase 3 | Pending |
| IBIS-02 | Phase 3 | Pending |
| IBIS-03 | Phase 3 | Pending |
| IBIS-04 | Phase 3 | Pending |
| IBIS-05 | Phase 3 | Pending |
| IBIS-06 | Phase 3 | Pending |
| QLOP-01 | Phase 3 | Pending |
| QLOP-02 | Phase 3 | Pending |
| QLOP-03 | Phase 3 | Pending |
| QLOP-04 | Phase 3 | Pending |
| QLOP-05 | Phase 3 | Pending |
| QLOP-06 | Phase 3 | Pending |
| QLOP-07 | Phase 3 | Pending |
| QLOP-08 | Phase 3 | Pending |
| QLOP-09 | Phase 3 | Pending |
| QLOP-10 | Phase 3 | Pending |
| QLOP-11 | Phase 3 | Pending |
| QLOP-12 | Phase 3 | Pending |
| QLOP-13 | Phase 3 | Pending |
| QLOP-14 | Phase 3 | Pending |
| QLOP-15 | Phase 3 | Pending |
| QLOP-16 | Phase 3 | Pending |
| QLOP-17 | Phase 3 | Pending |
| QLOP-18 | Phase 3 | Pending |
| COMPAT-01 | Phase 4 | Pending |
| COMPAT-02 | Phase 4 | Pending |
| COMPAT-03 | Phase 4 | Pending |
| COMPAT-04 | Phase 4 | Pending |
| COMPAT-05 | Phase 4 | Pending |
| COMPAT-06 | Phase 4 | Pending |
| COMPAT-07 | Phase 4 | Pending |
| COMPAT-08 | Phase 4 | Pending |
| TEST-03 | Phase 4 | Pending |
| TEST-04 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 46 total
- Mapped to phases: 46
- Unmapped: 0 (verified)

---
*Requirements defined: 2026-05-31*
*Last updated: 2026-05-30 after roadmap creation (traceability corrected to 4-phase structure)*
