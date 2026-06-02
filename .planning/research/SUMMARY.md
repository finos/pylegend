# Research Summary: PyLegend 2.0

**Date:** 2026-05-31
**Confidence:** MEDIUM-HIGH

## Executive Summary

PyLegend 2.0 replaces a three-layer architecture (LegendQL + Legacy + Pandas APIs, all compiling to a SQL metamodel) with a single Ibis backend (`ibis.legend`) that compiles directly to Pure — the Legend platform's native functional query language. The core insight: the Pure generation path already works for every operation today; the SQL path is unused complexity that was never needed.

The recommended approach is strictly additive-then-cut-over: fix the two broken Pure input frame roots first (they raise `RuntimeError` on `to_pure()` today), wire end-to-end Pure execution through `LegendClient`, confirm the PCT test matrix is green, then build the Ibis backend alongside existing code. The LegendQL API is rewired as a thin Ibis wrapper only after the backend is validated.

## Stack Recommendation

**Add:**
- `ibis-framework>=9.0,<10` — core new dependency (verify version at pypi.org)
- Entry point: `[project.entry-points."ibis.backends"] legend = "pylegend.ibis_backend:Backend"`

**Keep:**
- `requests>=2.27.1` — all Legend HTTP, auth, retry, streaming
- `ijson>=3.1.4` — streaming JSON for large responses
- `pandas + numpy` — keep as runtime deps (simplest path for 2.0)

**Move to dev:**
- `testcontainers` — only used in `local_legend_env.py`; library users shouldn't require Docker

**Remove (when SQL layer deleted):**
- `sqlalchemy`, `pg8000`, `pymysql`, `cryptography` — SQL layer test dependencies only

**Remove (when Legacy/Pandas APIs deleted):**
- `mockito` — used for Legacy/Pandas API unit tests

**Backend base class:** `ibis.backends.base.BaseBackend` directly — NOT `BaseSQLBackend`. Pure is not SQL; `BaseSQLBackend` adds SQLGlot coupling you'd need to fight.

## Table Stakes Features

1. **Pure input frames working** — `LegendServiceInputFrame.to_pure()` and `LegendFunctionInputFrame.to_pure()` (currently raise `RuntimeError`)
2. **LegendClient Pure execution** — `execute_pure_string()` and `get_pure_string_schema()` methods
3. **Full TdsFrame operation coverage in compiler** — filter, project, extend, rename, restrict, limit/slice/drop, sort, aggregate, groupBy, all join types, concatenate, window extend
4. **LegendQL API public interface frozen** — method signatures, `TdsColumn` types, `LegendQLApiTdsClient` entry points unchanged
5. **PCT matrix green** — end-to-end Pure execution must pass existing test suite

## Architecture Overview

Five components: Ibis Backend (new), Pure Compiler (new), LegendQL frame chain (keep, rewired internally), LegendClient (keep, +2 methods), ResultHandlers (unchanged). Both the Ibis compiler and the existing `to_pure()` recursive descent converge to the same `LegendClient.execute_pure_string()` call.

**Build order (dependency-driven):**
1. Fix input frame `to_pure()` roots
2. Add `execute_pure_string()` + `get_pure_string_schema()` to `LegendClient`
3. PCT green checkpoint
4. Remove Legacy, Pandas, SQL layers
5. Ibis backend skeleton + type mapping
6. Compiler — operation by operation (table scan → filter → select → limit → sort → aggregate → groupBy → extend → joins → as_of_join → concatenate → window_extend)
7. Rewire LegendQL API as Ibis wrapper
8. Delete dead code

## Critical Pitfalls

1. **Input frame `to_pure()` raises `RuntimeError` (PG-1)** — Fix before any other work. Both service and function input frames are broken. These are the root of every query tree.

2. **Eager SQL import causes `ImportError` at module load (BC-4)** — `tds_frame.py` eagerly imports the Postgres SQL extension. Removal must be in the same commit as the SQL layer deletion.

3. **Wrong Ibis base class (IB-1)** — Inheriting from `SQLGlotBackend` or `BaseSQLBackend` adds SQL-specific machinery. Use `BaseBackend` directly.

4. **Legend-specific ops have no Ibis node equivalent (IB-4)** — `as_of_join`, window functions with duration ranges, global `aggregate`, and `cast` require custom `ibis.expr.operations.Node` subclasses or direct Pure emission. Enumerate all `LegendQLApiBaseTdsFrame` methods and classify before writing compiler code.

5. **String formatting instead of DAG visitor (IB-5/IB-6)** — Implement the compiler as a `visit_<NodeName>` dispatcher. String concatenation breaks parenthesisation and lambda scoping for nested operations.

6. **Return type contract broken in wrapper (BC-1)** — Internal libraries do `isinstance(frame.filter(...), LegendQLApiTdsFrame)`. If any operation returns a different concrete type, it silently breaks. Every operation must return a `LegendQLApiTdsFrame` subclass.

7. **`project_cooridnates.py` typo is frozen** — Part of the public import surface. Do not rename in 2.0.

## Open Questions (must resolve in Phase 1)

1. **Legend engine Pure execution HTTP endpoint** — Most critical unknown. What is the route and request body for executing a Pure TDS query? (`LegendClient` uses `sql/v1/execution/execute` for SQL; Pure may be different.) Blocks all execution work.
2. **Service input Pure syntax** — Exact form of the Pure root expression for a Legend service. How `ProjectCoordinates` (groupId, artifactId, versionId) appear in the Pure grammar.
3. **Schema retrieval without SQL** — After SQL layer removal, `__init__()` can no longer call `get_sql_string_schema(self.to_sql_query())`. Must use a different Legend API endpoint.
4. **PCT test wiring** — How PyLegend participates in the Legend PCT matrix. Must be understood before touching `legend_test_server` fixture.
5. **ibis-framework stable version** — Verify `>=9.0,<10` range at pypi.org before finalising.

## Suggested Phase Structure

| Phase | Focus | Key Output |
|-------|-------|-----------|
| 1 | Fix Pure foundation + wire execution | `to_pure()` on input frames; `execute_pure_string()`; PCT green |
| 2 | Remove Legacy, Pandas, SQL layers | Codebase simplified; SQL metamodel gone |
| 3 | Ibis backend skeleton + type system | `ibis.legend.connect()` working; Legend↔Ibis type mapping |
| 4 | Ibis compiler — all operations | `Backend.execute()` working end-to-end |
| 5 | Rewire LegendQL API as Ibis wrapper | LegendQL fully backed by Ibis; `to_sql_query()` deprecated |
| 6 | Cleanup + integration validation | Dead code deleted; `as_of_join` engine-tested |

Phases needing deeper research during planning: 1 (Pure HTTP endpoint), 3 (BaseBackend contract), 4 (custom Ibis nodes for Legend-specific ops).
Phases with standard patterns (can skip pre-research): 2, 6.

---
*Research completed: 2026-05-31*
