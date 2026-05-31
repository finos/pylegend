# Stack Research: PyLegend 2.0

**Date:** 2026-05-31
**Domain:** Ibis backend + Pure generation for FINOS Legend

## Context: What the Codebase Already Has

- Python 3.9–3.14, uv package manager, `uv_build>=0.11.2,<0.12.0` build backend
- `requests>=2.27.1` — all HTTP to Legend engine (auth, retry, streaming in `ServiceClient`)
- `ijson>=3.1.4` — streaming JSON parsing for large responses
- `pandas + numpy` — DataFrame result handling (currently runtime deps)
- `testcontainers>=3.0.0` — Docker-based Legend engine for integration tests (currently a runtime dep but should be dev-only)
- Pure generation machinery already exists in `LegendQLApiBaseTdsFrame` and its function subclasses

## Recommended Stack

### Core Dependencies

**ibis-framework — ADD**

Version: `>=9.0,<10` (verify current stable at pypi.org before pinning)

Confidence: MEDIUM on exact version; HIGH on the requirement itself.

Registration pattern from `pyproject.toml`:
```toml
[project.entry-points."ibis.backends"]
legend = "pylegend.ibis_backend:Backend"
```

**requests >=2.27.1 — KEEP**

All auth schemes (HeaderTokenAuth, CookieAuth, LocalhostEmpty), retry logic, and streaming live in `LegendClient`. No reason to replace.

**ijson >=3.1.4 — KEEP**

Streaming JSON for large response bodies.

**pandas + numpy — KEEP as runtime deps**

Two viable options: keep as runtime deps (simplest, backward-compatible) or declare `pylegend[pandas]` optional extra. Option 1 is lower risk for 2.0.

**testcontainers >=3.0.0 — MOVE to dev group**

Currently a runtime dependency. Only imported in `pylegend/samples/local_legend_env.py`. Library users should not require Docker.

### Ibis Backend Protocol — What is Required

An Ibis backend must:

1. **Subclass `ibis.backends.base.BaseBackend`** (NOT `BaseSQLBackend` — Pure is not SQL; `BaseSQLBackend` adds SQLGlot coupling you'd need to fight)

2. **Required methods:**
   - `name` property → returns `"legend"`
   - `do_connect(host, port, secure_http, auth_scheme, ...)` → initializes `LegendClient`
   - `get_schema(table_name, ...)` → calls Legend API and converts `TdsColumn` list to `ibis.Schema`
   - `table(name, ...)` → returns an `ibis.Table` backed by a Legend service or function
   - `execute(expr, ...)` → compiles `expr` to Pure via custom compiler, POSTs to engine, returns DataFrame

3. **A custom Pure compiler** using the Ibis visitor/translator pattern that walks `ibis.expr.operations` nodes and emits Pure AST fragments

### Ibis op → Pure generator mapping

| Ibis operation | Existing Pure generator |
|---|---|
| `Filter` | `LegendQLApiFilterFunction.to_pure()` |
| `Aggregation` / `Reduction` | `LegendQLApiGroupByFunction.to_pure()` |
| `Projection` | `LegendQLApiProjectFunction.to_pure()` / `LegendQLApiExtendFunction.to_pure()` |
| `Limit` | `LegendQLApiHeadFunction.to_pure()` |
| `SortKey` / `Sort` | `LegendQLApiSortFunction.to_pure()` |
| `JoinChain` | `LegendQLApiJoinFunction.to_pure()` |
| Window functions | `LegendQLApiWindowExtendFunction.to_pure()` |
| AsOf join | `LegendQLApiAsOfJoinFunction.to_pure()` — no direct Ibis equivalent; needs investigation |

## Key Decisions

1. **Subclass `BaseBackend`, not `BaseSQLBackend`** — Pure is not SQL
2. **Keep `LegendClient` as the execution layer** — auth, retry, streaming all live there
3. **Ibis compiler produces Pure identical to existing `to_pure()` output** — use existing implementations as specification
4. **Pure generation is the only output target** — no SQL from the Ibis backend
5. **No async in 2.0** — synchronous API throughout

## What NOT to Use

- **SQLGlot for Pure compilation** — SQL dialect translator; Pure has fundamentally different syntax
- **`BaseSQLBackend`** — SQL-specific; adds unwanted constraints
- **httpx/aiohttp or any new HTTP client** — `LegendClient` is correct and tested
- **sqlalchemy for schema introspection** — existing `get_sql_string_schema()` endpoint already returns TDS column metadata

## pyproject.toml Changes Required

```toml
dependencies = [
    "ibis-framework>=9.0,<10",          # ADD
    "requests>=2.27.1",                  # KEEP
    "ijson>=3.1.4",                      # KEEP
    "pandas>=1.0.0 ; python_full_version < '3.12'",   # KEEP
    "pandas>=2.1.1 ; python_full_version >= '3.12'",  # KEEP
    "numpy>=1.20.0 ; python_full_version < '3.12'",   # KEEP
    "numpy>=1.26.0 ; python_full_version >= '3.12'",  # KEEP
    # testcontainers — MOVE to dev
]

[project.entry-points."ibis.backends"]
legend = "pylegend.ibis_backend:Backend"

[dependency-groups]
dev = [
    "pytest>=7.0.0",
    "pytest-cov>=3.0.0",
    "testcontainers>=3.0.0",   # MOVED from runtime
    # Remove when SQL layer is deleted: sqlalchemy, pg8000, pymysql, cryptography
    # Remove when Legacy/Pandas APIs are deleted: mockito
]
```

## Open Questions

1. **Current ibis-framework stable version** — verify at pypi.org before finalising version range
2. **asOfJoin in Ibis** — does ibis-framework have a temporal join? If not, needs custom `ibis.expr.operations.Node`
3. **Window functions with duration ranges** — `LegendQLApiWindowFrame` supports `DurationUnit`; Ibis window frames may not have direct equivalent
4. **PCT test wiring** — how Legend PCT matrix exercises PyLegend determines what shape `execute()` needs
5. **Schema population without SQL** — after SQL layer removal, must use a different Legend API endpoint for schema
