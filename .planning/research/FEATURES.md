# Features Research: PyLegend 2.0

**Date:** 2026-05-31

## Table Stakes

### Ibis Backend Protocol

| Feature | Why Required | Complexity |
|---------|--------------|------------|
| `Backend` class extending `BaseBackend` | Ibis dispatches all compilation/execution through the backend class | Low |
| `do_connect(host, port, auth, ...)` | Called when `ibis.legend.connect(...)` is invoked | Low |
| Entry-point in `pyproject.toml` | Ibis discovers backends via `ibis.backends` group; without it `ibis.legend` raises `AttributeError` | Low |
| `table(name, ...)` | Returns Ibis `Table` expression bound to a Legend service/function | Medium |
| `execute(expr, ...)` | Compiles Ibis expr to Pure, POSTs to engine, returns DataFrame | High |
| Schema at construction time | Ibis requires `Schema` at expression-build time; currently done via `get_sql_string_schema()` | Medium |

### TdsFrame Operations (must keep working)

**Row operations:**

| Operation | LegendQL Method | Pure Function | Complexity |
|-----------|----------------|---------------|------------|
| Filter | `filter(lambda r: ...)` | `->filter(...)` | Medium |
| Head / Limit | `head(n)` / `limit(n)` | `->take(n)` | Low |
| Drop | `drop(n)` | `->drop(n)` | Low |
| Slice | `slice(start, end_exclusive)` | `->slice(start, end)` | Low |

**Column operations:**

| Operation | LegendQL Method | Pure Function | Complexity |
|-----------|----------------|---------------|------------|
| Select / Restrict | `select(cols)` | `->restrict(~[...])` | Low |
| Rename | `rename([(old, new)])` | `->renameColumns(~[...])` | Low |
| Extend | `extend([(name, lambda)])` | `->extend(~[...])` | Medium |
| Project | `project([(name, lambda)])` | `->project(~[...])` | Medium |
| Cast | `cast({col: type})` | type annotation at column level | High |
| Distinct | `distinct(cols=None)` | `->distinct()` | Low |

**Aggregation:**

| Operation | LegendQL Method | Pure Function | Complexity |
|-----------|----------------|---------------|------------|
| GroupBy + Aggregate | `group_by(cols, agg_specs)` | `->groupBy(~[...], ~[...])` | High |
| Global Aggregate | `aggregate(agg_specs)` | `->aggregate(~[...])` | High |

Aggregate functions: `count`, `sum`, `avg/average`, `min`, `max`, `distinct_count`, `std_dev_sample`, `variance_sample`

**Joins:**

| Operation | LegendQL Method | Pure Function | Complexity |
|-----------|----------------|---------------|------------|
| Inner Join | `inner_join(other, cond)` | `->join(other, JoinType.INNER, ...)` | High |
| Left Outer Join | `left_join(other, cond)` | `->join(other, JoinType.LEFT_OUTER, ...)` | High |
| Right Outer Join | `right_join(other, cond)` | `->join(other, JoinType.RIGHT_OUTER, ...)` | High |
| Full Outer Join | `full_join(other, cond)` | `->join(other, JoinType.FULL, ...)` | High |
| As-Of Join | `as_of_join(other, match_fn, join_cond)` | `->asOfJoin(...)` | High — Pure-only; no SQL equivalent |

**Sort / Set:**

| Operation | LegendQL Method | Pure Function | Complexity |
|-----------|----------------|---------------|------------|
| Sort | `sort(cols_or_lambda)` | `->sort(~[...])` | Medium |
| Concatenate | `concatenate(other)` | `->concatenate(other)` | Medium |

**Window functions:**

| Operation | LegendQL Method | Pure Function | Complexity |
|-----------|----------------|---------------|------------|
| Window spec | `window(partition_by, order_by, frame)` | `->olapGroupBy(...)` | High |
| Window extend | `window_extend(window, extend_cols)` | `->olapGroupBy(...)` | High |
| Row-based frame | `rows(start, end)` | ROWS frame | Medium |
| Range-based frame | `range(number_start=..., duration_start=...)` | RANGE frame | High — requires `DurationUnit` mapping |

Window functions in `window_extend`: `row_number`, `rank`, `dense_rank`, `lead`, `lag`, plus aggregates over the window.

**Input frame types:**

| Frame Type | Complexity |
|------------|------------|
| Legend Service frame | Medium |
| Legend Function frame | Medium |
| CSV input frame | Low |
| Table spec frame | Low — test-only scaffold |

### Execution Path

| Feature | Complexity |
|---------|------------|
| Pure string generation | High (all operations × all expression variants) |
| HTTP execution via `LegendClient` | Medium — keep existing auth schemes |
| Streaming result handlers (CSV, Pandas, string) | Low — no changes |
| `to_pandas()` / `to_pandas_df()` aliases | Low |
| `to_pure_query()` on TdsFrame | Medium — must produce same output |

## Differentiators

### LegendQL API as Backwards-Compatible Wrapper

- `LegendQLApiTdsClient` delegates to Ibis backend — internal library teams make zero code changes
- `TdsFrame.to_pure_query()` routes through Ibis compiler — same Pure output as before

### Legend-Native Ibis Entry Points

- `backend.legend_service(pattern, coords)` — Ibis-idiomatic query root at a Legend service
- `backend.legend_function(path, coords)` — query root at a Pure function

## Anti-Features (deliberately out of scope for v1)

| Anti-Feature | Why |
|--------------|-----|
| SQL generation from Ibis backend | PROJECT.md: Pure only |
| Full `ibis-backends` test suite compliance | Legend/Pure is more restricted; defer full compliance |
| New TdsFrame operations beyond 1.x scope | Explicitly out of scope in PROJECT.md |
| Pure output formatting config | Keep generation simple; no pretty-print/indent |
| Pandas API | Being removed |
| Legacy API | Being removed |
| Async execution | Adds risk during major refactor; defer |
| Arrow / JSON streaming handlers | Not in current feature set |
| DML (INSERT/UPDATE/DELETE) | Legend engine is query-only from Python |

## The Six Hard Problems

1. **Join column disambiguation** — overlapping column names require explicit renaming before join; must enforce at expression-build time
2. **Window functions — three-argument lambda** — `window_extend` uses `(partial_frame, window_ref, row)`; no direct Ibis analytic equivalent; compile directly to Pure `olapGroupBy`
3. **Cast / type annotation** — Pure is strongly typed; `cast()` changes declared column type; schema propagation problem
4. **As-of join — Legend-specific semantics** — no Ibis join analogue; compile directly to Pure without routing through Ibis join semantics
5. **Schema-at-construction-time** — `columns()` must return correct `TdsColumn` list immediately after construction, before any query execution
6. **Range-based window frames with duration units** — `DurationUnit.DAYS` etc. map to Pure `Duration` enum; no SQL or standard Ibis equivalent
