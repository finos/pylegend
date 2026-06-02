# Architecture Research: PyLegend 2.0

**Date:** 2026-05-31

## Key Facts About Existing Architecture

1. **Pure generation already works for all operations.** Every `AppliedFunction` subclass already implements `to_pure(config: FrameToPureConfig) -> str`. `filter`, `groupBy`, `extend`, `project`, `sort`, `head`, `slice`, `drop`, `distinct`, `rename`, `join`, `as_of_join`, `concatenate`, `aggregate`, and `window_extend` all produce correct Pure strings today via recursive descent.

2. **SQL path is structurally separate from the Pure path.** Both live as sibling methods (`to_sql(config)` and `to_pure(config)`) in the same `AppliedFunction` classes. Removing SQL does not touch any Pure generation code.

3. **The only Pure gap in 1.x is input frames.** `LegendServiceInputFrameAbstract.to_pure()` and `LegendFunctionInputFrameAbstract.to_pure()` raise `RuntimeError("to_pure is not supported")`. These must be fixed before any end-to-end Pure execution is possible.

## Target Architecture

Five components. Three exist in working form; two are new.

```
┌─────────────────────────────────────────────────┐
│  User surface                                   │
│                                                 │
│  ibis.legend.connect(...)  LegendQLApiTdsClient │
│       [NEW]                    [keep, rewired]  │
└───────────┬─────────────────────┬───────────────┘
            │                     │ thin wrapper
            ▼                     ▼
┌──────────────────────┐  ┌───────────────────────┐
│  Ibis Backend [NEW]  │  │  LegendQL frame chain │
│                      │  │  [keep, rewired]      │
│  Backend             │  │                       │
│  Compiler            │  │  LegendQLApiBase      │
│  (Pure generation    │  │  TdsFrame +           │
│   from Ibis IR)      │  │  AppliedFunction      │
│                      │  │  nodes (to_pure       │
│                      │  │  already works)       │
└─────────┬────────────┘  └───────────┬───────────┘
          │  Pure string              │  Pure string
          └──────────────┬────────────┘
                         ▼
          ┌──────────────────────────┐
          │  LegendClient (HTTP)     │
          │  [keep, +2 new methods]  │
          │                         │
          │  execute_pure_string()   │  ← new
          │  get_pure_string_schema()│  ← new
          └──────────────────────────┘
                         │
                         ▼
           Legend Engine (external)
                         │
                         ▼
          ┌──────────────────────────┐
          │  ResponseReader          │
          │  ResultHandlers          │
          │  [unchanged]             │
          └──────────────────────────┘
```

## Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| Ibis Backend (`pylegend/backends/legend/`) | Registered as `ibis.legend`; `connect()`, `table()`, `execute()`; owns `Compiler` | LegendClient (execute), Ibis framework (entry point) |
| Compiler | Walks Ibis IR node tree; emits Pure string; dispatches on `ibis.expr.operations` types | Called by `Backend.execute()`; reuses `PureExpressionTranslator` for scalars |
| PureExpressionTranslator | Converts Ibis scalar operation nodes to Pure expression strings | Called by Compiler; mirrors existing `to_pure_expression()` logic |
| LegendQL frame chain | Public `LegendQLApiTdsFrame` API; builds Ibis expression tree; delegates Pure to Backend | Ibis Backend (expressions); LegendClient (execute) |
| LegendClient | HTTP client; adds `execute_pure_string()` + `get_pure_string_schema()` | Legend engine (HTTP) |
| ResultHandlers | Parse streaming response → pandas/string/raw | Called by LegendClient; no changes |

## Data Flow

```
User builds Ibis expression:
    t = ibis.legend.connect(...).table("/service/pattern")
    result = t.filter(t.col > 5).group_by("name").agg(count=t.col.count())
        ↓  (lazy, no I/O)
    Ibis IR node tree
        ↓  result.execute()
    Backend.execute(expr)
        ↓  Compiler.translate(expr)
    Pure string:
        #service(pattern='/service/pattern', ...)#
        ->filter({r | $r.col > 5})
        ->groupBy(~[name], ~[count: r|$r.col: c|$c->count()])
        ↓  LegendClient.execute_pure_string(pure_str)
    HTTP POST → Legend engine
        ↓  streaming CSV response
    ResponseReader → ResultHandler
        ↓
    pandas DataFrame
```

LegendQL compatibility path is identical except Pure is produced by the existing `frame.to_pure()` recursive descent (once input frames are fixed).

## Ibis Operation → Pure Mapping

| Ibis operation node | Pure output |
|---------------------|-------------|
| `DatabaseTable` (service input) | `#service(pattern='...', groupId='...', ...)#` |
| `Filter` | `->filter({r \| <expr>})` |
| `Aggregation` (with groupBy) | `->groupBy(~[cols], ~[agg: r\|$r.col: c\|$c->count()])` |
| `Aggregation` (no groupBy) | `->aggregate(~[agg: r\|$r.col: c\|$c->count()])` |
| `Selection` (project/extend) | `->project(~[col: r\|<expr>])` or `->extend(~[...])` |
| `SortKeys` | `->sort(~[col->ascending()])` |
| `Limit` | `->limit(<n>)` |
| `SelfReference` + join | `->join(<right>, <type>, {l,r\|<cond>})` |
| `Union` | `->concatenate(<other>)` |
| `TableColumn` | `$r.<colName>` |
| Scalar ops (eq, add, upper, etc.) | reuse existing `to_pure_expression()` output |

**Scalar expression reuse strategy:** Construct `PyLegendExpression` nodes from Ibis scalar nodes and call existing `to_pure_expression()` directly — avoids duplication and drift. The mapping from Ibis scalar types to PyLegend expression classes is straightforward (e.g., `ops.Equals` → boolean expression with `=` operator).

## Build Order

**Step 1 — Fix Pure input frame generation** (prerequisite for everything)
Implement `to_pure()` on `LegendServiceInputFrameAbstract` and `LegendFunctionInputFrameAbstract`. Verify exact Pure syntax against Legend engine. Unlocks end-to-end Pure execution.

**Step 2 — Add LegendClient Pure execution endpoint**
Add `execute_pure_string()` and `get_pure_string_schema()` to `LegendClient`. Determine correct engine API routes. Migrate `BaseTdsFrame.execute_frame()` to call `execute_pure_string()`.

**Step 3 — PCT green checkpoint**
LegendQL frame chain is now Pure-backed. Run PCT tests. First green checkpoint validates Pure generation is correct before Ibis work begins.

**Step 4 — Remove Legacy and Pandas APIs + SQL layer**
Safe to remove once Step 3 is green. Deletes `core/tds/legacy_api/`, `core/tds/pandas_api/`, `core/language/legacy_api/`, `core/language/pandas_api/`, `core/sql/`, `core/database/`, `extensions/database/vendors/`.

**Step 5 — Ibis backend skeleton**
Create `pylegend/backends/legend/` with `Backend` class. Add entry point to `pyproject.toml`. Implement `connect()`, `do_connect()`, `table()`. `ibis.legend.connect(...)` now works; `execute()` raises `NotImplementedError`.

**Step 6 — Ibis compiler, operation by operation**
Implement `Compiler` dispatching on Ibis IR nodes. Sub-order within this step (simplest → hardest):
1. Table scan (service/function input)
2. Filter
3. Select / project / rename / restrict
4. Limit / slice / drop
5. Sort
6. Aggregate (whole-table)
7. GroupBy + aggregate
8. Extend (computed columns)
9. Joins (inner, left, right, full)
10. AsOfJoin
11. Concatenate
12. Window extend

Each operation should have a unit test comparing output against existing `to_pure()` test output — those are the ground truth.

**Step 7 — Rewire LegendQL API as Ibis wrapper**
Replace `LegendQLApiBaseTdsFrame` operation implementations: each method now builds the Ibis expression. `to_pure_query()` calls `Backend.compile(ibis_expr)`. Public interface unchanged.

**Step 8 — Remove dead code**
Remove `FrameToSqlConfig`, `SqlToStringGenerator`, all `to_sql_query_object()` implementations, all `AppliedFunction.to_sql()` methods, and `AppliedFunction.to_pure()` recursive descent (superseded by Ibis compiler).

## Migration Path Summary

Steps 1–6 are additive — both LegendQL chain and Ibis backend coexist. No user-visible changes.
Step 7 is the cut-over — LegendQL internal wiring changes; public interface unchanged.
Step 8 is housekeeping.

**Backwards compatibility surface to preserve:**
- `LegendQLApiTdsFrame` method names and signatures
- `TdsColumn` types returned by all operations
- `LegendQLApiTdsClient.legend_service_frame()` and `legend_function_frame()` signatures
- `LegendClient`, `AuthScheme`, `HeaderTokenAuthScheme` public interfaces
- `ResultHandler` and the three built-in handlers

## Open Questions (must resolve in Phase 1)

1. **Legend engine Pure execution endpoint** — what HTTP route and request body for executing a Pure TDS query? (`LegendClient` uses `sql/v1/execution/execute` for SQL; Pure may differ)
2. **Service input Pure syntax** — exact form of the service input Pure expression; how `ProjectCoordinates` map to Pure named arguments
3. **Column schema retrieval without SQL** — after SQL layer removal, `__init__()` can no longer call `get_sql_string_schema(self.to_sql_query())`; must use different endpoint
4. **PCT test wiring** — how PyLegend participates in the Legend PCT matrix
5. **Ibis version constraints** — verify `BaseBackend` contract against specific Ibis version chosen
