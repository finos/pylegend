# Pitfalls Research: PyLegend 2.0

**Date:** 2026-05-31

## Critical Discoveries (Must Act On)

- **Five input frames currently raise `RuntimeError` on `to_pure()`** — `LegendServiceInputFrame`, `LegendFunctionInputFrame` are the roots of every query tree. Fix before any compiler work.
- **`tds_frame.py` has an eager import of the Postgres SQL extension at module load time** — removing the SQL layer without removing this import will cause `ImportError` at `import pylegend`.
- **The internal library subclasses concrete frame types** — every operation must return an instance that is still a `LegendQLApiTdsFrame` subclass or `isinstance` checks in downstream libraries silently break.
- **`as_of_join.to_sql()` already raises `RuntimeError`** — `to_pure()` exists but has never been validated against a real engine.
- **The JAR-based `legend_test_server` conftest fixture is the only execution path for integration tests** — must remain alive until a replacement Pure-execution fixture exists.

## Ibis Backend Implementation Pitfalls

**IB-1: Wrong base class for a non-SQL backend**
- What goes wrong: Inheriting from `SQLGlotBackend` pulls in schema machinery that assumes SQL `INFORMATION_SCHEMA` queries.
- Prevention: Start from `ibis.backends.base.BaseBackend` directly. The `ibis-pandas` reference is the canonical example.
- Phase: Phase 2 (backend scaffolding)

**IB-2: Missing or incorrect entry_point registration**
- What goes wrong: `ibis.legend` is discovered via `[project.entry-points."ibis.backends"]`. If absent, `ibis.legend` silently fails with `AttributeError`. Entry_points are cached at process startup — reinstall required after adding.
- Prevention: Add the entry_point in the first commit. Smoke test: `import ibis; assert hasattr(ibis, 'legend')`.
- Phase: Phase 2

**IB-3: Schema resolution not implemented at `Backend.table()` time**
- What goes wrong: If schema discovery is skipped, `frame.columns()` returns `[]` and join/filter validation passes vacuously — generating Pure for a 0-column frame that the engine rejects.
- Prevention: In `Backend.table()`, call the Legend HTTP API to retrieve schema; construct explicit `ibis.Schema`. Assert `len(frame.columns()) > 0` after construction.
- Phase: Phase 2–3

**IB-4: Assuming Ibis operations map 1:1 to Pure TDS functions**
- What goes wrong: `as_of_join`, `window_extend` with duration ranges, `aggregate` without `group_by`, and `cast` have no canonical Ibis node. Without custom nodes, these fall through to "not implemented" at execution time, not query-construction time.
- Prevention: Enumerate all `LegendQLApiBaseTdsFrame` methods before writing any compiler code. Classify each as: (a) maps to existing Ibis node, (b) requires custom node, (c) raises `OperationNotDefinedError`. Implement custom nodes for (b) first.
- Phase: Phase 2

**IB-5: Building the Pure compiler by string formatting rather than a DAG visitor**
- What goes wrong: String concatenation breaks parenthesisation and lambda scoping for 3+ levels of nesting.
- Prevention: Implement the compiler as a visitor (similar to `SQLGlotCompiler` pattern). Each node type gets a `visit_<NodeName>` method returning a Pure fragment.
- Warning sign: `filter().group_by()` works but `filter().join(other, ...).group_by()` fails.
- Phase: Phase 2

**IB-6: Lambda variable naming collisions in nested Pure expressions**
- What goes wrong: Pure TDS functions use lambda parameters (`$x` for filter, `$l`/`$r` for join, `$c` for aggregate). Nested operations can shadow variables.
- Prevention: Use conventional names by function type (verify against existing `to_pure()` snapshots). Or use depth-indexed names (`$x0`, `$x1`).
- Warning sign: `filter().filter()` or `join().filter()` generates Pure that the engine rejects.
- Phase: Phase 2

## API Migration / Backwards Compat Pitfalls

**BC-1: Changing the concrete type returned by frame operations**
- What goes wrong: The internal library subclasses `LegendQLApiTdsFrame`. If operations return a different concrete type, `isinstance` checks and `super()` calls in the internal library break silently.
- Prevention: Every operation must return an instance that is still a `LegendQLApiTdsFrame` subclass. Audit the MRO carefully.
- Warning sign: `isinstance(frame.filter(...), LegendQLApiTdsFrame)` returns `False`.
- Phase: Phase 3 (wrapper)

**BC-2: `columns()` populated with wrong names or types**
- What goes wrong: Ibis uses names like `int64` and `float64`. `TdsColumn` uses Legend names (`"Integer"`, `"Float"`, `"Number"`). Incomplete mapping causes validators to reject valid columns.
- Prevention: Build an explicit bidirectional type-mapping table. Test every type in the PyLegend test suite. Never rely on `str(ibis_dtype)`.
- Phase: Phase 2–3

**BC-3: Hard-removing `to_sql_query()` without deprecation**
- What goes wrong: `to_sql_query()` is abstract on `PyLegendTdsFrame`. Internal library may call it. Hard removal causes `RuntimeError`/`NotImplementedError`.
- Prevention: Add `DeprecationWarning` implementation before hard removal. Communicate in 2.0 release notes.
- Phase: Phase 3–4

**BC-4: Eager import of the Postgres SQL extension at module load**
- What goes wrong: `tds_frame.py` eagerly imports the Postgres extension module. Removing the SQL extension without removing this import causes `ImportError` at `import pylegend`.
- Prevention: Remove the eager import in the same commit that removes the SQL extension. Run `grep -r "FrameToSqlConfig\|to_sql_query" pylegend/` before committing.
- Warning sign: `import pylegend` raises `ModuleNotFoundError` after partial SQL removal.
- Phase: Phase 1 or early Phase 2 — must be coordinated

**BC-5: Renaming `project_cooridnates.py` (the typo) during the refactor**
- What goes wrong: The misspelled module path `pylegend.core.project_cooridnates` is part of the public import surface. Any rename breaks the internal library's import path.
- Prevention: Do not rename this module in 2.0. It is frozen.
- Phase: All phases — treat as frozen

**BC-6: Removing the JAR-based test server before the Pure execution path is wired**
- What goes wrong: `legend_test_server` fixture starts the Java JAR — the only integration test execution path. Removing it before the Ibis backend can execute queries makes the PCT matrix entirely red.
- Prevention: Keep the JAR fixture alive until there is a replacement fixture.
- Phase: Phase 1 — must be planned before API removal

## Pure Generation Pitfalls

**PG-1: Input frames raise `RuntimeError` on `to_pure()` — they are the query roots**
- What goes wrong: `LegendServiceInputFrameAbstract.to_pure()` and `LegendFunctionInputFrameAbstract.to_pure()` both raise `RuntimeError`. In 2.0, Pure generation is execution.
- Prevention: Fix both before any compiler work. Verify correct Pure root expression syntax against Legend documentation. Add unit tests for root frame Pure output independently.
- Phase: Phase 1 — must be done first

**PG-2: `TableSpecInputFrame` Pure output is non-executable against a Legend engine**
- What goes wrong: Returns `f"#Table({'.'.join(self.table.parts)})#"` — a SQL relational store reference; not valid Pure without a configured relational store.
- Prevention: Document as test-only scaffold. Add guard so `execute_frame()` cannot be called on it.
- Phase: Phase 4 (test cleanup)

**PG-3: `as_of_join` Pure output has never been validated against a real engine**
- What goes wrong: `to_sql()` raises `RuntimeError`; `to_pure()` exists but has no integration test coverage. May be syntactically correct but semantically wrong.
- Prevention: Require at least one integration test for `as_of_join` before 2.0 ships. Mark as `xfail` if engine is unavailable.
- Phase: Phase 2 (compiler) and Phase 3 (integration)

**PG-4: Type mapping between Legend types and Ibis types loses precision**
- What goes wrong: Legend's `Number` (abstract supertype) has no direct Ibis equivalent. `StrictDate` vs `date` (timezone handling). `Decimal` requires precision/scale in Ibis but not in Legend.
- Prevention: Build and test the type-mapping table in isolation as a standalone module before integrating. Verify the complete round-trip for all Legend primitive types.
- Phase: Phase 2

**PG-5: Column names with spaces not quoted in Pure lambda expressions**
- What goes wrong: Column names like `"Ship Name"` require Pure-compatible quoting (`$x.'Ship Name'`). The Ibis compiler must route through `escape_column_name()` — Ibis normalises column names to Python identifiers internally.
- Prevention: In the compiler's column-reference generation, always use `TdsColumn.get_name()` passed through `escape_column_name()`. Never derive names from Ibis's internal representation.
- Phase: Phase 2 (compiler). Caught by existing test suite in Phase 3 if column-name tests run end-to-end.

## Phase-Specific Warnings Table

| Phase | Pitfall | Mitigation |
|-------|---------|------------|
| Phase 1: Remove Legacy/Pandas | BC-4 — Eager SQL import | Remove `importlib.import_module(postgres_ext)` in the same commit as SQL layer removal |
| Phase 1: PCT matrix | BC-6 — JAR test server | Do not touch `legend_test_server` until Ibis execution has a replacement |
| Phase 1: Input frames | PG-1 — `to_pure()` raises on roots | Fix service/function input frame `to_pure()` before any compiler work |
| Phase 2: Backend scaffolding | IB-1, IB-2 — Base class + entry_point | Start from `BaseBackend`; add entry_point on day one |
| Phase 2: Schema discovery | IB-3, BC-2 — Empty `columns()` | Call Legend API in `Backend.table()`; assert `len(columns) > 0` |
| Phase 2: Compiler | IB-5, IB-6 — String vs DAG visitor | Implement visitor; use depth-indexed lambda variable names |
| Phase 2: Type system | BC-2, PG-4 — Legend↔Ibis type mapping | Standalone type-mapping module with round-trip unit tests before compiler |
| Phase 2: Operation coverage | IB-4 — Missing Legend-specific ops | Enumerate all `LegendQLApiBaseTdsFrame` methods; classify before coding |
| Phase 3: Wrapper | BC-1 — Return type contract | All wrapper operations must return `LegendQLApiTdsFrame` subclass instances |
| Phase 3: Column escaping | PG-5 — Spaces in column names | Route all column references through `escape_column_name()` |
| Phase 3: API surface | BC-3 — Hard removal of `to_sql_query` | `DeprecationWarning` first; audit external usage |
| Phase 3: Module naming | BC-5 — `project_cooridnates` typo | Freeze this module name for all of 2.0 |
| Phase 4: Integration tests | PG-3 — `as_of_join` unvalidated | Require engine integration test; `xfail` if unavailable |
| Phase 4: Test cleanup | PG-2 — `TableSpecInputFrame` in non-mocked tests | Add `NonExecutable` guard; document as test-only scaffold |
