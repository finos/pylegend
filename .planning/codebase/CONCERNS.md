# Concerns & Technical Debt

**Analysis Date:** 2026-05-31

## Technical Debt

### Unimplemented SQL Features
**Location:** `pylegend/core/database/sql_to_string/db_extension.py`

- **Line 869** — `# TODO: Handle distinct and filter` in aggregate function processing; `DISTINCT` and `FILTER` clauses in aggregates are silently ignored
- **Line 979** — `# TODO: Use limit, orderBy, offset at query level`; these clauses force a wrapping subquery (`SELECT * FROM (...)`) instead of being applied at the top level — generates unnecessarily verbose SQL
- **Line 1016** — `# TODO: code this` in `join_using_processor`; `JOIN ... USING (col)` raises `RuntimeError("Not supported yet!")` at runtime
- **Line 1171** — `# TODO: check quoted flag` in identifier processing; quoting behavior may be inconsistent

### Typo in Module Path (Widespread)
`project_cooridnates.py` (misspelled "coordinates") is imported by ~10 files. Renaming would break public API unless done carefully.

Files affected:
- `pylegend/core/project_cooridnates.py`
- `pylegend/legacy_api_tds_client.py`
- `pylegend/legendql_api_tds_client.py`
- `pylegend/__init__.py`
- `pylegend/extensions/tds/**` (6 files)

### `type: ignore` Suppressions
Several `# type: ignore` comments indicate unresolved type inference issues:

- `pylegend/core/language/pandas_api/pandas_api_series.py` (lines 1481–1553) — multiple Series subclass definitions suppress MRO/type errors
- `pylegend/core/language/shared/primitive_collection.py` (lines 225–403) — aggregate return types suppressed; underlying issue is covariant return types not expressible in current mypy
- `pylegend/core/database/sql_to_string/generator.py:55` — dynamic subclass discovery suppresses type check
- `pylegend/core/language/pandas_api/pandas_api_groupby_series.py:967` — `transform` override suppresses signature mismatch

## Test Infrastructure Fragility

### Java Server Dependency
`tests/conftest.py` starts a Java-based Legend SQL Server subprocess via `JAVA_HOME`. Tests requiring the live server fail silently or hang if:
- `JAVA_HOME` is not set
- The JAR build step (`mvn`) hasn't been run
- The server port conflicts

Retry logic (15 attempts × 4 seconds = 60s max) masks slow-startup issues but doesn't surface clear errors.

### Test Resource JAR
`tests/resources/legend/server/` contains a pre-built Legend SQL Server JAR. This must be kept in sync with the Legend server version tested against; version drift causes silent test failures.

## Single Database Vendor
Only PostgreSQL SQL generation is implemented (`pylegend/extensions/database/vendors/postgres/`). The architecture supports additional vendors, but `SqlToStringGenerator.find_sql_to_string_generator_for_db_type()` will raise an error for any other DB type.

## Limited Result Handler Coverage
Result handlers exist for CSV and Pandas DataFrame. Additional formats (JSON, Arrow, etc.) are not implemented; users needing them must implement `ResultHandler` themselves.

## No Async Support
All HTTP requests to the Legend server are synchronous (`requests` library). Large result sets or high-concurrency workloads have no async path; blocking calls can stall the event loop in async Python applications.

## Performance

### Subquery Wrapping for Limit/OrderBy/Offset
As noted above (`db_extension.py:979`), queries with `LIMIT`, `ORDER BY`, or `OFFSET` are wrapped in an extra `SELECT * FROM (...)`, adding unnecessary query nesting and potentially confusing query planners.

### Streaming Response Parsing
`ijson` is used for streaming JSON parsing (good), but CSV response parsing via the result handler reads line-by-line; very wide schemas or many columns may cause memory pressure.

## Security

- `HeaderTokenAuthScheme` passes tokens via callable — token is re-fetched per request (good for rotation)
- No input sanitization concerns found at the query-building layer (SQL is built via AST, not string concatenation)
- No hardcoded credentials found in source
