# Phase 1: Fix Pure Foundation - Context

**Gathered:** 2026-05-30
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix the broken Pure generation roots for Legend service and function input frames, add Pure execution methods to `LegendClient`, wire the LegendQL API to use Pure instead of SQL as its compilation target, and confirm the PCT test matrix is green.

</domain>

<decisions>
## Implementation Decisions

### Pure Root Expression Format
- **D-01:** Format is researcher-discovered — look at Legend engine source (GitHub), existing `to_pure_query()` test output patterns, and the test model JSON in `tests/resources/legend/metadata/`.
- **D-02:** The internal library that extends PyLegend provides its own `to_pure()` roots — it does not use `LegendServiceInputFrame.to_pure()` or `LegendFunctionInputFrame.to_pure()` in production. The fixed root implementations only need to produce valid Pure for PyLegend's own integration test suite.

### Test Server and Pure HTTP Endpoint
- **D-03:** Researcher investigates first — discover what endpoint the Legend engine already exposes for Pure TDS execution and schema retrieval before deciding whether `PyLegendSqlServer.java` needs modification.
- **D-04:** Maven is not installed locally. Installing Maven via `pixi global install maven` is a Phase 1 prerequisite for building the test server JAR and running integration tests.
- **D-05:** Local Java is at `/Users/deepyaman/Library/Caches/rattler/cache/pkgs/openjdk-17.0.17-h99a4030_0/lib/jvm` — set `JAVA_HOME` to this path when running integration tests locally.

### PCT Scope
- **D-06:** There is a separate FINOS Legend PCT (Protocol Conformance Tests) that runs externally against PyLegend — it is NOT the same as `pytest ./tests/`. Researcher must find how PyLegend participates (the wiring is unclear from the codebase). This is a blocking research item before touching `legend_test_server` fixture.

### LegendQL API Switchover (PURE-05)
- **D-07:** Full switchover for LegendQL frames only — `LegendQLApiLegendServiceInputFrame` and `LegendQLApiLegendFunctionInputFrame` switch to Pure for both schema retrieval and execution. Legacy and Pandas API frames are separate classes and use SQL until Phase 2.
- **D-08:** User noted that removing Legacy/Pandas APIs first (or at the same time as Phase 1) would simplify the switchover. If the planner/researcher determines this is significantly cleaner, merging Phase 1 and Phase 2 scope is acceptable. Otherwise, keep phases separate per the roadmap.

### Claude's Discretion
- Design of `execute_pure_string()` and `get_pure_string_schema()` method signatures on `LegendClient` — mirror the SQL equivalents (`execute_sql_string` / `get_sql_string_schema`) unless the Pure endpoint requires a meaningfully different request body.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements
- `.planning/REQUIREMENTS.md` — Phase 1 requirements: PURE-01 through PURE-05, TEST-01, TEST-02

### Existing Implementation (what to fix)
- `pylegend/extensions/tds/abstract/legend_service_input_frame.py` — `to_pure()` raises RuntimeError; `to_sql_query_object()` shows service SQL format for reference
- `pylegend/extensions/tds/abstract/legend_function_input_frame.py` — same: `to_pure()` raises RuntimeError
- `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py` — calls `get_sql_string_schema(self.to_sql_query())` at init; needs switching to Pure
- `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py` — same pattern
- `pylegend/core/request/legend_client.py` — existing `execute_sql_string()` / `get_sql_string_schema()` methods; Pure equivalents to be added here

### Test Infrastructure
- `tests/conftest.py` — `legend_test_server` session fixture; starts Java engine JAR; requires `JAVA_HOME`
- `tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java` — test server source; registers SQL endpoints only; researcher must determine if Pure endpoint requires changes here
- `tests/resources/legend/server/pylegend-sql-server/pom.xml` — Maven build; Legend engine version `4.112.0`
- `tests/core/request/test_legend_client_e2e.py` — existing e2e tests for SQL execution; Pure equivalents needed
- `tests/resources/legend/metadata/org.finos.legend.pylegend_pylegend-test-models_0.0.1-SNAPSHOT.json` — test model; contains `simplePersonService`

### Pure Generation Reference (existing working code)
- `pylegend/core/tds/tds_frame.py` — `FrameToPureConfig`, `to_pure_query()` interface
- `pylegend/extensions/tds/abstract/csv_tds_frame.py` — working `to_pure()` example (`#TDS\n...\n#` format for CSV root)
- `tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_service_frame.py` — existing SQL query tests; shows service/function SQL format; Pure test to be added

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `LegendClient._execute_service()` — generic HTTP method used by all existing endpoints; Pure endpoints follow the same pattern
- `tds_columns_from_json()` in `pylegend/core/tds/tds_column.py` — parses schema response; reusable if Pure schema endpoint returns the same JSON format
- `ResponseReader` — streaming response wrapper; reusable for Pure execution responses

### Established Patterns
- `LegendClient` methods: POST to `{path_prefix}/{endpoint}`, JSON body `{"sql": ...}` for SQL; Pure endpoint body format is unknown (research needed)
- `FrameToPureConfig` controls pretty-printing and indentation; existing applied-function Pure generation (filter, extend, etc.) works correctly — only root frames are broken

### Integration Points
- `LegendQLApiExecutableInputTdsFrame.__init__()` — where schema is fetched at frame construction; this is what switches from SQL to Pure
- `execute_frame_to_pandas_df()` on `PyLegendTdsFrame` — the execution path that eventually calls `execute_sql_string()`; this is what switches to `execute_pure_string()`

</code_context>

<specifics>
## Specific Ideas

- The FINOS PCT wiring is a hard unknown — treat it as a research blocker. Don't write any test infrastructure changes until the PCT participation mechanism is understood.
- The test server currently only registers `SqlExecute`. If the Legend engine `4.112.0` JAR exposes a Pure TDS endpoint at a different path (e.g., `pure/v1/execution/executeTDSPure`), the server may already support it without Java changes — researcher should confirm by inspecting the engine JAR or Legend engine source.

</specifics>

<deferred>
## Deferred Ideas

- Moving Legacy/Pandas API removal into Phase 1 scope — flagged by user as potentially cleaner, but kept as a planner decision rather than a locked choice. Current roadmap keeps it in Phase 2.

</deferred>

---

*Phase: 1-Fix Pure Foundation*
*Context gathered: 2026-05-30*
