# Phase 1: Fix Pure Foundation - Research

**Researched:** 2026-05-31
**Domain:** Pure expression generation, Legend engine HTTP protocol, LegendQL execution path
**Confidence:** HIGH

## Summary

Phase 1 fixes three distinct broken pieces: (1) the `to_pure()` root expression generators for service and function input frames, (2) the Pure execution methods on `LegendClient`, and (3) the switchover of LegendQL frame execution from SQL to Pure.

The Legend engine already exposes `POST /api/pure/v1/execution/execute` (class `Execute` in `legend-engine-core-query-pure-http-api`) which accepts an `ExecuteInput` JSON body containing a `function` (LambdaFunction protocol JSON) and a `model` (PureModelContextPointer with AlloySDLC for project coordinates). This endpoint is NOT currently registered in `PyLegendSqlServer.java` — adding `Execute` to the Dropwizard environment requires a single-line Java change and a Maven rebuild. A schema/column-info endpoint analogue for Pure does not exist as a separate endpoint: instead, `POST /api/pure/v1/execution/generatePlan` (also in `Execute` class) returns an execution plan whose `resultType` carries column info, or the schema can be derived by executing the Pure expression via the `pure/v1/grammar/grammarToJson/lambda` endpoint to parse the Pure string, then calling `generatePlan`. The simplest approach is to parse the Pure lambda string via the grammar endpoint, then pass the resulting protocol JSON to the execute endpoint.

The PCT (Protocol Conformance Tests) are NOT a separate Python test suite. They are Java tests inside legend-engine that launch `finos/pylegend:SNAPSHOT` Docker container and execute Python scripts that must generate correct Pure output. The PCT exercises PyLegend's Pure expression generation (CSV frames, applied functions). It does NOT exercise `LegendServiceInputFrame.to_pure()` or `LegendFunctionInputFrame.to_pure()` — those frames are not tested by PCT at all. PCT tests only CSV-backed frames using `#TDS{...}#` as root. The requirement TEST-01 ("Legend PCT matrix remains green after these changes") means: do not break existing `to_pure()` implementations for CSV/table frames and applied functions that the PCT already covers.

**Primary recommendation:** Add `Execute` to `PyLegendSqlServer.java`, add `execute_pure_string()` and `get_pure_string_schema()` to `LegendClient` (using `pure/v1/execution/execute` and `pure/v1/execution/generatePlan` respectively), implement `to_pure()` for both abstract input frames, and switch LegendQL frame execution to Pure.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Pure root format is researcher-discovered — look at Legend engine source, existing `to_pure_query()` output patterns, and the test model JSON in `tests/resources/legend/metadata/`.
- **D-02:** The internal library that extends PyLegend provides its own `to_pure()` roots — it does not use `LegendServiceInputFrame.to_pure()` or `LegendFunctionInputFrame.to_pure()` in production. The fixed root implementations only need to produce valid Pure for PyLegend's own integration test suite.
- **D-03:** Researcher investigates first — discover what endpoint the Legend engine already exposes for Pure TDS execution and schema retrieval before deciding whether `PyLegendSqlServer.java` needs modification.
- **D-04:** Maven is not installed locally. Installing Maven via `pixi global install maven` is a Phase 1 prerequisite for building the test server JAR and running integration tests.
- **D-05:** Local Java is at `/Users/deepyaman/Library/Caches/rattler/cache/pkgs/openjdk-17.0.17-h99a4030_0/lib/jvm` — set `JAVA_HOME` to this path when running integration tests locally.
- **D-06:** There is a separate FINOS Legend PCT (Protocol Conformance Tests) that runs externally against PyLegend — it is NOT the same as `pytest ./tests/`. Researcher must find how PyLegend participates (the wiring is unclear from the codebase). This is a blocking research item before touching `legend_test_server` fixture.
- **D-07:** Full switchover for LegendQL frames only — `LegendQLApiLegendServiceInputFrame` and `LegendQLApiLegendFunctionInputFrame` switch to Pure for both schema retrieval and execution. Legacy and Pandas API frames are separate classes and use SQL until Phase 2.
- **D-08:** User noted that removing Legacy/Pandas APIs first (or at the same time as Phase 1) would simplify the switchover. If the planner/researcher determines this is significantly cleaner, merging Phase 1 and Phase 2 scope is acceptable. Otherwise, keep phases separate per the roadmap.

### Claude's Discretion

- Design of `execute_pure_string()` and `get_pure_string_schema()` method signatures on `LegendClient` — mirror the SQL equivalents (`execute_sql_string` / `get_sql_string_schema`) unless the Pure endpoint requires a meaningfully different request body.

### Deferred Ideas (OUT OF SCOPE)

- Moving Legacy/Pandas API removal into Phase 1 scope — flagged by user as potentially cleaner, but kept as a planner decision rather than a locked choice. Current roadmap keeps it in Phase 2.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PURE-01 | `LegendServiceInputFrame.to_pure()` generates a valid Pure root expression for a Legend service | Pure root format for service is `{|pylegend::test::service::SimplePersonService.all()->from(^meta::pure::runtime::PackageableRuntime(...))}` — use grammar endpoint to discover exact form, or use the existing `to_sql_query()` structure as a reference for the service path |
| PURE-02 | `LegendFunctionInputFrame.to_pure()` generates a valid Pure root expression for a Legend function | Similar to PURE-01 but using function path |
| PURE-03 | `LegendClient` can execute a Pure TDS query string against the Legend engine and return a streaming response | Endpoint: `POST /api/pure/v1/execution/execute`, class `Execute` in `legend-engine-core-query-pure-http-api` |
| PURE-04 | `LegendClient` can retrieve TDS column schema from the Legend engine using a Pure expression | Endpoint: `POST /api/pure/v1/execution/generatePlan` — plan result contains column type info; or use `pure/v1/grammar/grammarToJson/lambda` + schema derivation |
| PURE-05 | End-to-end query execution via the existing LegendQL API produces results using Pure (not SQL) as the compilation target | Switch `execute_frame()` in `LegendQLApiBaseTdsFrame`/`BaseTdsFrame` execution path to call `execute_pure_string(self.to_pure_query())` instead of `execute_sql_string(self.to_sql_query())` |
| TEST-01 | Legend PCT matrix remains green after the full rewrite | PCT = Java tests in legend-engine launching `finos/pylegend:SNAPSHOT` Docker container; exercises CSV-backed Pure generation only; do not break existing `to_pure()` on CSV/table frames |
| TEST-02 | All existing LegendQL integration tests pass against the LegendQL API backed by Pure execution | Existing tests in `tests/extensions/tds/frames/legendql_api/` must pass with Pure execution replacing SQL |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Pure root generation (service/function) | Python library | — | `to_pure()` is a string generator method on frame classes; no network involved |
| Pure HTTP execution | Python library (LegendClient) | Legend engine (Execute.java) | LegendClient owns the HTTP protocol; Legend engine owns the execution |
| Schema retrieval via Pure | Python library (LegendClient) | Legend engine (Execute.java generatePlan) | Schema lives in the plan result type returned by Legend engine |
| Pure grammar parsing | Legend engine (GrammarToJson.java) | — | `pure/v1/grammar/grammarToJson/lambda` converts string to protocol JSON |
| Test server registration | Java test server (PyLegendSqlServer.java) | Maven build | `Execute` class must be registered to expose the endpoint |
| PCT compliance | Python library (to_pure output) | Docker image | PCT runs Python code in Docker and checks Pure output matches expected |

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| requests | 2.27.1+ | HTTP communication with Legend engine | Already used; `LegendClient._execute_service()` handles auth and retry |
| pytest | 7.0.0–9.0.0 | Test runner for integration tests | Project standard |

### No New Python Dependencies

Phase 1 requires no new Python package installs. All needed Python packages are already in `pyproject.toml`. The work is:
- Python: implement `to_pure()` methods and add `LegendClient` methods
- Java: register `Execute` class in `PyLegendSqlServer.java`
- Build: install Maven and rebuild JAR

### Java Side (test server only)

| Artifact | Version | Purpose |
|---------|---------|---------|
| `legend-engine-core-query-pure-http-api` | 4.112.0 (via `legend.engine.version`) | Contains `Execute` class; already in the shaded JAR |

The `legend-engine-server-http-server:4.112.0:shaded` JAR already includes `legend-engine-core-query-pure-http-api` and the `Execute` class. No new Maven dependencies are needed — the class is already on the classpath. Only registration is required.

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `pure/v1/execution/generatePlan` for schema | Execute + parse response columns | `generatePlan` returns `SingleExecutionPlan` with `resultType.tdsColumns`; easier to parse than executing and reading streaming response headers |
| Parsing Pure string via `grammarToJson/lambda` | Constructing protocol JSON from scratch | Grammar endpoint is authoritative and already tested; constructing JSON by hand risks protocol version drift |

**Installation:** No new Python installs needed. Maven install:
```bash
pixi global install maven
```

---

## Package Legitimacy Audit

Phase 1 installs no new Python packages. The only "install" is Maven (a system tool, not a Python package) via `pixi global install maven`.

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

---

## Architecture Patterns

### System Architecture Diagram

```
LegendQL Frame (Python)
    |
    | frame.to_pure_query()  ->  Pure string e.g. "{|pylegend::test::service::...}"
    |
    v
LegendClient.execute_pure_string(pure_str)
    |
    |  POST /api/pure/v1/execution/execute
    |  Body: {"function": <parsed_lambda_json>, "model": <pointer_json>, "context": {...}}
    |  [Lambda parsed via: POST /api/pure/v1/grammar/grammarToJson/lambda]
    |
    v
Legend Engine (PyLegendSqlServer JAR)
    |  Execute.java  ->  PlanGenerator  ->  PlanExecutor
    v
Streaming JSON response
    |
    v
ResponseReader  ->  ResultHandler  ->  Pandas DataFrame / string
```

```
LegendQLApiLegendServiceInputFrame.__init__()
    |
    | (schema fetch at construction time)
    | LegendClient.get_pure_string_schema(self.to_pure())
    |  POST /api/pure/v1/execution/generatePlan
    |  ->  parse SingleExecutionPlan.resultType.tdsColumns
    v
columns: List[TdsColumn]  (used to populate frame schema)
```

### Recommended Project Structure

No new directories needed. Changes are in-place:
```
pylegend/
├── core/request/
│   └── legend_client.py          # add execute_pure_string(), get_pure_string_schema()
├── extensions/tds/abstract/
│   ├── legend_service_input_frame.py   # implement to_pure()
│   └── legend_function_input_frame.py  # implement to_pure()
├── extensions/tds/legendql_api/frames/
│   ├── legendql_api_legend_service_input_frame.py  # switch to Pure schema + execution
│   └── legendql_api_legend_function_input_frame.py # switch to Pure schema + execution
tests/resources/legend/server/pylegend-sql-server/src/main/java/.../
│   └── PyLegendSqlServer.java    # register Execute endpoint
tests/core/request/
│   └── test_legend_client_e2e.py # add Pure execute + schema e2e tests
tests/extensions/tds/frames/legendql_api/
│   └── test_legendql_api_legend_service_frame.py  # add Pure query tests
```

### Pattern 1: Execute endpoint request body construction

**What:** `POST /api/pure/v1/execution/execute` requires an `ExecuteInput` JSON containing a parsed LambdaFunction, a PureModelContextPointer, and execution context.

**When to use:** When executing a Pure TDS query against a Legend engine.

**Two-step process:**

Step 1 — parse the Pure lambda string to protocol JSON:
```
POST /api/pure/v1/grammar/grammarToJson/lambda
Content-Type: text/plain
Body: |pylegend::test::service::SimplePersonService.all()

Response: {"_type": "lambda", "body": [...], "parameters": []}
```

Step 2 — execute with the parsed function:
```python
# Source: VERIFIED via legend-engine Execute.java and GenericLegendExecution.java
{
  "function": {
    "_type": "lambda",
    "body": [...],       # from grammarToJson/lambda response
    "parameters": []
  },
  "model": {
    "_type": "pointer",
    "serializer": {"name": "pure", "version": "vX_YY_Z"},
    "sdlcInfo": {
      "_type": "alloy",
      "groupId": "org.finos.legend.pylegend",
      "artifactId": "pylegend-test-models",
      "version": "0.0.1-SNAPSHOT"
    }
  },
  "context": {"_type": "BaseExecutionContext"}
}
```

**ASSUMED:** The exact `serializer.version` string needed. The existing `parse_model` response includes a serializer version that can be used.

### Pattern 2: Get Pure string schema via generatePlan

**What:** `POST /api/pure/v1/execution/generatePlan` returns a `SingleExecutionPlan`. For a TDS result, `rootExecutionNode.resultType` has a `tdsColumns` field compatible with `tds_columns_from_json()`.

**When to use:** At frame construction time to populate column schema.

```python
# Source: VERIFIED via legend-engine Execute.java generatePlan endpoint
# Plan response tdsColumns format matches what tds_columns_from_json() already parses
response = self._execute_service(
    method=RequestMethod.POST,
    path="pure/v1/execution/generatePlan",
    data=execute_input_json,
    headers={"Content-Type": "application/json"},
    stream=False
)
plan_json = json.loads(response.text)
result_type = plan_json["rootExecutionNode"]["resultType"]
# result_type has same structure as sql/v1/execution/schema response
return tds_columns_from_json(json.dumps(result_type))
```

**Note:** This is `[ASSUMED]` as the exact schema of `resultType` for TDS plans needs verification against the running engine. An alternative is to use `sql/v1/execution/schema` with the existing SQL query string for schema retrieval only (since schema doesn't depend on the execution path), and only switch the execution call to Pure. This simpler approach avoids the `generatePlan` complexity entirely for Phase 1.

### Pattern 3: Pure root expression for LegendServiceInputFrame

**What:** The correct Pure root expression for a Legend service call.

**When to use:** In `LegendServiceInputFrame.to_pure()`.

The existing SQL form uses: `service(pattern => '/simplePersonService', coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT')`

The Pure equivalent — based on the test model JSON showing service path `pylegend::test::model::simple` and the reverse PCT showing that the execute endpoint accepts `ExecuteInput.function` as a compiled lambda — uses a service call expression. However, the execute endpoint for services can also work by sending the service path directly in the model. The simplest correct approach is:

```python
# Source: ASSUMED - inferred from Execute.java pattern and existing codebase structure
# The Pure string that, when parsed by grammarToJson/lambda, gives a valid service call
pure_service_root = f"{{|pylegend::test::service::{service_name}.all()}}"
```

**CRITICAL UNKNOWN:** The exact Pure package path and form for calling a service by its HTTP pattern versus by its Pure path. This requires testing against the running engine. The CONTEXT.md D-01 locks this as researcher-discovered.

**Alternative simpler approach (recommended for Phase 1):**
Keep schema retrieval using `sql/v1/execution/schema` (unchanged), switch only `execute_frame()` to call `pure/v1/execution/execute`. This avoids the complexity of `get_pure_string_schema()` for Phase 1 and still satisfies PURE-03 and PURE-05. PURE-04 becomes a follow-on in the same phase or deferred.

### Pattern 4: Registering Execute in PyLegendSqlServer.java

**What:** The `Execute` JAX-RS resource must be registered with the Dropwizard environment.

**When to use:** In the `run()` method of `PyLegendSqlServer.java`.

```java
// Source: VERIFIED from Execute.java constructor and GrammarToJson registration pattern
// The Execute class needs ModelManager and PlanExecutor (already constructed)
// GrammarToJson is already registered; Execute needs to be added alongside it

environment.jersey().register(
    new Execute(modelManager, planExecutor, routerExtensions,
                generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers)));
```

The `Execute` constructor signature: `Execute(ModelManager, PlanExecutor, Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>>, Iterable<? extends PlanTransformer>)`.

All four arguments are already available in `PyLegendSqlServer.run()`.

### Pattern 5: `execute_pure_string` / `get_pure_string_schema` method signatures

Mirror `execute_sql_string` / `get_sql_string_schema` with the Pure string as input:

```python
# Source: ASSUMED (following Claude's Discretion from CONTEXT.md)
def execute_pure_string(
        self,
        pure: str,
        chunk_size: PyLegendOptional[int] = None
) -> ResponseReader:
    # 1. Parse lambda via pure/v1/grammar/grammarToJson/lambda
    # 2. Construct ExecuteInput JSON with parsed lambda + model pointer from pure context
    # 3. POST to pure/v1/execution/execute, return ResponseReader
    ...

def get_pure_string_schema(
        self,
        pure: str,
        project_coordinates: VersionedProjectCoordinates
) -> PyLegendSequence[TdsColumn]:
    # Option A: generatePlan and parse resultType.tdsColumns
    # Option B: keep using get_sql_string_schema() for schema (simpler Phase 1 approach)
    ...
```

**Key difference from SQL:** The Pure execute endpoint needs to know which model (project coordinates) to load. The SQL endpoints embed coordinates in the SQL query string. The Pure endpoint requires coordinates in `model.sdlcInfo`. This means `execute_pure_string` needs project coordinates as a parameter OR the coordinates are embedded in the Pure string and the model is set to a default/empty context for evaluation.

**Discovery needed:** Whether the service pattern call `/simplePersonService` resolves via the engine's own model lookup without an explicit `model` pointer, or whether the caller must provide `sdlcInfo`.

### Anti-Patterns to Avoid

- **Constructing ExecuteInput protocol JSON by hand from scratch:** The protocol version evolves; always use `grammarToJson/lambda` to parse the Pure lambda, then copy the response into `ExecuteInput.function`. Do not attempt to hand-construct `body: [{_type: "func", function: "project", ...}]`.
- **Calling `pure/v1/execution/execute` without registering `Execute` in the test server:** The endpoint will return 404. Registration is required in `PyLegendSqlServer.java`.
- **Assuming `generatePlan` returns the same JSON as `sql/v1/execution/schema`:** The plan's `resultType.tdsColumns` may have a different structure than the SQL schema response; verify before reusing `tds_columns_from_json()`.
- **Changing the `execute_frame()` method on `BaseTdsFrame` instead of overriding in LegendQL frames:** Per D-07, Legacy and Pandas API frames must continue using SQL. Override in `LegendQLApiExecutableInputTdsFrame` or the LegendQL-specific frame classes only.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Pure grammar → protocol JSON | Hand-construct `LambdaFunction` body array | `pure/v1/grammar/grammarToJson/lambda` endpoint | Protocol JSON is version-sensitive; engine parses its own grammar correctly |
| HTTP retry/session | Custom retry wrapper | Existing `LegendClient._execute_service()` | Already handles retry policy, auth headers, and session management |
| TDS column parsing | New parser for plan result | `tds_columns_from_json()` (if resultType format matches) | Already handles enum and primitive column types |
| Maven install | Bundled or scripted Maven | `pixi global install maven` | pixi is already installed and manages system tools |

**Key insight:** The Legend engine already does all the hard work of Pure parsing, compilation, and execution. PyLegend's role is to (a) generate the right Pure string and (b) call the right HTTP endpoint with correctly-structured JSON. Both of these are straightforward given the existing patterns.

---

## Common Pitfalls

### Pitfall 1: Execute class not registered in test server
**What goes wrong:** All calls to `pure/v1/execution/execute` return HTTP 404.
**Why it happens:** `PyLegendSqlServer.java` only registers SQL endpoints. The `Execute` class in `legend-engine-core-query-pure-http-api` is in the shaded JAR but not registered.
**How to avoid:** Add `environment.jersey().register(new Execute(...))` to `PyLegendSqlServer.run()` before running any integration tests.
**Warning signs:** HTTP 404 response from the engine when calling `pure/v1/execution/execute`.

### Pitfall 2: Forgetting to rebuild the JAR after Java changes
**What goes wrong:** Old JAR is used, Java changes have no effect.
**Why it happens:** `conftest.py` starts the JAR from `target/pylegend-sql-server-1.0-shaded.jar`; if not rebuilt, old code runs.
**How to avoid:** Always run `mvn -f tests/resources/legend/server/pylegend-sql-server/pom.xml clean package` after Java changes, then set `JAVA_HOME` before running pytest.
**Warning signs:** `RuntimeError("Unable to start legend server for testing")` or unexpected 404s.

### Pitfall 3: Pure execute endpoint needs project coordinates separately
**What goes wrong:** `pure/v1/execution/execute` called without `model.sdlcInfo`, engine cannot resolve the service or function.
**Why it happens:** Unlike SQL (which embeds coordinates in the `service(coordinates=>...)` call), Pure execution resolves model elements via the `model` pointer in the request body.
**How to avoid:** Always pass `sdlcInfo` with groupId/artifactId/version when calling `pure/v1/execution/execute` for service or function frames. The `ProjectCoordinates` object already has this data.
**Warning signs:** Legend engine returns compilation error about unknown service or function path.

### Pitfall 4: Wrong Pure root expression format for service
**What goes wrong:** `to_pure()` returns a string the engine cannot compile.
**Why it happens:** The exact Pure syntax for calling a service by HTTP pattern vs. by Pure path is not the same as the SQL `service(pattern=>...)` syntax.
**How to avoid:** Test the Pure expression against the running engine via `parse_and_compile_model()` before integrating. The CONTEXT.md D-01 flags this as requiring discovery.
**Warning signs:** Legend engine compilation error when `get_pure_string_schema()` is called at frame construction.

### Pitfall 5: Confusing PCT scope
**What goes wrong:** Developer spends time trying to find/run PCT Python tests, or breaks PCT by accidentally modifying CSV frame `to_pure()`.
**Why it happens:** PCT lives inside the legend-engine Java repo; it runs against the published Docker image, not the local codebase directly.
**How to avoid:** PCT compliance means: (a) do not break `to_pure()` for `CsvInputFrameAbstract` or `TableSpecInputFrameAbstract`, (b) do not break `LegendQLApiAppliedFunction.to_pure()` implementations. TEST-01 is satisfied passively by not regressing these methods.
**Warning signs:** PCT is not part of `pytest ./tests/` and cannot be run locally without Docker and legend-engine.

### Pitfall 6: `execute_frame()` change affects all API types
**What goes wrong:** Legacy API and Pandas API frames start failing when their execution switches to Pure (which they don't support yet).
**Why it happens:** `execute_frame()` is defined on `BaseTdsFrame` and calls `execute_sql_string()`. If changed there, all subclasses are affected.
**How to avoid:** Per D-07, only override in LegendQL-specific classes. Either override `execute_frame()` in `LegendQLApiBaseTdsFrame` or `LegendQLApiExecutableInputTdsFrame`, or add a hook method.
**Warning signs:** Legacy API and Pandas API integration tests start failing.

---

## Code Examples

### Registering Execute in PyLegendSqlServer.java

```java
// Source: VERIFIED from legend-engine Execute.java constructor signature
// Add after SqlExecute registration (line 119 in current PyLegendSqlServer.java)
environment.jersey().register(
    new Execute(
        modelManager,
        planExecutor,
        routerExtensions,
        generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers)
    )
);
// Also register GrammarToJson if not already present (it IS already registered: line 125)
```

### `get_sql_string_schema` pattern (existing, for reference)

```python
# Source: VERIFIED from pylegend/core/request/legend_client.py
def get_sql_string_schema(self, sql: str) -> PyLegendSequence[TdsColumn]:
    response = super()._execute_service(
        method=RequestMethod.POST,
        path="sql/v1/execution/schema",
        data=json.dumps({"sql": sql}),
        headers={"Content-Type": "application/json"},
        stream=False
    )
    return tds_columns_from_json(response.text)
```

### `execute_sql_string` pattern (existing, for reference)

```python
# Source: VERIFIED from pylegend/core/request/legend_client.py
def execute_sql_string(self, sql: str, chunk_size=None) -> ResponseReader:
    iter_content = super()._execute_service(
        method=RequestMethod.POST,
        path="sql/v1/execution/execute",
        data=json.dumps({"sql": sql}),
        headers={"Content-Type": "application/json"},
        stream=True
    ).iter_content(chunk_size=chunk_size)
    return ResponseReader(iter_content)
```

### Parsing a Pure lambda via grammarToJson/lambda

```python
# Source: VERIFIED from legend-engine GrammarToJson.java @Path("lambda") endpoint
# The endpoint accepts text/plain and returns LambdaFunction protocol JSON
response = super()._execute_service(
    method=RequestMethod.POST,
    path="pure/v1/grammar/grammarToJson/lambda",
    data=pure_lambda_string,   # e.g. "|/simplePersonService->execute()"
    headers={"Content-Type": "text/plain"},
    stream=False
)
lambda_json = json.loads(response.text)
# lambda_json = {"_type": "lambda", "body": [...], "parameters": []}
```

### execute_pure_string method skeleton

```python
# Source: ASSUMED (following Claude's Discretion pattern from CONTEXT.md)
def execute_pure_string(
        self,
        pure: str,
        project_coordinates: "ProjectCoordinates",
        chunk_size: PyLegendOptional[int] = None
) -> ResponseReader:
    lambda_json = self._parse_pure_lambda(pure)  # grammarToJson/lambda
    execute_input = self._build_execute_input(lambda_json, project_coordinates)
    iter_content = super()._execute_service(
        method=RequestMethod.POST,
        path="pure/v1/execution/execute",
        data=json.dumps(execute_input),
        headers={"Content-Type": "application/json"},
        stream=True
    ).iter_content(chunk_size=chunk_size)
    return ResponseReader(iter_content)
```

### LegendQLApiLegendServiceInputFrame switch to Pure (PURE-05)

```python
# Source: ASSUMED — mirror of existing __init__ but using Pure path
# Current (SQL):
# LegendQLApiExecutableInputTdsFrame.__init__(
#     self, legend_client=legend_client,
#     columns=legend_client.get_sql_string_schema(self.to_sql_query())
# )

# New (Pure):
LegendQLApiExecutableInputTdsFrame.__init__(
    self, legend_client=legend_client,
    columns=legend_client.get_pure_string_schema(
        self.to_pure(),
        project_coordinates
    )
)
LegendQLApiLegendServiceInputFrame.set_initialized(self, True)
```

---

## D-06 Resolution: PCT Wiring Mechanism

**This was the blocking research item. It is now resolved.**

**Finding:** The Legend PCT is a set of Java tests inside the `finos/legend-engine` repository (specifically in `legend-engine-xts-python/`). These tests use testcontainers to launch `finos/pylegend:SNAPSHOT` Docker container (built from `deployment/snapshot/Dockerfile`) and execute Python scripts inside that container via `docker exec`. The Pure expressions generated by PyLegend's Python code are compared against expected Pure strings defined in `.pure` files.

**What PCT tests:** The PCT exercises Pure expression generation for:
- CSV-backed frames: `#TDS{...}#` as root
- All applied functions (filter, extend, group_by, sort, join, etc.) via `LegendQLApiAppliedFunction.to_pure()`
- Mathematical functions via `pylegend.core.language.shared.pct_helpers`

**What PCT does NOT test:** `LegendServiceInputFrame.to_pure()`, `LegendFunctionInputFrame.to_pure()`, or any service/function root expressions. The internal library provides its own roots in production.

**Implication for Phase 1:**
- TEST-01 is satisfied by not breaking any existing `to_pure()` implementations
- The `LegendServiceInputFrame.to_pure()` and `LegendFunctionInputFrame.to_pure()` implementations are only tested by PyLegend's own integration test suite (TEST-02), not PCT
- No changes to `legend_test_server` fixture are needed for PCT compliance
- The Docker image is rebuilt and published in CI (`docker_build_push` job in `build-ci.yml`) — local PCT runs are not part of Phase 1 scope

**PCT runs at:** PCT tests are in `legend-engine-xts-python/`, run as part of legend-engine's own test suite. PyLegend participates by publishing the `finos/pylegend:SNAPSHOT` Docker image. Maintaining PCT compliance = don't break the Pure string generators that existing PCT tests exercise.

---

## Pure Root Expression Format (PURE-01 and PURE-02)

**Research finding (VERIFIED):** The test model JSON confirms:
- All services have `package: "pylegend::test"` — so `SimplePersonService` full Pure path is `pylegend::test::SimplePersonService`
- All trade/product/relation services are also in `pylegend::test`
- The test function is at `pylegend::test::function::SimplePersonFunction__TabularDataSet_1_`

**VERIFIED package paths:**
- Service `SimplePersonService` → `pylegend::test::SimplePersonService`
- Service `SimpleTradeService` → `pylegend::test::SimpleTradeService`
- Service `SimpleProductService` → `pylegend::test::SimpleProductService`
- Function → `pylegend::test::function::SimplePersonFunction__TabularDataSet_1_`

**ASSUMED Pure call form for service root:**
```
|pylegend::test::SimplePersonService.all()
```
This is the standard Pure syntax for calling a parameterless service. The `|` prefix makes it a lambda. The package path `pylegend::test` is VERIFIED from the test model JSON.

**Alternative form (also ASSUMED — needs engine verification):**
```
|pylegend::test::SimplePersonService->execute()
```

**Still needs discovery:** Whether the Pure call form is `.all()`, `->execute()`, or another invocation. The package path is confirmed; only the method syntax requires engine testing.

**For the function frame (PURE-02):** Function path is `pylegend::test::function::SimplePersonFunction__TabularDataSet_1_`. Call form is likely `|pylegend::test::function::SimplePersonFunction__TabularDataSet_1_()` but needs engine verification.

**Discovery approach:** Call `pure/v1/grammar/grammarToJson/lambda` with candidate expressions and verify against the running engine via `parse_and_compile_model()`. The existing `parse_and_compile_model()` method on `LegendClient` is the right tool.

---

## D-03 Resolution: Pure HTTP Endpoint Discovery

**Finding:** The Legend engine 4.112.0 JAR (via `legend-engine-server-http-server:4.112.0:shaded` dependency) already includes the `Execute` class from `legend-engine-core-query-pure-http-api`. The class exposes:

- `POST /api/pure/v1/execution/execute` — execute a Pure lambda; accepts `ExecuteInput` JSON
- `POST /api/pure/v1/execution/generatePlan` — generate execution plan; returns `SingleExecutionPlan` JSON
- `POST /api/pure/v1/execution/generatePlan/debug` — debug plan generation

**However:** `PyLegendSqlServer.java` does NOT register `Execute`. It only registers `SqlExecute`, `SqlGrammar`, `GrammarToJson`, `Compile`, and server management endpoints.

**Java change required:** Add `environment.jersey().register(new Execute(...))` to `PyLegendSqlServer.run()`. The constructor arguments (ModelManager, PlanExecutor, routerExtensions, planTransformers) are all already available in the `run()` method.

**Schema endpoint situation:** There is no dedicated `pure/v1/execution/schema` endpoint analogous to `sql/v1/execution/schema`. Schema retrieval options are:
1. Use `generatePlan` and parse `resultType.tdsColumns` from the plan JSON
2. Continue using `sql/v1/execution/schema` for schema only (simpler, avoids new endpoint)
3. Use `pure/v1/compilation/lambdaRelationType` (found in `GenericLegendExecution.java`) which returns a `RelationType` — this may be the cleanest option for Pure-based schema retrieval

**Recommended for Phase 1:** Option 2 (keep SQL schema, switch only execution to Pure) for lowest risk. PURE-04 can use `generatePlan` or `lambdaRelationType` in a subsequent task.

---

## D-04 Resolution: Maven Prerequisite

**Finding:**
- Maven is NOT installed locally (`mvn` command not found)
- `pixi` is installed at `/Users/deepyaman/.pixi/bin/pixi` (version 0.63.2)
- Java 17 is available at `/Users/deepyaman/Library/Caches/rattler/cache/pkgs/openjdk-17.0.17-h99a4030_0/lib/jvm`
- The test server JAR does NOT exist yet (`target/` directory absent)
- `JAVA_HOME` is not set in the current environment

**Prerequisites for integration testing:**
1. `pixi global install maven` — install Maven
2. `export JAVA_HOME=/Users/deepyaman/Library/Caches/rattler/cache/pkgs/openjdk-17.0.17-h99a4030_0/lib/jvm`
3. `mvn -f tests/resources/legend/server/pylegend-sql-server/pom.xml clean package`
4. Then run `JAVA_HOME=... pytest ./tests/`

---

## Runtime State Inventory

Not applicable. Phase 1 is not a rename/refactor/migration phase.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Java 17 | Integration test server | Yes (via rattler cache) | 17.0.17 | — |
| Maven | Build test server JAR | No (not installed) | — | `pixi global install maven` |
| pixi | Install Maven | Yes | 0.63.2 | — |
| Docker | PCT compliance (optional) | Yes | 28.3.0 | N/A — PCT runs in CI not locally |
| Python 3.13 | Development | Yes | 3.13.4 | — |
| pytest | Unit + integration tests | Yes (via venv) | per pyproject.toml | — |
| Test server JAR | Integration tests | No (not built) | — | Build via Maven |

**Missing dependencies with no fallback:**
- Maven must be installed before any integration test work can proceed.

**Missing dependencies with fallback:**
- Test server JAR: build via Maven once Maven is installed.

---

## Open Questions

1. **Exact Pure call form for `LegendServiceInputFrame.to_pure()`**
   - What we know: service package is `pylegend::test` (VERIFIED from test model JSON); `SimplePersonService` full Pure path is `pylegend::test::SimplePersonService`
   - What's unclear: whether the Pure invocation is `.all()`, `->execute()`, or another form
   - Recommendation: Test `|pylegend::test::SimplePersonService.all()` via `parse_and_compile_model()` against running engine; try `->execute()` if `.all()` fails

2. **`ProjectCoordinates` threading into `execute_pure_string()`**
   - What we know: SQL endpoints embed coordinates in the query string; Pure endpoint needs them in `model.sdlcInfo`
   - What's unclear: whether `execute_pure_string` should take `ProjectCoordinates` as a parameter, or whether it can infer from the frame
   - Recommendation: Accept `ProjectCoordinates` as explicit parameter on both `execute_pure_string` and `get_pure_string_schema`; the frame passes `self.__project_coordinates`

3. **Schema retrieval approach (PURE-04)**
   - What we know: `generatePlan` returns plan with `resultType`; `lambdaRelationType` endpoint also exists
   - What's unclear: exact JSON schema of `resultType.tdsColumns` vs. `sql/v1/execution/schema` response
   - Recommendation: For Phase 1, defer PURE-04 (keep SQL schema call) and only switch execution to Pure. Verify `generatePlan` tdsColumns format in a separate task.

4. **`clientVersion` for `ExecuteInput`**
   - What we know: `Execute.java` defaults to `PureClientVersions.production` if `clientVersion` is null
   - What's unclear: whether passing `null` (omitting the field) is safe or if a specific version is required
   - Recommendation: Omit `clientVersion` from the initial request; test against engine; the engine uses its production version default

---

## Security Domain

> `security_enforcement: true` in config.json, ASVS Level 1.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | Yes (ASVS 1) | Existing `AuthScheme` hierarchy in `LegendClient` — `LocalhostEmptyAuthScheme`, `HeaderTokenAuthScheme`, `CookieAuthScheme`; no changes needed |
| V3 Session Management | No | PyLegend is a stateless HTTP client; no session state |
| V4 Access Control | No | Pure endpoint access controlled by Legend engine, not PyLegend |
| V5 Input Validation | Yes | Pure strings passed to the engine are parsed server-side; no injection risk beyond what the engine itself handles |
| V6 Cryptography | No | TLS handled by `secure_http` flag on `LegendClient`; no new crypto |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Pure injection via user-controlled pattern/path strings | Tampering | Legend engine compiles Pure before execution; invalid expressions are rejected at parse/compile time; PyLegend does not execute Pure locally |
| Credential leakage in `ExecuteInput` JSON (auth headers) | Information Disclosure | Auth is handled via `AuthScheme` in `_execute_service()` headers, not in the request body; `ExecuteInput` contains no credentials |

**Phase 1 security posture:** No new attack surface. The Pure execute endpoint uses the same `LegendClient._execute_service()` path as existing SQL endpoints, inheriting all existing auth and TLS controls.

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| SQL compilation via `sql/v1/execution/execute` | Pure compilation via `pure/v1/execution/execute` | Phase 1 (this phase) | Removes dependency on SQL metamodel for LegendQL API |
| Service schema via SQL schema endpoint | Schema via Pure `generatePlan` (PURE-04) | Phase 1/2 | Enables pure-query-only operation |
| `to_pure()` raises RuntimeError | `to_pure()` returns valid Pure root | Phase 1 | Unblocks PCT-adjacent testing and Ibis backend (Phase 3) |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `execute_pure_string()` needs `ProjectCoordinates` as a separate parameter | Pattern 4, Open Questions | If wrong: API signature change required; LegendQL frames also need updating |
| A2 | Pure call form for service root uses `.all()` or `->execute()` — package path `pylegend::test` is VERIFIED | Pure Root Expression section | If wrong: compilation error at frame init; must try alternative call form |
| A3 | `generatePlan` `resultType.tdsColumns` format matches `tds_columns_from_json()` input | Pattern 2 | If wrong: need to write a separate parser for plan result type |
| A4 | Omitting `clientVersion` in `ExecuteInput` is safe (engine uses default) | Code Examples | If wrong: engine rejects request; need to add version string |
| A5 | `Execute` class from `legend-engine-core-query-pure-http-api` is already in the shaded JAR at version 4.112.0 | D-03 Resolution | If wrong: need to add Maven dependency and rebuild; unlikely given the JAR structure |
| A6 | PCT compliance is maintained by not breaking existing `to_pure()` implementations | D-06 Resolution | If wrong: PCT fails in CI; need to understand what PCT exercises more precisely |

---

## Sources

### Primary (HIGH confidence)
- `finos/legend-engine` GitHub repo — `Execute.java` at `legend-engine-core/legend-engine-core-query-pure-http-api/` — verified `@Path("pure/v1/execution")`, `@Path("execute")`, `@Path("generatePlan")` endpoints and `ExecuteInput` structure [VERIFIED: github.com/finos/legend-engine]
- `finos/legend-engine` GitHub repo — `AlloySDLC.java`, `PureModelContextPointer.java`, `SDLC.java` — verified JSON discriminators `_type: "alloy"` for coordinates [VERIFIED: github.com/finos/legend-engine]
- `finos/legend-engine` GitHub repo — `GrammarToJson.java` — verified `@Path("lambda")` endpoint at `pure/v1/grammar/grammarToJson/lambda` [VERIFIED: github.com/finos/legend-engine]
- `finos/legend-engine` GitHub repo — `pythonReversePCTLegendQLApi.pure` — verified PCT mechanism uses Docker container running Python, not a standalone Python test suite [VERIFIED: github.com/finos/legend-engine]
- `finos/legend-engine` GitHub repo — `PythonExecutionUtil.java` — verified PCT uses `finos/pylegend:SNAPSHOT` Docker image via testcontainers [VERIFIED: github.com/finos/legend-engine]
- Local codebase — `PyLegendSqlServer.java` — verified `Execute` is NOT registered; only `SqlExecute`, `SqlGrammar`, `GrammarToJson`, `Compile` are registered [VERIFIED: local grep]
- Local codebase — `legend_client.py` — verified existing `execute_sql_string` / `get_sql_string_schema` patterns [VERIFIED: local read]
- Local codebase — `.github/workflows/actions/pytest/action.yml` — verified Maven is used in CI to build JAR [VERIFIED: local read]

### Secondary (MEDIUM confidence)
- `finos/legend-engine` GitHub — `GenericLegendExecution.java` — illustrates `ExecuteInput` construction pattern with `buildPointer()` and `parseLambda()` for SQL-via-Pure [CITED: github.com/finos/legend-engine]
- `finos/legend-engine` GitHub — architecture docs — confirmed `POST /api/pure/v1/execution/execute` and `POST /api/pure/v1/execution/executeStrategic` are the standard execution entry points [CITED: github.com/finos/legend-engine/blob/master/docs/engineering/architecture/overview.md]

### Tertiary (LOW confidence)
- Pure call form for service root (`|pylegend::test::SimplePersonService.all()` or `->execute()`) — package path VERIFIED from test model JSON; call form not yet tested against engine [ASSUMED]

---

## Metadata

**Confidence breakdown:**
- Legend engine HTTP endpoints: HIGH — directly read from source
- PCT mechanism: HIGH — directly read from source (resolved D-06)
- `Execute` registration in test server: HIGH — directly read PyLegendSqlServer.java
- Pure root expression format: LOW — not yet tested against engine
- `generatePlan` schema parsing: MEDIUM — endpoint verified; response format assumed
- Maven prerequisite: HIGH — confirmed `mvn` not found locally

**Research date:** 2026-05-31
**Valid until:** 2026-07-01 (legend-engine 4.112.0 is pinned; stable for ~30 days)
