# Phase 1: Fix Pure Foundation - Pattern Map

**Mapped:** 2026-05-31
**Files analyzed:** 7
**Analogs found:** 7 / 7

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `pylegend/extensions/tds/abstract/legend_service_input_frame.py` | model/frame | request-response | `pylegend/extensions/tds/abstract/csv_tds_frame.py` | role-match (same `to_pure()` method slot) |
| `pylegend/extensions/tds/abstract/legend_function_input_frame.py` | model/frame | request-response | `pylegend/extensions/tds/abstract/csv_tds_frame.py` | role-match |
| `pylegend/core/request/legend_client.py` | service | request-response | `pylegend/core/request/legend_client.py` (extend in-place) | exact |
| `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py` | frame | request-response | `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py` | exact |
| `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py` | frame | request-response | `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py` | exact |
| `tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java` | config/server | request-response | itself (extend in-place) | exact |
| `tests/core/request/test_legend_client_e2e.py` | test | request-response | itself (extend in-place) | exact |

## Pattern Assignments

---

### `pylegend/extensions/tds/abstract/legend_service_input_frame.py` (frame, request-response)

**What changes:** Replace the `to_pure()` body (currently raises `RuntimeError`) with a valid Pure root string.

**Analog:** `pylegend/extensions/tds/abstract/csv_tds_frame.py` — the only other frame that implements `to_pure()` as a concrete string generator.

**Working `to_pure()` pattern** (`csv_tds_frame.py` line 91-92):
```python
def to_pure(self, config: FrameToPureConfig) -> str:
    return f"#TDS\n{self.__csv_string}#"
```

**Target pattern for `LegendServiceInputFrameAbstract.to_pure()`** (`legend_service_input_frame.py` line 105-106 — to replace):
```python
def to_pure(self, config: FrameToPureConfig) -> str:
    raise RuntimeError("to_pure is not supported for LegendServiceInputFrame")
```

Replace with (Pure root string — package path `pylegend::test` is VERIFIED from test model JSON; call form needs engine verification):
```python
def to_pure(self, config: FrameToPureConfig) -> str:
    # self.__pattern is the HTTP pattern, e.g. '/simplePersonService'
    # Pure path: package prefix + service class name derived from pattern
    # ASSUMED call form: |<package>::<ServiceName>.all()
    # Exact call form must be tested against running engine (see RESEARCH.md Open Question 1)
    return f"|{self.__pattern}"
```

**NOTE:** The `__pattern` field is name-mangled. The concrete Pure string format is the main unknown. The `get_pattern()` getter (line 108) is available and avoids name-mangling: `self.get_pattern()`. The `__project_coordinates` field is also available via instance state.

**Existing instance access pattern** (lines 47-58):
```python
class LegendServiceInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __pattern: str
    __project_coordinates: ProjectCoordinates
    __initialized: bool = False

    def __init__(
            self,
            pattern: str,
            project_coordinates: ProjectCoordinates,
    ) -> None:
        self.__pattern = pattern
        self.__project_coordinates = project_coordinates
```

Use `self.get_pattern()` and a new `get_project_coordinates()` getter (or access via the existing `__project_coordinates` with name-mangling `_LegendServiceInputFrameAbstract__project_coordinates`) to build the Pure string in `to_pure()`.

---

### `pylegend/extensions/tds/abstract/legend_function_input_frame.py` (frame, request-response)

**What changes:** Same as above but for function frames. Replace `to_pure()` `RuntimeError` with valid Pure function call.

**Analog:** `legend_service_input_frame.py` (symmetric pattern).

**Target replacement** (line 105-106):
```python
def to_pure(self, config: FrameToPureConfig) -> str:
    raise RuntimeError("to_pure is not supported for LegendFunctionInputFrame")
```

**Existing instance fields** (lines 46-57):
```python
class LegendFunctionInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __path: str
    __project_coordinates: ProjectCoordinates
    __initialized: bool = False

    def get_path(self) -> str:
        return self.__path
```

Use `self.get_path()` to build the Pure path. Function path from test model is `pylegend::test::function::SimplePersonFunction__TabularDataSet_1_`. ASSUMED Pure call form: `|pylegend::test::function::SimplePersonFunction__TabularDataSet_1_()`.

---

### `pylegend/core/request/legend_client.py` (service, request-response)

**What changes:** Add `execute_pure_string()` and `get_pure_string_schema()` methods.

**Analog:** Existing `execute_sql_string()` and `get_sql_string_schema()` in the same file — direct mirror.

**`get_sql_string_schema` pattern to copy** (lines 53-65):
```python
def get_sql_string_schema(
        self,
        sql: str
) -> PyLegendSequence[TdsColumn]:
    response = super()._execute_service(
        method=RequestMethod.POST,
        path="sql/v1/execution/schema",
        data=json.dumps({"sql": sql}),
        headers={"Content-Type": "application/json"},
        stream=False
    )
    response_text: str = response.text
    return tds_columns_from_json(response_text)
```

**`execute_sql_string` pattern to copy** (lines 67-79):
```python
def execute_sql_string(
        self,
        sql: str,
        chunk_size: PyLegendOptional[int] = None
) -> ResponseReader:
    iter_content = super()._execute_service(
        method=RequestMethod.POST,
        path="sql/v1/execution/execute",
        data=json.dumps({"sql": sql}),
        headers={"Content-Type": "application/json"},
        stream=True
    ).iter_content(chunk_size=chunk_size)
    return ResponseReader(iter_content)
```

**`parse_model` pattern for text/plain requests** (lines 81-93 — use for `grammarToJson/lambda` call):
```python
def parse_model(
        self,
        model_code: str,
        return_source_information: bool = False
) -> str:
    response = super()._execute_service(
        method=RequestMethod.POST,
        path="pure/v1/grammar/grammarToJson/model",
        data=model_code,
        headers={"Content-Type": "text/plain"},
        query_params=[("returnSourceInformation", "true" if return_source_information else "false")]
    )
    return response.text
```

**New methods to add** (mirror of existing, per Claude's Discretion in CONTEXT.md):

`execute_pure_string` — two-step: parse lambda via `grammarToJson/lambda`, then POST to `pure/v1/execution/execute`:
```python
def execute_pure_string(
        self,
        pure: str,
        project_coordinates: "ProjectCoordinates",
        chunk_size: PyLegendOptional[int] = None
) -> ResponseReader:
    # Step 1: parse lambda string to protocol JSON
    lambda_response = super()._execute_service(
        method=RequestMethod.POST,
        path="pure/v1/grammar/grammarToJson/lambda",
        data=pure,
        headers={"Content-Type": "text/plain"},
        stream=False
    )
    lambda_json = json.loads(lambda_response.text)
    # Step 2: build ExecuteInput and POST to execute endpoint
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

`get_pure_string_schema` — For Phase 1 simplest approach: keep using `get_sql_string_schema()` with `to_sql_query()` for schema (per RESEARCH.md recommended Option 2). If implementing PURE-04 fully, use `generatePlan`:
```python
def get_pure_string_schema(
        self,
        pure: str,
        project_coordinates: "ProjectCoordinates"
) -> PyLegendSequence[TdsColumn]:
    lambda_response = super()._execute_service(
        method=RequestMethod.POST,
        path="pure/v1/grammar/grammarToJson/lambda",
        data=pure,
        headers={"Content-Type": "text/plain"},
        stream=False
    )
    lambda_json = json.loads(lambda_response.text)
    execute_input = self._build_execute_input(lambda_json, project_coordinates)
    response = super()._execute_service(
        method=RequestMethod.POST,
        path="pure/v1/execution/generatePlan",
        data=json.dumps(execute_input),
        headers={"Content-Type": "application/json"},
        stream=False
    )
    plan_json = json.loads(response.text)
    result_type = plan_json["rootExecutionNode"]["resultType"]
    return tds_columns_from_json(json.dumps(result_type))
```

**Helper to add** (builds ExecuteInput JSON per RESEARCH.md Pattern 1):
```python
def _build_execute_input(
        self,
        lambda_json: "PyLegendDict[str, object]",
        project_coordinates: "ProjectCoordinates"
) -> "PyLegendDict[str, object]":
    # project_coordinates must be VersionedProjectCoordinates for Pure execution
    # (WorkspaceProjectCoordinates need different sdlcInfo structure)
    from pylegend.core.project_cooridnates import VersionedProjectCoordinates
    if isinstance(project_coordinates, VersionedProjectCoordinates):
        sdlc_info: "PyLegendDict[str, object]" = {
            "_type": "alloy",
            "groupId": project_coordinates.get_group_id(),
            "artifactId": project_coordinates.get_artifact_id(),
            "version": project_coordinates.get_version()
        }
    else:
        raise RuntimeError(
            "Pure execution requires VersionedProjectCoordinates; "
            f"got {type(project_coordinates).__name__}"
        )
    return {
        "function": lambda_json,
        "model": {
            "_type": "pointer",
            "sdlcInfo": sdlc_info
        },
        "context": {"_type": "BaseExecutionContext"}
    }
```

**Imports needed** (already present in file at lines 15-27; no new imports required):
```python
from pylegend.core.request.service_client import (ServiceClient, RequestMethod)
import json
from pylegend.core.request.response_reader import ResponseReader
from pylegend._typing import (PyLegendSequence, PyLegendOptional)
from pylegend.core.tds.tds_column import TdsColumn, tds_columns_from_json
```

---

### `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py` (frame, request-response)

**What changes:** In `__init__`, switch schema fetch from `get_sql_string_schema(self.to_sql_query())` to Pure-based schema (either `get_pure_string_schema(self.to_pure(), project_coordinates)` or keep SQL for schema per Phase 1 simplification).

**Analog:** Current file itself, plus `legendql_api_legend_function_input_frame.py` (symmetric).

**Current `__init__` pattern** (lines 31-43):
```python
def __init__(
        self,
        pattern: str,
        project_coordinates: ProjectCoordinates,
        legend_client: LegendClient,
) -> None:
    LegendServiceInputFrameAbstract.__init__(self, pattern=pattern, project_coordinates=project_coordinates)
    LegendQLApiExecutableInputTdsFrame.__init__(
        self,
        legend_client=legend_client,
        columns=legend_client.get_sql_string_schema(self.to_sql_query())
    )
    LegendQLApiLegendServiceInputFrame.set_initialized(self, True)
```

**New pattern** (switch the `columns=` argument):
```python
LegendQLApiExecutableInputTdsFrame.__init__(
    self,
    legend_client=legend_client,
    columns=legend_client.get_pure_string_schema(
        self.to_pure(),
        project_coordinates
    )
)
```

**NOTE:** `self.to_pure()` can only be called after `LegendServiceInputFrameAbstract.__init__()` has set `self.__pattern` and `self.__project_coordinates`. Order of `__init__` calls must remain: abstract frame init first, then executable frame init.

**WARNING (from RESEARCH.md Pitfall 6):** Do NOT change `execute_frame()` on `BaseTdsFrame`. The execution path for LegendQL frames must be overridden specifically in `LegendQLApiExecutableInputTdsFrame` or these concrete classes only.

---

### `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py` (frame, request-response)

**What changes:** Same as service frame above, but for function frames. Switch `columns=legend_client.get_sql_string_schema(self.to_sql_query())` to Pure.

**Analog:** `legendql_api_legend_service_input_frame.py` (symmetric — identical change pattern).

**Current `__init__` pattern** (lines 31-43):
```python
def __init__(
        self,
        path: str,
        project_coordinates: ProjectCoordinates,
        legend_client: LegendClient,
) -> None:
    LegendFunctionInputFrameAbstract.__init__(self, path=path, project_coordinates=project_coordinates)
    LegendQLApiExecutableInputTdsFrame.__init__(
        self,
        legend_client=legend_client,
        columns=legend_client.get_sql_string_schema(self.to_sql_query())
    )
    LegendFunctionInputFrameAbstract.set_initialized(self, True)
```

**New pattern** — same substitution as service frame:
```python
columns=legend_client.get_pure_string_schema(self.to_pure(), project_coordinates)
```

---

### `tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java` (config/server)

**What changes:** Register `Execute` class in `run()` alongside existing registrations.

**Analog:** Existing `SqlExecute` registration in the same file (lines 119-123).

**Existing registration pattern** (lines 119-127):
```java
environment.jersey().register(new SqlExecute(new SQLExecutor(modelManager, planExecutor, routerExtensions, FastList.newListWith(
        new RelationalStoreSQLSourceProvider(projectCoordinateLoader),
        new FunctionSQLSourceProvider(projectCoordinateLoader),
        new LegendServiceSQLSourceProvider(projectCoordinateLoader)),
        generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers))));
environment.jersey().register(new SqlGrammar());
environment.jersey().register(new GrammarToJson());
environment.jersey().register(new Compile(modelManager));
```

**New registration to add** (after line 126 `GrammarToJson` registration, per RESEARCH.md Pattern 4):
```java
environment.jersey().register(
    new Execute(
        modelManager,
        planExecutor,
        routerExtensions,
        generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers)
    )
);
```

**Import to add** at top of file (alongside existing imports):
```java
import org.finos.legend.engine.query.pure.api.Execute;
```

All four constructor arguments (`modelManager`, `planExecutor`, `routerExtensions`, `generatorExtensions.flatCollect(...)`) are already in scope from existing code.

---

### `tests/core/request/test_legend_client_e2e.py` (test, request-response)

**What changes:** Add `test_e2e_pure_schema_api` and `test_e2e_pure_execute_api` test methods.

**Analog:** Existing `test_e2e_schema_string_api` and `test_e2e_execute_string_api` in the same file.

**Test class and fixture pattern** (lines 20-34):
```python
class TestLegendClientE2E:

    def test_e2e_schema_string_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
        res = client.get_sql_string_schema(
            "SELECT * FROM "
            "   service("
            "       pattern => '/simplePersonService', "
            "       coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'"
            "   )"
        )
        assert ", ".join([str(x) for x in res]) == \
            "TdsColumn(Name: First Name, Type: String), ..."
```

**New test pattern to add** (mirror the above, using Pure):
```python
def test_e2e_pure_schema_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
    from pylegend.core.project_cooridnates import VersionedProjectCoordinates
    client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
    coords = VersionedProjectCoordinates(
        "org.finos.legend.pylegend", "pylegend-test-models", "0.0.1-SNAPSHOT"
    )
    res = client.get_pure_string_schema(
        "|pylegend::test::SimplePersonService.all()",  # call form TBD — verify against engine
        coords
    )
    assert ", ".join([str(x) for x in res]) == \
        "TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), " \
        "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String)"

def test_e2e_pure_execute_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
    from pylegend.core.project_cooridnates import VersionedProjectCoordinates
    client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
    coords = VersionedProjectCoordinates(
        "org.finos.legend.pylegend", "pylegend-test-models", "0.0.1-SNAPSHOT"
    )
    res = client.execute_pure_string(
        "|pylegend::test::SimplePersonService.all()",  # call form TBD
        coords
    )
    assert json.loads(b"".join(res))["result"]["columns"] == [
        "First Name", "Last Name", "Age", "Firm/Legal Name"
    ]
```

**Import pattern** (lines 14-18 — already has `json` import; add `VersionedProjectCoordinates` locally in tests or at top):
```python
import json
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
```

---

## Shared Patterns

### Copyright Header
**Source:** Every existing file in the codebase (e.g., `legend_client.py` lines 1-13)
**Apply to:** All modified files (Python files already have it; Java file already has it)
```python
# Copyright 2023 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# ...
```

### `__all__` Export Declaration
**Source:** `legend_client.py` lines 29-31; `legend_service_input_frame.py` lines 41-43
**Apply to:** Any new Python module
```python
__all__: PyLegendSequence[str] = [
    "ClassName",
]
```

### Error Chaining
**Source:** `pylegend/core/tds/tds_column.py` pattern (two-argument RuntimeError)
**Apply to:** `_build_execute_input()` and any new error sites
```python
except Exception as e:
    raise RuntimeError("Descriptive message", e)
```

### `_execute_service()` as HTTP primitive
**Source:** `legend_client.py` lines 57-65 (all four existing methods use it)
**Apply to:** All new `LegendClient` methods — never call `requests` directly
```python
response = super()._execute_service(
    method=RequestMethod.POST,
    path="...",
    data=...,
    headers={"Content-Type": "application/json"},
    stream=False  # or True for streaming
)
```

### Test fixture access
**Source:** `test_legend_client_e2e.py` lines 22-23
**Apply to:** All new e2e tests
```python
def test_name(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
    client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
```

---

## No Analog Found

All files have analogs. No new directories or framework patterns are needed.

---

## Metadata

**Analog search scope:** `pylegend/core/request/`, `pylegend/extensions/tds/`, `tests/core/request/`, `tests/extensions/tds/`, `tests/resources/legend/server/`
**Files read:** 9
**Pattern extraction date:** 2026-05-31
