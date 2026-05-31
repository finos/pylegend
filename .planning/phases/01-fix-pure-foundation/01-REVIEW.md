---
phase: 01-fix-pure-foundation
reviewed: 2026-05-31T00:00:00Z
depth: standard
files_reviewed: 14
files_reviewed_list:
  - pylegend/core/request/legend_client.py
  - pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py
  - pylegend/extensions/tds/abstract/legend_function_input_frame.py
  - pylegend/extensions/tds/abstract/legend_service_input_frame.py
  - pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py
  - pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py
  - tests/conftest.py
  - tests/core/request/test_legend_client_e2e.py
  - tests/extensions/tds/abstract/__init__.py
  - tests/extensions/tds/abstract/test_legend_function_input_frame.py
  - tests/extensions/tds/abstract/test_legend_service_input_frame.py
  - tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_function_frame.py
  - tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_service_frame.py
  - tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java
findings:
  critical: 3
  warning: 5
  info: 1
  total: 9
status: issues_found
---

# Phase 01: Code Review Report

**Reviewed:** 2026-05-31T00:00:00Z
**Depth:** standard
**Files Reviewed:** 14
**Status:** issues_found

## Summary

This phase wired the LegendQL API to a Pure execution path by implementing `to_pure()` on abstract input frames, adding `execute_pure_string()` and `get_pure_string_schema()` to `LegendClient`, and registering the Execute JAX-RS endpoint in the test server.

Three blockers were found. Two are runtime-visible bugs in `__str__` implementations that produce character-joined garbage output. The third is the hardcoded `pylegend::test` package in `LegendServiceInputFrameAbstract.to_pure()`, which makes the Pure path silently wrong for any non-test service — undermining the entire purpose of the phase. Five warnings cover silent error masking in fallback paths, a misleading test structure that claims to test Pure execution but falls back to SQL, and an index-out-of-range edge case in the service `to_pure()` logic.

---

## Critical Issues

### CR-01: `__str__` in `LegendQLApiLegendFunctionInputFrame` joins characters, not path segments

**File:** `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py:47`

**Issue:** `'.'.join(self.get_path())` iterates over the characters of the string returned by `get_path()` (a `str`, not a list), joining each character with a dot. For path `"pylegend::test::function::SimplePersonFunction__TabularDataSet_1_"` the result is `"p.y.l.e.g.e.n.d.:.:.t.e.s.t...."` instead of the path itself.

**Fix:**
```python
def __str__(self) -> str:
    return f"LegendQLApiLegendFunctionInputFrame({self.get_path()})"
```

---

### CR-02: `__str__` in `LegendQLApiLegendServiceInputFrame` joins characters, not the pattern

**File:** `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py:47`

**Issue:** Same defect as CR-01. `'.'.join(self.get_pattern())` iterates over characters of the pattern string. For `"/simplePersonService"` the output is `"/.s.i.m.p.l.e.P.e.r.s.o.n.S.e.r.v.i.c.e"`.

**Fix:**
```python
def __str__(self) -> str:
    return f"LegendQLApiLegendServiceInputFrame({self.get_pattern()})"
```

---

### CR-03: `LegendServiceInputFrameAbstract.to_pure()` hardcodes `pylegend::test` package — broken for any real service

**File:** `pylegend/extensions/tds/abstract/legend_service_input_frame.py:105-112`

**Issue:** `to_pure()` always emits `|pylegend::test::{service_name}.all()` regardless of the actual package the service lives in. Only services whose Pure package is `pylegend::test` will produce a correct Pure expression. Any production service in a different package (e.g., `com::example::MyService`) will generate a wrong Pure string and produce incorrect results or silent fallback to SQL — with no error raised. The comment on line 109 acknowledges the limitation but the method is being used as a real execution path, not just a placeholder.

The `LegendServiceInputFrameAbstract` has no way to discover the service's Pure package from the URL pattern alone. The pure string must incorporate the fully-qualified service name. This requires either:
1. Accepting the fully-qualified Pure path as a constructor parameter (analogous to how `LegendFunctionInputFrameAbstract` accepts `path`), or
2. Looking up the service record from depot/model data to resolve the package.

**Fix (minimal — pass qualified name alongside pattern):**
```python
# legend_service_input_frame.py
class LegendServiceInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __pattern: str
    __pure_qualified_name: PyLegendOptional[str]
    __project_coordinates: ProjectCoordinates

    def __init__(
            self,
            pattern: str,
            project_coordinates: ProjectCoordinates,
            pure_qualified_name: PyLegendOptional[str] = None,
    ) -> None:
        self.__pattern = pattern
        self.__project_coordinates = project_coordinates
        self.__pure_qualified_name = pure_qualified_name

    def to_pure(self, config: FrameToPureConfig) -> str:
        if self.__pure_qualified_name is None:
            raise RuntimeError(
                "Cannot generate Pure expression: pure_qualified_name not provided. "
                "Pass the fully-qualified service name (e.g. 'com::example::MyService') "
                "to the constructor."
            )
        return f"|{self.__pure_qualified_name}.all()"
```

---

## Warnings

### WR-01: `get_pure_string_schema` silently swallows plan-parse errors and falls back to SQL

**File:** `pylegend/core/request/legend_client.py:123-130`

**Issue:** The outer `except RuntimeError` on line 130 catches not only engine-level HTTP failures from `_execute_service`, but also `RuntimeError` raised on line 126-128 when `plan_json["rootExecutionNode"]["resultType"]` is missing — and also `RuntimeError` raised by `_tds_columns_from_plan_result_type` on line 129 when column parsing fails. A malformed-but-200 plan response silently triggers the depot or SQL fallback path. This masks bugs in plan response parsing and could result in the SQL schema being returned when a Pure schema was intended and the Pure path is actually functional.

The try block should only wrap the `_execute_service` call, not the subsequent response parsing:

**Fix:**
```python
try:
    plan_response = super()._execute_service(
        method=RequestMethod.POST,
        path="pure/v1/execution/generatePlan",
        data=json.dumps(execute_input),
        headers={"Content-Type": "application/json"},
        stream=False
    )
except RuntimeError as pure_err:
    LOGGER.debug("Pure generatePlan failed (%s); attempting depot-based schema", pure_err)
    # ... fallback logic ...
    return ...

# Outside except: parse errors propagate to caller
plan_json = json.loads(plan_response.text)
try:
    result_type = plan_json["rootExecutionNode"]["resultType"]
except (KeyError, TypeError) as e:
    raise RuntimeError(
        "Unexpected resultType JSON shape from generatePlan: " + repr(str(plan_json))[:200], e
    )
return self._tds_columns_from_plan_result_type(result_type)
```

Apply the same refactor to `execute_pure_string`.

---

### WR-02: Tests asserting Pure execution path are actually running the SQL fallback path

**File:** `tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_service_frame.py:59-153`

**Issue:** `test_legendql_api_legend_person_service_frame_execution`, `test_legendql_api_legend_trade_service_frame_execution`, and `test_legendql_api_legend_product_service_frame_execution` all construct frames via `simple_*_service_frame_legendql_api()` helpers, which create a `LegendQLApiLegendServiceInputFrame` without a depot server configured. Because `to_pure()` emits `|pylegend::test::SimplePersonService.all()`, which the test engine rejects (the Pure schema and execute endpoints reject the `.all()` form — confirmed by the `@pytest.mark.xfail` on `test_e2e_pure_schema_api` and `test_e2e_pure_execute_api` in `test_legend_client_e2e.py`), both `get_pure_string_schema` and `execute_pure_string` silently fall back to the SQL path. The tests pass but are validating SQL behavior, not Pure behavior.

These tests provide false confidence that the Pure wiring works correctly.

**Fix:** Either:
- Use the `monkeypatch`-style approach (as in `test_legendql_api_legend_person_service_frame_pure_execution_uses_execute_pure_string`) to assert which path is taken, or
- Clearly mark the non-depot tests as exercising the SQL fallback path, and rename them accordingly.

---

### WR-03: `to_pure()` in `LegendServiceInputFrameAbstract` raises `IndexError` on empty pattern

**File:** `pylegend/extensions/tds/abstract/legend_service_input_frame.py:110-111`

**Issue:** Line 111 performs `raw[0].upper() + raw[1:]` but the guard `if raw else raw` evaluates the non-empty branch when `raw` is truthy. If `pattern` is `"/"` (or any string containing only slashes), `lstrip("/")` yields an empty string (`""`), `if raw` is `False`, and the else branch returns `raw` (empty string). The generated Pure expression is then `"|pylegend::test::.all()"` — a syntactically invalid Pure string that will silently fail at parse time. If the guard logic were changed to call `raw[0]` without the guard, it would raise `IndexError`. As written it produces a corrupt string instead.

**Fix:**
```python
def to_pure(self, config: FrameToPureConfig) -> str:
    raw = self.get_pattern().lstrip("/")
    if not raw:
        raise ValueError(
            f"Cannot derive Pure service name from pattern '{self.get_pattern()}': "
            "pattern must have a non-empty path component."
        )
    service_name = raw[0].upper() + raw[1:]
    return f"|pylegend::test::{service_name}.all()"
```

---

### WR-04: `_tds_columns_from_plan_result_type` silently maps unknown column types to `String`

**File:** `pylegend/core/request/legend_client.py:352-358`

**Issue:** When a column type string from the plan is not a recognized `PrimitiveType` enum member (e.g., `"StrictDate"`, `"DateTime"`, `"Number"` or any future engine-returned type), the `except KeyError` on line 355 silently coerces it to `PrimitiveType.String`. This means date, datetime, and numeric columns from the plan are silently mislabelled as `String`, causing downstream type errors in operations that depend on column type. The silent coercion also makes debugging very difficult.

**Fix:** Log a warning (or raise an error) when an unknown type is encountered:
```python
except KeyError:
    LOGGER.warning(
        "Unrecognized Pure plan column type '%s' for column '%s'; defaulting to String",
        col_type_str, col_name
    )
    result_columns.append(
        PrimitiveTdsColumn(col_name, PrimitiveType.String)
    )
```

---

### WR-05: `_get_model_context_data` uses an unauthenticated `requests.get` that bypasses the configured auth scheme

**File:** `pylegend/core/request/legend_client.py:204-218`

**Issue:** The depot server fetch on line 213 calls `req_lib.get(url)` (bare `requests.get`) instead of going through the `ServiceClient._execute_service` mechanism. This bypasses the `AuthScheme` configured on the client and any retry logic set up in `ServiceClient.__init__`. In environments where the depot server requires authentication (e.g., OAuth tokens), the request will fail with 401/403 while the error message only shows the URL and raw response text, hiding the auth issue.

**Fix:** Route the depot fetch through the `ServiceClient` infrastructure or at minimum apply the configured auth headers:
```python
auth_headers = self._get_auth_headers()  # or equivalent on ServiceClient
response = req_lib.get(url, headers=auth_headers)
```
Alternatively, refactor to call `super()._execute_service(method=RequestMethod.GET, path=..., ...)` if the depot server host/port is registered as a separate client.

---

## Info

### IN-01: Inline `import re` and `import requests as req_lib` inside methods

**File:** `pylegend/core/request/legend_client.py:204, 240, 296`

**Issue:** `import re` and `import requests as req_lib` are repeated inside individual methods (`_build_depot_execute_input`, `_pure_to_sql_fallback`, `_get_model_context_data`) rather than at module level. While Python caches imports after the first load, this is non-idiomatic and obscures the module's dependencies. The `re` standard library module in particular is always available and carries no cost concern.

**Fix:** Move `import re` and `import requests` to the module-level import block at the top of the file.

---

_Reviewed: 2026-05-31T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
