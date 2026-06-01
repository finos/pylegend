---
phase: 01-fix-pure-foundation
reviewed: 2026-05-31T18:30:00Z
depth: standard
files_reviewed: 3
files_reviewed_list:
  - pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py
  - pylegend/core/request/legend_client.py
  - tests/core/request/test_legend_client_e2e.py
findings:
  critical: 1
  warning: 3
  info: 2
  total: 6
status: issues_found
---

# Phase 01: Code Review Report (Plan 05 — Gap Closure)

**Reviewed:** 2026-05-31T18:30:00Z
**Depth:** standard
**Files Reviewed:** 3
**Status:** issues_found

## Summary

This review covers the three files changed in Plan 05 of Phase 01:

1. **`legendql_api_base_tds_frame.py`** — `execute_frame` override and `_get_legendql_input_project_coordinates` were deleted. The frame now inherits `BaseTdsFrame.execute_frame`, which routes execution through `execute_sql_string`. The deletion is correct and complete; no residual references remain.

2. **`legend_client.py`** — the SQL fallback branches (`_pure_to_sql_fallback` calls) were removed from both `get_pure_string_schema` and `execute_pure_string`. The deletion is correct. However, the surviving depot-cascade `except RuntimeError` scope remains too wide: it wraps both the HTTP call and the plan-response parsing, so a malformed-but-200 plan response silently triggers a depot retry rather than propagating a diagnostic error.

3. **`test_legend_client_e2e.py`** — stale `@pytest.mark.xfail` removed from both Pure tests; `LegendClient` constructors now include `depot_server_host`/`depot_server_port`. The changes are functionally correct. A structural inconsistency in the `@pytest.mark.skipif` guards is noted below.

---

## Critical Issues

### CR-01: Overly Broad `except RuntimeError` Silently Swallows Plan-Parse Failures as "Pure Failed"

**File:** `pylegend/core/request/legend_client.py:114-150` (and analogously `167-188`)

**Issue:** In `get_pure_string_schema`, the `try` block (line 114) wraps the HTTP call to `pure/v1/execution/generatePlan` AND the subsequent parsing at lines 123-129. Both the inner `KeyError`/`TypeError` handler (line 125-128) and `_tds_columns_from_plan_result_type` (line 129) raise `RuntimeError` when the plan response is well-formed HTTP but contains unexpected JSON. Those `RuntimeError` instances are caught by the outer `except RuntimeError` at line 130. When a depot server is configured, the code silently retries the depot path instead of surfacing the parse error. A successful primary-path HTTP response with a missing `rootExecutionNode` key is therefore misclassified as "Pure execution failed", and the depot path is attempted — with the original diagnostic discarded.

The same structural problem exists in `execute_pure_string` (lines 167-188), though it is less severe there because `_tds_columns_from_plan_result_type` is not called in that path.

**Fix:** Narrow the try block in both methods so only the HTTP call is guarded; move response parsing outside the except scope:

```python
def get_pure_string_schema(self, pure, project_coordinates):
    lambda_response = super()._execute_service(...)
    lambda_json = json.loads(lambda_response.text)
    execute_input = self._build_execute_input(lambda_json, project_coordinates)
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
        if self.__depot_server_host is not None and self.__depot_server_port is not None:
            # ... depot fallback ...
        raise
    # Parse errors propagate directly — NOT masked as "Pure failed"
    plan_json = json.loads(plan_response.text)
    try:
        result_type = plan_json["rootExecutionNode"]["resultType"]
    except (KeyError, TypeError) as e:
        raise RuntimeError(
            "Unexpected resultType JSON shape from generatePlan: "
            + repr(str(plan_json))[:200], e
        )
    return self._tds_columns_from_plan_result_type(result_type)
```

---

## Warnings

### WR-01: `_get_model_context_data` Uses Raw `requests.get`, Bypassing Auth Scheme and Retry

**File:** `pylegend/core/request/legend_client.py:201-215`

**Issue:** The depot model fetch calls `req_lib.get(url)` directly (a bare `import requests as req_lib` inside the method) instead of going through `ServiceClient._execute_service`. This bypasses: (a) the `AuthScheme` configured at `LegendClient` construction time — requests to an authenticated depot server will silently fail with 401/403; (b) the retry adapter configured on the session; (c) the `secure_http` flag — the URL is always built with `http://` (line 203) regardless of how the depot server is actually served. The inline `import requests as req_lib` is also non-idiomatic; `requests` is already imported at module level implicitly through `ServiceClient`.

**Fix:** Route the depot fetch through `ServiceClient` infrastructure or build the depot URL with an `https://` option controlled by a constructor parameter (e.g., `depot_server_secure_http: bool = False`). At minimum, apply the configured auth scheme headers:

```python
# Or add depot_server_secure_http param and use self._get_session() / _execute_service
scheme = "https" if self.__depot_server_secure_http else "http"
url = f"{scheme}://{self.__depot_server_host}:{self.__depot_server_port}/..."
```

### WR-02: Uncaught `KeyError` in `_build_depot_execute_input` on Service Execution Dict Access

**File:** `pylegend/core/request/legend_client.py:248-255`

**Issue:** Lines 250-254 access `execution["func"]`, `execution["mapping"]`, and `execution["runtime"]` without guarding against missing keys. If the depot model contains a service with a non-relational or unexpected execution format, bare `KeyError` propagates out of `_build_depot_execute_input`. This is inconsistent with the project's error-handling convention (wrap in `RuntimeError` with a descriptive message) and surfaces a confusing traceback with no context about which service or field was problematic.

**Fix:**
```python
try:
    return {
        "function": execution["func"],
        "model": model_pointer,
        "context": {"_type": "BaseExecutionContext"},
        "mapping": execution["mapping"],
        "runtime": execution["runtime"],
    }
except KeyError as e:
    raise RuntimeError(
        f"Service '{service_full_path}' execution element missing expected key {e}. "
        f"Execution dict: {repr(str(execution))[:200]}"
    ) from e
```

### WR-03: `@pytest.mark.skipif(JAVA_HOME is None)` on Pure Tests Is Inconsistent with Fixture Behaviour

**File:** `tests/core/request/test_legend_client_e2e.py:118,132`

**Issue:** The `legend_test_server` session fixture (in `tests/conftest.py:47`) raises `RuntimeError("JAVA_HOME environment variable is not set")` unconditionally when `JAVA_HOME` is absent. This means all six tests in `TestLegendClientE2E` fail at fixture setup regardless — including the four tests without a `skipif` guard. The `@pytest.mark.skipif(os.environ.get("JAVA_HOME") is None, ...)` on `test_e2e_pure_schema_api` and `test_e2e_pure_execute_api` provides no real protection: when `JAVA_HOME` is absent, those two are skipped but the other four still error at the fixture layer. The decoration creates a false impression that these tests are independently optional.

**Fix:** Either (a) remove the `skipif` from the two Pure tests (they are not more optional than the SQL tests), or (b) add a fixture-level skip guard in `conftest.py` so the entire `legend_test_server` fixture is skipped when `JAVA_HOME` is absent, avoiding errors in the four unguarded tests. Option (b) is the correct fix if the intent is to allow running the test suite without Java installed:

```python
# conftest.py
@pytest.fixture(scope="session")
def legend_test_server():
    java_home = os.environ.get("JAVA_HOME")
    if java_home is None:
        pytest.skip("JAVA_HOME unset; skipping legend_test_server")
    ...
```

---

## Info

### IN-01: Missing Space in Two Error Message String Literals in `range()`

**File:** `pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py:364-368,383-385`

**Issue:** Implicit string concatenation at lines 364–368 produces `"...duration_start, duration_end).(with duration_start_unit..."` — no space before the opening parenthesis. Same defect at lines 383–385: `"Both duration_start and duration_end must be provided.(with ..."`. Lines 357–361 are unaffected (they correctly end the first string with a trailing space).

This is pre-existing (introduced in commit `8bb53b9`); Plan 05 did not touch this code.

**Fix:**
```python
# line 366: add trailing space to first string
"Use either (number_start, number_end) or (duration_start, duration_end). "
"(with duration_start_unit and duration_end_unit as needed)."

# line 384: add trailing space to first string
"Both duration_start and duration_end must be provided. "
"(with duration_start_unit and duration_end_unit as needed)."
```

### IN-02: `head()` and `limit()` Parameter Name `row_count` Diverges from Abstract `count`

**File:** `pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py:55,64`

**Issue:** `LegendQLApiBaseTdsFrame.head(self, row_count: int = 5)` and `.limit(self, row_count: int = 5)` use the parameter name `row_count`, while the abstract interface in `LegendQLApiTdsFrame` declares both as `(self, count: int = 5)`. Any caller using the keyword form `frame.head(count=5)` against the abstract type receives `TypeError: unexpected keyword argument 'count'` at runtime. `drop(count)` is consistent between the two levels. This is pre-existing (not introduced by Plan 05).

**Fix:**
```python
def head(self, count: int = 5) -> "LegendQLApiTdsFrame":
    ...
    return LegendQLApiAppliedFunctionTdsFrame(LegendQLApiHeadFunction(self, count))

def limit(self, count: int = 5) -> "LegendQLApiTdsFrame":
    return self.head(count=count)
```

---

_Reviewed: 2026-05-31T18:30:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
