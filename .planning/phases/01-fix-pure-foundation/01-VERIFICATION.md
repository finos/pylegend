---
phase: 01-fix-pure-foundation
verified: 2026-05-31T12:30:00Z
status: gaps_found
score: 3/4 must-haves verified
overrides_applied: 0
gaps:
  - truth: "The Legend PCT matrix remains green after these changes"
    status: failed
    reason: >
      `test_table_spec_frame_execution_error` in
      tests/extensions/tds/frames/legendql_api/test_legendql_api_table_spec_input_frame.py
      now raises RuntimeError (from the new execute_frame override) instead of the
      expected ValueError ('Cannot execute frame as its built on top of non-executable input frames').
      The LegendQLApiBaseTdsFrame.execute_frame override calls
      _get_legendql_input_project_coordinates() before get_legend_client(), so
      TableSpecInputFrame (which has no service/function ancestry) hits the
      'found 0' RuntimeError path rather than the pre-existing ValueError path.
      This is a regression introduced by Plan 04.
    artifacts:
      - path: "pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py"
        issue: >
          execute_frame override does not guard for non-service/function frames
          (e.g., LegendQLApiTableSpecInputFrame). Must check whether the frame has
          a service/function ancestor before taking the Pure path, or fall back to
          calling get_legend_client().execute_sql_string (which raises ValueError
          for non-executable frames as expected).
    missing:
      - >
        In LegendQLApiBaseTdsFrame.execute_frame, before calling
        _get_legendql_input_project_coordinates(), check whether any frame in
        get_all_tds_frames() is a LegendServiceInputFrameAbstract or
        LegendFunctionInputFrameAbstract. If none, delegate to the inherited
        BaseTdsFrame.execute_frame (SQL path) so that non-executable frames
        continue to raise ValueError with the correct message.
  - truth: "Running the existing LegendQL integration tests against a Legend engine produces results via Pure execution (no SQL path invoked)"
    status: partial
    reason: >
      The new Pure-path integration tests in test_legendql_api_legend_service_frame.py
      and test_legendql_api_legend_function_frame.py require JAVA_HOME and the depot
      server (metadata_port) to be configured. These could not be verified without the
      test server running. Additionally, test_e2e_pure_schema_api and
      test_e2e_pure_execute_api in test_legend_client_e2e.py remain marked
      @pytest.mark.xfail with an outdated reason ('To be resolved in Plan 04') even
      though Plan 04 has completed — the xfail marks were never removed.
    artifacts:
      - path: "tests/core/request/test_legend_client_e2e.py"
        issue: >
          test_e2e_pure_schema_api and test_e2e_pure_execute_api still carry
          @pytest.mark.xfail with reason text that references Plan 04 as a future fix.
          Plan 04 has been completed; if the tests actually pass now, xfail should be
          removed. If they still fail (e.g., because they do not use depot_server_host),
          the tests need fixing.
    missing:
      - >
        Remove the @pytest.mark.xfail decorators from test_e2e_pure_schema_api and
        test_e2e_pure_execute_api (or fix the underlying failure and remove them). The
        outdated xfail reason text 'To be resolved in Plan 04' is no longer accurate
        now that Plan 04 is complete.
human_verification:
  - test: "Run the full test suite with JAVA_HOME and depot server configured"
    expected: >
      All tests pass. Specifically: test_legendql_api_legend_person_service_frame_pure_gen,
      test_legendql_api_legend_person_service_frame_pure_execution,
      test_legendql_api_legend_trade_service_frame_pure_execution,
      test_legendql_api_legend_product_service_frame_pure_execution,
      test_legendql_api_legend_person_service_frame_pure_execution_uses_execute_pure_string,
      and the function-frame equivalents all pass. Existing SQL-path tests also pass.
    why_human: >
      These tests require a running Legend server with a metadata/depot endpoint.
      JAVA_HOME is not set in the verification environment. Cannot run without Docker
      or a live engine instance.
  - test: "Confirm test_e2e_pure_schema_api and test_e2e_pure_execute_api pass after removing xfail"
    expected: >
      Both tests pass against the test server. The Pure path through
      execute_pure_string and get_pure_string_schema produces correct data for
      SimplePersonService.
    why_human: >
      Requires JAVA_HOME and a running test server. Cannot automate without the
      environment.
---

# Phase 1: Fix Pure Foundation Verification Report

**Phase Goal:** LegendClient can execute queries end-to-end using Pure (not SQL) as the compilation target, with the PCT test matrix green
**Verified:** 2026-05-31T12:30:00Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `LegendServiceInputFrame.to_pure()` and `LegendFunctionInputFrame.to_pure()` return valid Pure root expressions without raising `RuntimeError` | VERIFIED | Both files have concrete `to_pure()` bodies. Service: returns `\|pylegend::test::{ServiceName}.all()`. Function: returns `\|{path}()`. `raise RuntimeError(...)` stubs are gone (grep count = 0). 3 unit tests pass. |
| 2 | `LegendClient` exposes `execute_pure_string()` and `get_pure_string_schema()` methods that communicate with the Legend engine over the correct HTTP endpoint | VERIFIED | Both methods exist in `legend_client.py`. `execute_pure_string` POSTs to `pure/v1/execution/execute`; `get_pure_string_schema` POSTs to `pure/v1/execution/generatePlan`. Both first parse the Pure string via `pure/v1/grammar/grammarToJson/lambda`. `_build_execute_input` helper exists. Static checks (flake8) pass. |
| 3 | Running the existing LegendQL integration tests against a Legend engine produces results via Pure execution (no SQL path invoked) | PARTIAL | New Pure-path tests added in Plan 04 for service and function frames including a monkeypatch test that asserts `execute_sql_string` is NOT called. BUT: (a) requires JAVA_HOME + depot server — cannot confirm pass/fail in this environment; (b) `test_e2e_pure_schema_api` and `test_e2e_pure_execute_api` still carry `@pytest.mark.xfail` with outdated Plan 04 reason text. |
| 4 | The Legend PCT matrix remains green after these changes | FAILED | `tests/extensions/tds/frames/legendql_api/test_legendql_api_table_spec_input_frame.py::TestLegendQLApiTableSpecInputFrame::test_table_spec_frame_execution_error` FAILS with `RuntimeError: Expected exactly one LegendQL service/function input frame with project_coordinates, found 0`. The test expects `ValueError('Cannot execute frame as its built on top of non-executable input frames...')`. The `execute_frame` override introduced in `LegendQLApiBaseTdsFrame` (Plan 04) intercepts execution for TableSpecInputFrame (which inherits via `LegendQLApiNonExecutableInputTdsFrame -> LegendQLApiInputTdsFrame -> LegendQLApiBaseTdsFrame`) and raises the wrong exception type before `get_legend_client()` can raise the correct ValueError. |

**Score:** 3/4 truths verified (Truth 3 is partial — treated as a human verification item, not a blocker on its own given the code structure is correct; Truth 4 is a hard BLOCKER)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `pylegend/extensions/tds/abstract/legend_service_input_frame.py` | Concrete `to_pure()` body | VERIFIED | Returns `\|pylegend::test::{ServiceName}.all()` via `get_pattern()`. `get_project_coordinates()` getter added. |
| `pylegend/extensions/tds/abstract/legend_function_input_frame.py` | Concrete `to_pure()` body | VERIFIED | Returns `\|{path}()` via `get_path()`. `get_project_coordinates()` getter added. |
| `tests/extensions/tds/abstract/test_legend_service_input_frame.py` | Unit + integration tests | VERIFIED | Exists. `TestLegendServiceInputFramePure` with `test_to_pure_person_service_unit`, `test_to_pure_trade_service_unit`, `test_to_pure_person_service_grammar_round_trip`. |
| `tests/extensions/tds/abstract/test_legend_function_input_frame.py` | Unit + integration tests | VERIFIED | Exists. `TestLegendFunctionInputFramePure` with `test_to_pure_function_unit`, `test_to_pure_function_grammar_round_trip`. |
| `pylegend/core/request/legend_client.py` | `execute_pure_string`, `get_pure_string_schema`, `_build_execute_input` | VERIFIED | All three methods present and substantive. Also includes `_build_depot_execute_input`, `_tds_columns_from_plan_result_type`, `_pure_to_sql_fallback` as supporting helpers. |
| `tests/core/request/test_legend_client_e2e.py` | E2E tests for Pure methods | PARTIAL | Tests exist but still marked `@pytest.mark.xfail` with outdated reason text. |
| `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_service_input_frame.py` | Pure schema fetch in `__init__` | VERIFIED | Uses `get_pure_string_schema(self.to_pure(FrameToPureConfig()), project_coordinates)`. `get_sql_string_schema` absent. |
| `pylegend/extensions/tds/legendql_api/frames/legendql_api_legend_function_input_frame.py` | Pure schema fetch in `__init__` | VERIFIED | Same as service frame. |
| `pylegend/core/tds/legendql_api/frames/legendql_api_base_tds_frame.py` | `execute_frame` override routing Pure | PARTIAL — BLOCKER | Override exists and routes through `execute_pure_string`. But it breaks `LegendQLApiTableSpecInputFrame.execute_frame_to_string()` which now raises `RuntimeError` instead of the expected `ValueError`. |
| `tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java` | Execute JAX-RS registration | VERIFIED | Import line 46 and `register(new Execute(...))` line 127 both confirmed present. |
| `tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_service_frame.py` | New Pure-path tests | VERIFIED | 5 new tests added: `_pure_gen`, `_pure_execution`, `_trade_pure_execution`, `_product_pure_execution`, `_pure_execution_uses_execute_pure_string`. |
| `tests/extensions/tds/frames/legendql_api/test_legendql_api_legend_function_frame.py` | New Pure-path tests | VERIFIED | 3 new tests added: `_pure_gen`, `_pure_execution`, `_pure_execution_uses_execute_pure_string`. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `LegendServiceInputFrameAbstract.to_pure` | `self.get_pattern()` | instance method | VERIFIED | `raw = self.get_pattern().lstrip("/")` found at line 110 |
| `LegendFunctionInputFrameAbstract.to_pure` | `self.get_path()` | instance method | VERIFIED | `return f"\|{self.get_path()}()"` found at line 109 |
| `LegendClient.execute_pure_string` | `pure/v1/execution/execute` | `ServiceClient._execute_service` | VERIFIED | Path `"pure/v1/execution/execute"` at line 173 |
| `LegendClient.get_pure_string_schema` | `pure/v1/execution/generatePlan` | `ServiceClient._execute_service` | VERIFIED | Path `"pure/v1/execution/generatePlan"` at line 116 |
| `LegendClient._build_execute_input` | `VersionedProjectCoordinates` getters | isinstance check | VERIFIED | `isinstance(project_coordinates, VersionedProjectCoordinates)` at line 370 |
| `LegendQLApiLegendServiceInputFrame.__init__` | `LegendClient.get_pure_string_schema` | constructor schema fetch | VERIFIED | `get_pure_string_schema(self.to_pure(FrameToPureConfig()), project_coordinates)` at line 42 |
| `LegendQLApiLegendFunctionInputFrame.__init__` | `LegendClient.get_pure_string_schema` | constructor schema fetch | VERIFIED | Same pattern at line 42 |
| `LegendQLApiBaseTdsFrame.execute_frame` | `LegendClient.execute_pure_string` | override | PARTIAL — BLOCKER | Wired correctly for service/function frames but intercepts ALL LegendQL frame types including non-service ones (TableSpec), breaking the expected ValueError for non-executable frames |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| PURE-01 | 01-02 | `LegendServiceInputFrame.to_pure()` generates valid Pure root | SATISFIED | Concrete implementation present, unit tests pass |
| PURE-02 | 01-02 | `LegendFunctionInputFrame.to_pure()` generates valid Pure root | SATISFIED | Concrete implementation present, unit tests pass |
| PURE-03 | 01-01, 01-03 | `LegendClient` can execute Pure TDS query string | SATISFIED | `execute_pure_string` method present, posts to correct endpoint, Test server exposes endpoint |
| PURE-04 | 01-01, 01-03 | `LegendClient` can retrieve TDS schema from Pure expression | SATISFIED | `get_pure_string_schema` method present, posts to `generatePlan` endpoint |
| PURE-05 | 01-04 | End-to-end via LegendQL API uses Pure not SQL | PARTIAL | Switchover code exists. But `execute_frame` override breaks TableSpec frames — pre-existing test fails |
| TEST-01 | 01-02, 01-04 | PCT matrix remains green | BLOCKED | `test_table_spec_frame_execution_error` fails; this is a pre-existing PCT test |
| TEST-02 | 01-04 | Existing LegendQL integration tests pass via Pure | UNCERTAIN | New tests added; require JAVA_HOME + depot server to confirm; e2e xfail marks not removed |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `tests/core/request/test_legend_client_e2e.py` | 119, 135 | `@pytest.mark.xfail` with stale reason text "To be resolved in Plan 04" | Warning | Plan 04 completed; xfail masks whether tests pass or fail. If tests now pass, xfail causes them to be reported as XPASS or unexpectedly passing, obscuring the result. |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `to_pure()` on service frame returns non-empty string | `uv run pytest tests/extensions/tds/abstract/ -q` | 3 passed, 2 skipped | PASS |
| `execute_pure_string` and `get_pure_string_schema` methods exist | grep count on `legend_client.py` | 1 each | PASS |
| `execute_frame` override — non-executable frame regression | `uv run pytest tests/extensions/tds/frames/legendql_api/test_legendql_api_table_spec_input_frame.py` | 1 FAILED (`test_table_spec_frame_execution_error`) | FAIL |
| Legacy/Pandas frames still use SQL | grep count on `legacy_api/` and `pandas_api/` service frames | 1 each (unchanged) | PASS |
| `get_sql_string_schema` absent from LegendQL input frames | grep = 0 on both LegendQL input frames | 0 | PASS |

### Probe Execution

No probes declared in PLAN files. Step 7c skipped.

### Human Verification Required

#### 1. Full integration test suite with JAVA_HOME and depot

**Test:** Run `JAVA_HOME=<path> uv run pytest tests/ -q` with the full Legend server fixture active (including `metadata_port` as depot).
**Expected:** All tests pass. New Pure-path tests in `test_legendql_api_legend_service_frame.py` and `test_legendql_api_legend_function_frame.py` pass. Existing SQL-path tests pass. `test_e2e_pure_schema_api` and `test_e2e_pure_execute_api` pass after removing xfail marks.
**Why human:** Requires JAVA_HOME and a running Legend engine + depot endpoint. Cannot start Docker in this verification environment.

#### 2. Confirm xfail status on test_e2e_pure_* tests

**Test:** Remove `@pytest.mark.xfail` from `test_e2e_pure_schema_api` and `test_e2e_pure_execute_api`, then run `JAVA_HOME=<path> uv run pytest tests/core/request/test_legend_client_e2e.py -q`.
**Expected:** Both tests pass (6 total passing, 0 xfail). The Pure schema and execute paths return the correct column names and row data for `SimplePersonService`.
**Why human:** Requires a running test server. The xfail reason text is stale but whether the tests actually pass depends on server availability.

## Gaps Summary

Two gaps block the phase goal:

**Gap 1 (BLOCKER):** `test_table_spec_frame_execution_error` fails. The `execute_frame` override on `LegendQLApiBaseTdsFrame` (Plan 04) applies to ALL LegendQL frame subclasses including `LegendQLApiTableSpecInputFrame` (a non-service, non-executable frame). When `execute_frame_to_string()` is called on a TableSpec frame, the override calls `_get_legendql_input_project_coordinates()` which raises `RuntimeError("found 0")` instead of allowing `get_legend_client()` to raise the expected `ValueError("Cannot execute frame as its built on top of non-executable input frames...")`. The fix is to guard the execute_frame override — only take the Pure path when service/function input frames are present in the frame tree; otherwise delegate to `super().execute_frame()` (which triggers `get_legend_client()` → `ValueError`).

**Gap 2 (Warning):** `test_e2e_pure_schema_api` and `test_e2e_pure_execute_api` in `test_legend_client_e2e.py` remain `@pytest.mark.xfail` with an outdated reason referencing Plan 04 as a future fix. Plan 04 is complete. If these tests now pass, the xfail marks should be removed; if they still fail (e.g., because the direct e2e tests don't configure `depot_server_host`), the tests need to be updated to use the depot cascade pattern.

---

_Verified: 2026-05-31T12:30:00Z_
_Verifier: Claude (gsd-verifier)_
