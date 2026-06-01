# Phase 2: Remove Legacy Code and SQL Layer - Research

**Researched:** 2026-06-01
**Domain:** Python codebase cleanup — directory deletion, import graph surgery, pyproject.toml dependency trimming
**Confidence:** HIGH

## Summary

Phase 2 is a large-scale deletion task: remove everything that belongs exclusively to the Legacy API, Pandas API, and SQL layer from the codebase, while leaving the LegendQL API and Pure execution path fully intact and importable. The scope is purely subtractive — no new functionality is added.

The most important finding is that the SQL metamodel (`core/sql/`) is NOT confined to `core/sql/`, `core/database/`, and `extensions/database/vendors/`. It permeates the shared language layer: every `PyLegendPrimitive` subclass implements `to_sql_expression()`, and all operation expression classes import from `core.sql.metamodel`. The success criterion says `core/sql/` must not exist, but deleting it without touching the shared layer would break all imports. The plan must account for this cross-cutting cleanup.

A second cross-cutting dependency: `grammar_method` — a decorator used on dunder methods (`__eq__`, `__add__`, etc.) in ALL shared primitive files (`primitive.py`, `integer.py`, `float.py`, `boolean.py`, `number.py`, `string.py`, `date.py`, `datetime.py`, `strictdate.py`, `decimal.py`) — is defined in `pylegend/core/tds/pandas_api/frames/helpers/series_helper.py`, which is being deleted. The decorator is a simple identity tag (`setattr(func, "_is_grammar_method", True)`) used purely for Pandas API introspection. It must be either removed or re-homed in `pylegend/utils/` before deleting `pandas_api/`.

The phase also has a critical distinction about `testcontainers`: it is currently a **runtime** dependency in `pyproject.toml` but only used in `pylegend/samples/local_legend_env.py` (samples). Moving it to dev-only is correct; the samples module itself stays (samples/legendql_api/ is kept, samples/pandas_api/ is deleted).

The LegendQL API function files (e.g., `legendql_api_filter_function.py`) each contain both a `to_sql()` method (used for SQL path) and a `to_pure()` method (used for Pure path). Only `to_sql()` and its imports are deleted from these files; `to_pure()` is preserved.

**Primary recommendation:** Proceed in three waves: (1) move `grammar_method` out of `pandas_api/`, then delete entire Legacy/Pandas API trees and their test mirrors; (2) gut SQL from shared abstractions (BaseTdsFrame, AppliedFunction, PyLegendTdsFrame, PyLegendPrimitive, all operation classes, project_cooridnates) and from LegendQL function files; (3) prune pyproject.toml, samples, and the root `__init__.py`.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| REMV-01 | Legacy API (`LegacyApiTdsClient`, all `legacy_api/` modules) is removed from the codebase | Full tree identified — see Architecture Patterns section |
| REMV-02 | Pandas API (`PandasApiTdsClient`, all `pandas_api/` modules) is removed from the codebase | Full tree identified; `grammar_method` dependency must be handled first |
| REMV-03 | SQL metamodel layer (`core/sql/`, `core/database/`, `extensions/database/vendors/`) is removed from the codebase | Identified; requires cross-cutting cleanup of shared language layer too |
| REMV-04 | SQL-related dev dependencies (`sqlalchemy`, `pg8000`, `pymysql`, `cryptography`, `mockito`) are removed from `pyproject.toml` | Confirmed these are dev-group only; straightforward deletion from pyproject.toml |
| REMV-05 | `testcontainers` is moved from runtime dependency to dev-only dependency | Confirmed — used only in `pylegend/samples/local_legend_env.py` |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- Apache 2.0 header required on every Python file — when editing files, preserve the existing header
- Max line length 127 chars (flake8)
- `mypy` strict mode — all parameters and return types must be annotated
- Every module must define `__all__: PyLegendSequence[str]`
- Public APIs centralized in `__init__.py` files — root `__init__.py` must be updated
- `flake8` linting enforced — no unused imports allowed after deletions
- No direct SQL path invocation after phase is complete
- Python 3.9–3.14 compatibility maintained

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Legacy API deletion | Package layer (`pylegend/legacy_api_tds_client.py`, `core/tds/legacy_api/`, `core/language/legacy_api/`, `extensions/tds/legacy_api/`) | Tests (`tests/core/tds/legacy_api/`, `tests/extensions/tds/frames/legacy_api/`) | Complete top-to-bottom removal of one query-building layer |
| Pandas API deletion | Package layer (`core/tds/pandas_api/`, `core/language/pandas_api/`, `extensions/tds/pandas_api/`, `samples/pandas_api/`) | Tests (all pandas_api test trees) | Complete top-to-bottom removal of another query-building layer |
| SQL metamodel deletion | Package layer (`core/sql/`, `core/database/`, `extensions/database/vendors/`) | Shared language layer (all primitives and operations that implement `to_sql_expression`) | Cross-cutting — SQL types woven into shared primitives |
| execute_frame / SQL method removal | Abstract base classes (`BaseTdsFrame`, `PyLegendTdsFrame`) | LegendQL function files (remove `to_sql()` method from each) | Interface-level surgery on retained code |
| pyproject.toml cleanup | Build configuration | None | Dependency graph trimming |
| Root `__init__.py` cleanup | Public API surface | `samples/__init__.py`, `core/language/__init__.py` | Remove LegacyApiTdsClient export; remove `agg`/`olap_*` legacy exports |

## Standard Stack

This phase performs deletions only — no new libraries are introduced. The post-phase retained stack is:

### Retained Runtime Dependencies (after phase)
| Library | Purpose | Notes |
|---------|---------|-------|
| `requests>=2.27.1` | HTTP communication with Legend engine | Kept [VERIFIED: pyproject.toml] |
| `ijson>=3.1.4` | Streaming JSON parsing | Kept [VERIFIED: pyproject.toml] |

### Moved to Dev-Only
| Library | Current | Target | Reason |
|---------|---------|--------|--------|
| `testcontainers>=3.0.0` | `dependencies` | `dev` group | Used only in `pylegend/samples/local_legend_env.py`; not needed at runtime [VERIFIED: grepping codebase] |

### Removed Entirely
| Library | Where Used | Reason for Removal |
|---------|-----------|-------------------|
| `pandas>=1.0.0/2.1.1` | `base_tds_frame.py`, `tds_frame.py`, `csv_tds_frame.py`, `to_pandas_df_result_handler.py` | No more Pandas API or DataFrame return type on BaseTdsFrame |
| `numpy>=1.20.0/1.26.0` | `to_pandas_df_result_handler.py` | No more Pandas result handler |
| `sqlalchemy>=2.0.0` | `extensions/database/vendors/postgres/` (dev dep) | SQL metamodel gone |
| `pg8000>=1.0.0` | PostgreSQL driver for SQL execution tests | SQL execution gone |
| `pymysql>=1.0.0` | MySQL driver for SQL execution tests | SQL execution gone |
| `cryptography>=40.0.0` | SSL for database connections | SQL execution gone |
| `mockito>=1.0.0` | Mocking in Legacy/Pandas API tests | Those tests gone |
| `pandas-stubs>=1.5.0` | Type stubs for pandas (dev dep) | pandas removed; stubs no longer needed |

## Package Legitimacy Audit

No external packages are being installed in this phase. This section is not applicable — Phase 2 removes dependencies, it does not add them.

## Architecture Patterns

### System Architecture Diagram

Current (Phase 1 complete):

```
User Code
    |
    v
LegendQLApiTdsClient / LegacyApiTdsClient / PandasApiTdsClient
    |
    v
Frame hierarchy (LegendQL / Legacy / Pandas)
    |                               |
    v                               v
to_pure() [Pure path]           to_sql() [SQL path — DEAD]
    |                               |
    v                               v
LegendClient.execute_pure_string()  LegendClient.execute_sql_string() [TO DELETE]
    |
    v
Legend Engine
```

After Phase 2:

```
User Code
    |
    v
LegendQLApiTdsClient
    |
    v
LegendQL Frame hierarchy (core/tds/legendql_api/, extensions/tds/legendql_api/)
    |
    v
to_pure() [only compilation target]
    |
    v
LegendClient.execute_pure_string() / get_pure_string_schema()
    |
    v
Legend Engine
```

### What Gets Deleted

#### Entire directories (rm -rf equivalent)

**Production code:**
- `pylegend/core/tds/legacy_api/` — Legacy API TDS frames and functions
- `pylegend/core/tds/pandas_api/` — Pandas API TDS frames and functions
- `pylegend/core/language/legacy_api/` — Legacy API language expressions
- `pylegend/core/language/pandas_api/` — Pandas API language expressions
- `pylegend/core/sql/` — SQL metamodel (AST nodes)
- `pylegend/core/database/` — SqlToStringGenerator and db_extension
- `pylegend/extensions/tds/legacy_api/` — Legacy API extension frames
- `pylegend/extensions/tds/pandas_api/` — Pandas API extension frames
- `pylegend/extensions/database/` — Postgres SQL-to-string extension
- `pylegend/samples/pandas_api/` — Pandas API samples

**Test code (parallel mirrors of above):**
- `tests/core/tds/legacy_api/`
- `tests/core/tds/pandas_api/`
- `tests/core/language/legacy_api/`
- `tests/core/database/`
- `tests/extensions/database/`
- `tests/extensions/tds/frames/legacy_api/`
- `tests/extensions/tds/frames/pandas_api/`
- `tests/samples/pandas_api/`

#### Individual file deletions
- `pylegend/legacy_api_tds_client.py` — Legacy API client factory
- `pylegend/core/tds/sql_query_helpers.py` — SQL query manipulation helpers
- `pylegend/extensions/tds/result_handler/to_pandas_df_result_handler.py` — Pandas result handler

#### Also delete
- `tests/test_legacy_api_tds_client.py` — Legacy API client integration test

### What Gets Surgically Modified

These files are **retained** but require targeted editing:

#### 0. `pylegend/utils/` — Add `grammar_method.py` (BEFORE deleting pandas_api)

The `grammar_method` decorator is defined in `series_helper.py` (being deleted) and imported by ALL shared primitive files. It must be moved first. Create `pylegend/utils/grammar_method.py`:

```python
# Copyright 2026 Goldman Sachs
# [Apache 2.0 header]

from pylegend._typing import PyLegendSequence
from typing import TypeVar, Callable

__all__: PyLegendSequence[str] = ["grammar_method"]

F = TypeVar('F', bound=Callable)  # type: ignore[type-arg]

def grammar_method(func: F) -> F:
    """Tag a method as a grammar method (Pandas API introspection marker)."""
    setattr(func, "_is_grammar_method", True)
    return func
```

Then update all shared primitive files that import `grammar_method` from `pandas_api/frames/helpers/series_helper` to import from `pylegend.utils.grammar_method` instead.

Files needing this import update: `primitive.py`, `boolean.py`, `integer.py`, `float.py`, `number.py`, `decimal.py`, `string.py`, `date.py`, `datetime.py`, `strictdate.py` in `core/language/shared/primitives/`.

#### 1. `pylegend/__init__.py`
- Remove: `from pylegend.legacy_api_tds_client import LegacyApiTdsClient, legacy_api_tds_client`
- Remove: `"LegacyApiTdsClient"`, `"legacy_api_tds_client"` from `__all__`
- Remove: `agg`, `olap_agg`, `olap_rank` imports (these come from `core/language/legacy_api/` which is deleted; LegendQL API does NOT use these factory functions internally — verified by grep) and remove from `__all__`
- Also remove the `agg` import line: `from pylegend.core.language import agg, now, today, current_user, olap_rank, olap_agg` — replace with `from pylegend.core.language import now, today, current_user`

#### 2. `pylegend/samples/__init__.py`
- Remove: `from pylegend.samples import pandas_api`
- Remove: `"pandas_api"` from `__all__`

#### 3. `pylegend/core/tds/tds_frame.py` (`PyLegendTdsFrame` abstract base)
- Remove: `import pandas as pd`
- Remove: `from pylegend.core.database.sql_to_string import SqlToStringGenerator`
- Remove: `import importlib` and `importlib.import_module(postgres_ext)` (registers Postgres extension at import time — module will be deleted)
- Remove: `FrameToSqlConfig` class entirely (including `postgres_ext` constant)
- Remove: `to_sql_query()` abstract method from `PyLegendTdsFrame`
- Remove: `execute_frame()` abstract method from `PyLegendTdsFrame`
- Remove: `execute_frame_to_string()` abstract method from `PyLegendTdsFrame`
- Remove: `execute_frame_to_pandas_df()` abstract method from `PyLegendTdsFrame`
- Remove: `to_pandas_df()` / `to_pandas()` convenience methods
- Update `__all__` to remove `FrameToSqlConfig`
- `FrameToPureConfig` stays

#### 4. `pylegend/core/tds/abstract/frames/base_tds_frame.py` (`BaseTdsFrame`)
- Remove: `import pandas as pd`
- Remove: `from pylegend.core.sql.metamodel import QuerySpecification`
- Remove: `from pylegend.core.database.sql_to_string import SqlToStringConfig, SqlToStringFormat`
- Remove: `from pylegend.core.tds.tds_frame import FrameToSqlConfig`
- Remove: `from pylegend.extensions.tds.result_handler import ToPandasDfResultHandler, PandasDfReadConfig`
- Remove: `to_sql_query_object()` abstract method
- Remove: `to_sql_query()` concrete method
- Remove: `execute_frame()` concrete method (calls `execute_sql_string`)
- Remove: `execute_frame_to_pandas_df()` concrete method

#### 5. `pylegend/core/tds/abstract/frames/applied_function_tds_frame.py` (`AppliedFunction` / `AppliedFunctionTdsFrame`)
- Remove: `from pylegend.core.sql.metamodel import QuerySpecification`
- Remove: `from pylegend.core.tds.tds_frame import FrameToSqlConfig`
- Remove: `to_sql()` abstract method from `AppliedFunction`
- Remove: `to_sql_query_object()` concrete method from `AppliedFunctionTdsFrame`

#### 6. `pylegend/core/request/legend_client.py`
- Remove: `get_sql_string_schema()` method
- Remove: `execute_sql_string()` method
- Keep: `get_pure_string_schema()`, `execute_pure_string()`, and all helper/private methods

#### 7. Each LegendQL API function file (17 files in `core/tds/legendql_api/frames/functions/`)
For each of: `legendql_api_head_function.py`, `legendql_api_filter_function.py`, `legendql_api_drop_function.py`, `legendql_api_slice_function.py`, `legendql_api_select_function.py`, `legendql_api_distinct_function.py`, `legendql_api_rename_function.py`, `legendql_api_concatenate_function.py`, `legendql_api_sort_function.py`, `legendql_api_join_function.py`, `legendql_api_extend_function.py`, `legendql_api_window_extend_function.py`, `legendql_api_groupby_function.py`, `legendql_api_aggregate_function.py`, `legendql_api_project_function.py`, `legendql_api_cast_function.py`, `legendql_api_asofjoin_function.py`:
- Remove: `from pylegend.core.tds.sql_query_helpers import ...`
- Remove: `from pylegend.core.sql.metamodel import ...`
- Remove: `from pylegend.core.sql.metamodel_extension import ...`
- Remove: `from pylegend.core.tds.tds_frame import FrameToSqlConfig`
- Remove: `to_sql()` method entirely
- Keep: `to_pure()`, `base_frame()`, `tds_frame_parameters()`, `calculate_columns()`, `validate()`, `name()`

#### 8. `pylegend/core/language/shared/primitives/primitive.py` and all subclasses
- Update: `from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method` → `from pylegend.utils.grammar_method import grammar_method` (done in step 0 above)
- Remove: `from pylegend.core.sql.metamodel import Expression, QuerySpecification`
- Remove: `from pylegend.core.tds.tds_frame import FrameToSqlConfig`
- Remove: `to_sql_expression()` abstract method from `PyLegendPrimitive`
- Cascade: remove `to_sql_expression()` implementations in all subclasses (`boolean.py`, `integer.py`, `float.py`, `decimal.py`, `string.py`, `date.py`, `datetime.py`, `strictdate.py`, `number.py`, `precise_primitives.py`)

#### 9. All `core/language/shared/operations/` files
Each operation expression class (binary_expression, boolean_operation_expressions, collection_operation_expressions, date_operation_expressions, decimal_operation_expressions, float_operation_expressions, integer_operation_expressions, nary_expression, nullary_expression, number_operation_expressions, primitive_operation_expressions, string_operation_expressions, unary_expression) contains:
- `from pylegend.core.sql.metamodel import ...` imports
- `to_sql_expression()` method implementations
All of these must be removed, keeping only `to_pure_expression()` and the Pure-related infrastructure.

#### 10. `pylegend/core/language/shared/expression.py`, `column_expressions.py`, `literal_expressions.py`, `variable_expressions.py`, `tds_row.py`, `pylegend_custom_expressions.py`
All import from `core.sql.metamodel` and implement `to_sql_expression()`. Remove SQL elements, keep Pure elements.

#### 11. `pylegend/core/language/legendql_api/legendql_api_custom_expressions.py` and `legendql_api_tds_row.py`
- Both import `core.sql.metamodel` for SQL expression building
- Remove SQL-related imports and `to_sql_expression()` methods
- Keep Pure-related methods

#### 12. `pylegend/core/project_cooridnates.py`
- Currently imports `NamedArgumentExpression`, `StringLiteral` from `core.sql.metamodel`
- These are used only in `sql_params()` methods which were used by `legend_service_input_frame.py`/`legend_function_input_frame.py` `to_sql_query_object()` implementations
- After SQL removal, `sql_params()` is no longer called from any kept code — the entire `sql_params()` abstract method and all implementations can be removed
- Remove: `from pylegend.core.sql.metamodel import NamedArgumentExpression, StringLiteral`
- Remove: `sql_params()` abstract method from `ProjectCoordinates`
- Remove: `sql_params()` implementations from all subclasses
- `ProjectCoordinates` retains `get_group_id()`, `get_artifact_id()`, `get_version()`, `get_project_id()`, `get_workspace()`, `get_group_workspace()`

#### 13. `pylegend/extensions/tds/abstract/legend_service_input_frame.py` and `legend_function_input_frame.py`
- These abstract base classes have `to_sql_query_object()` methods using SQL metamodel
- Remove: all SQL metamodel imports
- Remove: `to_sql_query_object()` method from each
- Remove: `from pylegend.core.tds.tds_frame import FrameToSqlConfig` import
- Keep: `to_pure()` method, `get_pattern()`, `get_project_coordinates()`, `set_initialized()`, `get_path()`

#### 14. `pylegend/extensions/tds/abstract/csv_tds_frame.py` and `table_spec_input_frame.py`
- Both have `to_sql_query_object()` methods
- Remove SQL elements; keep Pure elements

#### 15. `pylegend/extensions/tds/result_handler/__init__.py`
- Remove: `ToPandasDfResultHandler`, `PandasDfReadConfig` exports (file being deleted)
- Keep: other result handler exports (CSV, string, JSON)

#### 16. LegendQL extension input frames (`extensions/tds/legendql_api/frames/`)
- `legendql_api_legend_service_input_frame.py`, `legendql_api_legend_function_input_frame.py`, `legendql_api_table_spec_input_frame.py`, `legendql_api_csv_input_frame.py`
- Each inherits from the abstract base and calls `super().to_sql_query_object()` — remove that override
- Keep `to_pure()` implementations

#### 17. `core/language/__init__.py`
- Remove: `from pylegend.core.language.legacy_api.legacy_api_tds_row import LegacyApiTdsRow`
- Remove: `from pylegend.core.language.legacy_api.aggregate_specification import LegacyApiAggregateSpecification, agg`
- Remove: `from pylegend.core.language.legacy_api.legacy_api_custom_expressions import LegacyApiOLAPGroupByOperation, LegacyApiOLAPAggregation, LegacyApiOLAPRank, olap_agg, olap_rank`
- Remove all their names from `__all__`

#### 18. LegendQL API function tests — SQL assertions removed
The test files in `tests/core/tds/legendql_api/frames/functions/` each contain both `to_sql_query()` assertions and `to_pure_query()` assertions. Only the SQL assertions are deleted; the Pure assertions are kept. These tests themselves are NOT deleted.

Files to partially clean: `test_legendql_api_head_function.py`, `test_legendql_api_filter_function.py`, `test_legendql_api_drop_function.py`, `test_legendql_api_slice_function.py`, `test_legendql_api_select_function.py`, `test_legendql_api_distinct_function.py`, `test_legendql_api_rename_function.py`, `test_legendql_api_concatenate_function.py`, `test_legendql_api_sort_function.py`, `test_legendql_api_join_function.py`, `test_legendql_api_extend_function.py`, `test_legendql_api_window_extend_function.py`, `test_legendql_api_groupby_function.py`, `test_legendql_api_aggregate_function.py`, `test_legendql_api_project_function.py`, `test_legendql_api_rename_function.py`, `test_legendql_api_limit_function.py`.

#### 19. Shared language tests — SQL fixture setup removed
Tests in `tests/core/language/shared/primitives/` and `tests/core/language/shared/` have class-level SQL fixtures (`base_query = test_frame.to_sql_query_object(...)`) and SQL-testing methods. Remove SQL fixture setup lines and all test methods that use `base_query` or call `to_sql_expression`. Keep all test methods that use `to_pure_expression`.

#### 20. `tests/core/request/test_legend_client.py` and `test_legend_client_e2e.py`
- Remove tests for `execute_sql_string()` and `get_sql_string_schema()` (these methods are deleted)
- Keep tests for `execute_pure_string()`, `get_pure_string_schema()`, `parse_model()`, `compile_model()`

#### 21. `pyproject.toml`
See Pattern 3 in Code Examples.

### Anti-Patterns to Avoid

- **Deleting shared language layer tests wholesale:** Tests in `tests/core/language/shared/primitives/` test both SQL and Pure expression building. Only the SQL-assertion parts should be removed; the Pure parts must stay.
- **Leaving dead import at top of `tds_frame.py`:** The `importlib.import_module('pylegend.extensions.database.vendors.postgres...')` line auto-registers the Postgres extension. Once the Postgres extension is deleted, this line must also go.
- **Forgetting `wrapt<2.0.0` in dev deps:** This is an explicit dep currently. When mockito is removed, `wrapt` should also be removed as nothing else needs it.
- **Leaving `pandas_api` sample reference in `samples/__init__.py`:** This would cause an `ImportError` on `import pylegend` after `pandas_api/` is deleted.
- **Not removing `FrameToSqlConfig` from `tds_frame.py`'s `__all__`:** The planner must be aware this name is currently exported.
- **Deleting `pandas_api/` before moving `grammar_method`:** All shared primitive files import from `pandas_api/frames/helpers/series_helper`. Deleting that module before updating those imports will cause immediate import failures across the entire language layer.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Recursive directory deletion | Custom walker | `git rm -r` / shell `rm -rf` | Standard and atomic |
| Unused import detection | Manual scan | `flake8` run after each file edit | Automated and exhaustive |
| Type annotation verification | Manual inspection | `mypy` run at wave end | Catches abstract method signature drift |

**Key insight:** This phase's primary risk is missed import cleanup causing `ImportError` at `import pylegend` — use `python -c "import pylegend"` after each wave as a smoke test.

## Common Pitfalls

### Pitfall 1: `to_sql_expression` in shared primitives is not "SQL layer"
**What goes wrong:** Planner assumes `core/sql/` deletion = SQL is gone. Actually, `core/language/shared/primitives/primitive.py` has `to_sql_expression()` as an abstract method, and every primitive subclass implements it using SQL metamodel types. Deleting `core/sql/` without removing these methods causes `ModuleNotFoundError: No module named 'pylegend.core.sql'`.
**Why it happens:** The SQL metamodel was used both as a query builder AND as a type system for expression trees in the shared language layer.
**How to avoid:** Treat `to_sql_expression()` removal in the shared language layer as a separate task in the same phase, not as a consequence of deleting `core/sql/`.
**Warning signs:** Any file in `core/language/shared/` that imports `from pylegend.core.sql`.

### Pitfall 2: `core/project_cooridnates.py` imports SQL metamodel
**What goes wrong:** `project_cooridnates.py` (retained module, used by LegendQL API) imports `NamedArgumentExpression` and `StringLiteral` from `core.sql.metamodel` for its `sql_params()` method. Deleting `core/sql/` without cleaning `project_cooridnates.py` causes `ImportError` on any LegendQL operation.
**Why it happens:** `sql_params()` was used when building SQL `FunctionCall` nodes for the `service()` table function — a path that no longer exists.
**How to avoid:** Remove `sql_params()` from `ProjectCoordinates` and all subclasses, and remove the SQL import.

### Pitfall 3: `agg`, `olap_agg`, `olap_rank` are exported from root but ONLY used in Legacy API
**What goes wrong:** `pylegend/__init__.py` currently exports `agg`, `olap_agg`, `olap_rank` which come from `core/language/legacy_api/`. A planner might assume these are used by LegendQL and try to move them.
**What we found:** LegendQL API function tests (`test_legendql_api_aggregate_function.py`, `test_legendql_api_groupby_function.py`) do NOT import `agg` or OLAP helpers — they use inline lambdas. The LegendQL API frames themselves do not import from `legacy_api`. [VERIFIED: grep of `core/tds/legendql_api/` for `legacy_api` — no results]
**How to handle:** Simply remove `agg`, `LegacyApiTdsRow`, `LegacyApiAggregateSpecification`, `LegacyApiOLAPGroupByOperation`, `LegacyApiOLAPAggregation`, `LegacyApiOLAPRank`, `olap_agg`, `olap_rank` from `core/language/__init__.py` and `pylegend/__init__.py`. No re-homing needed.

### Pitfall 4: `grammar_method` decorator in shared primitives imports from deleted module
**What goes wrong:** `pylegend/core/language/shared/primitives/primitive.py` (and 9 other shared primitive files) imports `grammar_method` from `pylegend.core.tds.pandas_api.frames.helpers.series_helper` — which is being deleted. If `pandas_api/` is deleted before this import is updated, ALL shared primitive imports fail, breaking the entire language layer.
**Why it happens:** The `@grammar_method` decorator was added to dunder methods (`__eq__`, `__ne__`, `__add__`, etc.) in ALL shared primitives to support Pandas API introspection.
**What `grammar_method` does:** It is a simple identity function that sets `func._is_grammar_method = True`. In a post-Pandas world it is still needed as long as the attribute is used, but the implementation can live anywhere.
**How to avoid:** Create `pylegend/utils/grammar_method.py` first, update all 10 shared primitive files to import from it, then delete `pandas_api/`.
**Warning signs:** `from pylegend.core.tds.pandas_api` anywhere in a non-pandas file.

### Pitfall 5: LegendQL test files have mixed SQL/Pure assertions
**What goes wrong:** Tests in `tests/core/tds/legendql_api/` have both `frame.to_sql_query(FrameToSqlConfig())` assertions (which test the SQL path) and `frame.to_pure_query(FrameToPureConfig())` assertions (which test the Pure path). A naive "delete all tests that use SQL" would remove valuable Pure coverage.
**Why it happens:** The LegendQL functions were tested for both paths side by side.
**How to avoid:** Within each retained test file, only delete the `to_sql_query`/`FrameToSqlConfig` assertions; keep the `to_pure_query`/`FrameToPureConfig` assertions.

### Pitfall 6: Shared language tests call `to_sql_query_object` as class-level setup
**What goes wrong:** `tests/core/language/shared/primitives/test_integer.py` and similar files call `test_frame.to_sql_query_object(frame_to_sql_config)` at class level to build a `base_query` fixture. After SQL is removed, `to_sql_query_object` no longer exists, so pytest collection fails.
**Why it happens:** The fixture setup interleaves SQL and Pure infrastructure.
**How to avoid:** Remove the SQL fixture setup lines (`base_query = test_frame.to_sql_query_object(...)`, `frame_to_sql_config = FrameToSqlConfig()`, `db_extension = SqlToStringDbExtension()`) and all test methods that use them. Only the Pure-oriented tests remain.

### Pitfall 7: `wrapt` transitive dep
**What goes wrong:** `wrapt<2.0.0` in dev deps is explicitly listed alongside `mockito`. After removing `mockito`, if `wrapt` stays in `pyproject.toml` and `uv.lock` it is dead weight.
**How to avoid:** Remove `wrapt<2.0.0` from `pyproject.toml` dev group together with `mockito`.

## Code Examples

### Pattern 1: Removing `to_sql()` from a LegendQL function file

Before (e.g., `legendql_api_head_function.py`):
```python
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LongLiteral,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig

class LegendQLApiHeadFunction(LegendQLApiAppliedFunction):
    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        ...

    def to_pure(self, config: FrameToPureConfig) -> str:
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->limit({self.__row_count})")
```

After:
```python
from pylegend.core.tds.tds_frame import FrameToPureConfig

class LegendQLApiHeadFunction(LegendQLApiAppliedFunction):
    def to_pure(self, config: FrameToPureConfig) -> str:
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->limit({self.__row_count})")
```

Note: `AppliedFunction.to_sql()` abstract method is removed, so subclasses no longer need to implement it. [VERIFIED: applied_function_tds_frame.py source read]

### Pattern 2: Removing `to_sql_query_object` from abstract base

Before (`base_tds_frame.py`):
```python
@abstractmethod
def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
    pass

def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
    query = self.to_sql_query_object(config)
    sql_to_string_config = SqlToStringConfig(format_=SqlToStringFormat(pretty=config.pretty))
    return config.sql_to_string_generator().generate_sql_string(query, sql_to_string_config)
```

After (`base_tds_frame.py`): both methods and their imports removed entirely.

### Pattern 3: `pyproject.toml` dependency change

Before:
```toml
dependencies = [
    "requests>=2.27.1",
    "ijson>=3.1.4",
    "pandas>=1.0.0 ; python_full_version < '3.12'",
    "pandas>=2.1.1 ; python_full_version >= '3.12'",
    "numpy>=1.20.0 ; python_full_version < '3.12'",
    "numpy>=1.26.0 ; python_full_version >= '3.12'",
    "testcontainers>=3.0.0",
]

[dependency-groups]
dev = [
    "pytest>=7.0.0,<9.0.0 ; python_full_version < '3.11'",
    "pytest>=7.0.0 ; python_full_version >= '3.11'",
    "pytest-cov>=3.0.0",
    "types-requests>=2.28.0",
    "pandas-stubs>=1.5.0",
    "mockito>=1.0.0",
    "sqlalchemy>=2.0.0",
    "pg8000>=1.0.0",
    "pymysql>=1.0.0",
    "cryptography>=40.0.0",
    "wrapt<2.0.0",
]
```

After:
```toml
dependencies = [
    "requests>=2.27.1",
    "ijson>=3.1.4",
]

[dependency-groups]
dev = [
    "pytest>=7.0.0,<9.0.0 ; python_full_version < '3.11'",
    "pytest>=7.0.0 ; python_full_version >= '3.11'",
    "pytest-cov>=3.0.0",
    "types-requests>=2.28.0",
    "testcontainers>=3.0.0",
]
```

### Pattern 4: `grammar_method` relocation

New file `pylegend/utils/grammar_method.py`:
```python
# Copyright 2026 Goldman Sachs
# [Apache 2.0 header]

from typing import TypeVar, Callable
from pylegend._typing import PyLegendSequence

__all__: PyLegendSequence[str] = ["grammar_method"]

F = TypeVar('F', bound=Callable)  # type: ignore[type-arg]


def grammar_method(func: F) -> F:
    """Mark a method as a grammar method for API introspection."""
    setattr(func, "_is_grammar_method", True)
    return func
```

Then in each shared primitive file, change:
```python
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
```
to:
```python
from pylegend.utils.grammar_method import grammar_method
```

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `agg`, `olap_agg`, `olap_rank` are NOT used internally by LegendQL API code | Architecture Patterns #7, #17 | If wrong, those symbols must be moved before deletion, not simply removed |

Note: A1 was partially assumed during initial research but then VERIFIED by grepping `core/tds/legendql_api/` for all `legacy_api` imports — zero results. The assertion is now treated as VERIFIED.

## Open Questions

1. **Does `grammar_method` need to stay as a no-op or can it be completely removed?**
   - What we know: The decorator sets `func._is_grammar_method = True`. The `add_primitive_methods` function in `series_helper.py` checks `getattr(attr, "_is_grammar_method", False)` to discover which methods to proxy onto Series. After `pandas_api/` is deleted, `add_primitive_methods` is also gone.
   - What's unclear: Whether any kept code (e.g., LegendQL tests, shared language tests) introspects `_is_grammar_method` at runtime.
   - Recommendation: The safest approach is to keep the decorator as a no-op in `pylegend/utils/grammar_method.py` rather than removing all `@grammar_method` usages across 10 files. This minimizes diff size and preserves the attribute for any future Pandas API reintroduction. The planner can choose to fully strip the decorator in a cleanup wave if preferred.

2. **`tests/core/language/legendql_api/test_legendql_api_tds_row.py` — partial cleanup or full delete?**
   - What we know: The file imports `from pylegend.core.database.sql_to_string import ...` (SQL layer). This file tests the LegendQL API TDS row, which is kept.
   - Recommendation: Partially clean this test file — remove SQL assertion methods, keep Pure assertion methods.

## Environment Availability

Step 2.6: SKIPPED — Phase 2 has no external tool dependencies. All work is Python file editing, directory deletion, and `pyproject.toml` editing. No new runtimes, databases, or CLIs are required.

## Security Domain

> `security_enforcement: true` per config. ASVS level 1.

This phase performs deletions only — it reduces attack surface by removing SQL execution paths that could be targets for injection. No new capabilities are introduced. ASVS categories applicable to this phase:

| ASVS Category | Applies | Notes |
|---------------|---------|-------|
| V2 Authentication | No | No auth changes |
| V3 Session Management | No | No session changes |
| V4 Access Control | No | No access control changes |
| V5 Input Validation | Positive impact | SQL injection surface removed with SQL execution path |
| V6 Cryptography | No | No crypto changes |

Deleting `cryptography`, `pg8000`, `pymysql` reduces the dependency surface (fewer packages = fewer CVE vectors). This is a security improvement.

## Sources

### Primary (HIGH confidence)
- `pylegend/pyproject.toml` — verified current dependency list [VERIFIED]
- `pylegend/core/tds/abstract/frames/base_tds_frame.py` — verified SQL methods and imports [VERIFIED]
- `pylegend/core/tds/tds_frame.py` — verified `FrameToSqlConfig`, SQL abstractions [VERIFIED]
- `pylegend/core/tds/abstract/frames/applied_function_tds_frame.py` — verified `to_sql()` abstract [VERIFIED]
- `pylegend/core/language/shared/primitives/primitive.py` — verified `to_sql_expression()` abstract [VERIFIED]
- `pylegend/core/project_cooridnates.py` — verified SQL metamodel dependency [VERIFIED]
- `pylegend/extensions/tds/abstract/legend_service_input_frame.py` — verified SQL usage [VERIFIED]
- `pylegend/core/request/legend_client.py` — verified `execute_sql_string` / `get_sql_string_schema` [VERIFIED]
- `pylegend/core/tds/pandas_api/frames/helpers/series_helper.py` — verified `grammar_method` definition [VERIFIED]
- Directory listing of all `legacy_api/`, `pandas_api/`, `core/sql/`, `core/database/`, `extensions/database/` trees [VERIFIED]

### Secondary (MEDIUM confidence)
- Grep analysis of `to_sql_expression` call sites — comprehensive scan of all non-deleted files [VERIFIED]
- Grep analysis of `from pylegend.core.sql` imports across retained code [VERIFIED]
- Grep analysis of `sql_params()` callers [VERIFIED]
- Grep analysis of `grammar_method` usages — 10 shared primitive files [VERIFIED]
- Grep analysis of LegendQL API code for `legacy_api` imports — zero results [VERIFIED]

## Metadata

**Confidence breakdown:**
- Deletion scope (which directories/files): HIGH — directly verified by filesystem inspection
- Surgical edit scope (which methods to remove from retained files): HIGH — directly verified by reading source
- Import surgery in shared language layer: HIGH — grep-verified all SQL metamodel import points
- `grammar_method` re-homing: HIGH — verified what it does and where it is used
- `agg`/`olap_agg`/`olap_rank` disposal: HIGH — verified LegendQL API does not import from legacy_api

**Research date:** 2026-06-01
**Valid until:** 2026-07-01 (stable codebase, no external APIs changing)
