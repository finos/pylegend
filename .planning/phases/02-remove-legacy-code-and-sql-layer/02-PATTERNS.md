# Phase 2: Remove Legacy Code and SQL Layer - Pattern Map

**Mapped:** 2026-06-01
**Files analyzed:** 21 modified files + 1 new file + ~20+ deleted file trees
**Analogs found:** 18 / 21 (3 are deletion-only with no retained analog needed)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `pylegend/utils/grammar_method.py` | utility | transform | `pylegend/utils/class_utils.py` | role-match |
| `pylegend/__init__.py` | config/public-api | request-response | self (edit) | exact |
| `pylegend/samples/__init__.py` | config/public-api | request-response | self (edit) | exact |
| `pylegend/core/language/__init__.py` | config/public-api | request-response | self (edit) | exact |
| `pylegend/core/tds/tds_frame.py` | model/abstract | request-response | self (edit) | exact |
| `pylegend/core/tds/abstract/frames/base_tds_frame.py` | model/abstract | CRUD | self (edit) | exact |
| `pylegend/core/tds/abstract/frames/applied_function_tds_frame.py` | model/abstract | transform | self (edit) | exact |
| `pylegend/core/request/legend_client.py` | service | request-response | self (edit) | exact |
| `pylegend/core/project_cooridnates.py` | model | CRUD | self (edit) | exact |
| `pylegend/core/language/shared/primitives/primitive.py` | model/abstract | transform | self (edit) | exact |
| `pylegend/core/language/shared/primitives/*.py` (9 subclasses) | model | transform | self (edit) | exact |
| `pylegend/core/language/shared/operations/binary_expression.py` | model | transform | self (edit) | exact |
| `pylegend/core/language/shared/operations/*.py` (12 other op files) | model | transform | self (edit) | exact |
| `pylegend/core/language/shared/expression.py` et al. | model | transform | self (edit) | exact |
| `pylegend/core/tds/legendql_api/frames/functions/*.py` (17 files) | service | request-response | `legendql_api_head_function.py` | exact |
| `pylegend/extensions/tds/abstract/legend_service_input_frame.py` | service | request-response | self (edit) | exact |
| `pylegend/extensions/tds/abstract/*.py` (3 other abstract bases) | service | request-response | self (edit) | exact |
| `pylegend/extensions/tds/legendql_api/frames/*.py` (4 input frames) | service | request-response | self (edit) | exact |
| `pylegend/extensions/tds/result_handler/__init__.py` | config/public-api | transform | self (edit) | exact |
| `pyproject.toml` | config | batch | self (edit) | exact |
| Tests: `tests/core/tds/legendql_api/frames/functions/*.py` (17) | test | request-response | self (edit) | exact |
| Tests: `tests/core/language/shared/primitives/*.py` | test | transform | self (edit) | exact |
| Tests: `tests/core/request/test_legend_client.py` et al. | test | request-response | self (edit) | exact |

## Pattern Assignments

### `pylegend/utils/grammar_method.py` (NEW FILE — utility, transform)

**Analog:** `pylegend/utils/class_utils.py`

**Imports pattern** (`class_utils.py` lines 15-19):
```python
from pylegend._typing import (
    PyLegendList,
    PyLegendType,
    PyLegendTypeVar
)
```

**Module structure pattern** (`class_utils.py` lines 1-29):
- Apache 2.0 header (14 lines), copyright year 2023
- One import block from `pylegend._typing`
- `__all__: PyLegendSequence[str] = [...]` immediately after imports
- Single function, no classes

**Exact content to write** (from RESEARCH.md Pattern 4):
```python
# Copyright 2026 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import TypeVar, Callable
from pylegend._typing import PyLegendSequence

__all__: PyLegendSequence[str] = ["grammar_method"]

F = TypeVar('F', bound=Callable)  # type: ignore[type-arg]


def grammar_method(func: F) -> F:
    """Mark a method as a grammar method for API introspection."""
    setattr(func, "_is_grammar_method", True)
    return func
```

---

### `pylegend/__init__.py` (config/public-api — remove legacy exports)

**Current state** (lines 18-46 and 52-80):
```python
# REMOVE these lines:
from pylegend.legacy_api_tds_client import (
    LegacyApiTdsClient,
    legacy_api_tds_client,
)
from pylegend.core.language import (
    agg,
    now,
    today,
    current_user,
    olap_rank,
    olap_agg,
)
```

**After edit** — replace the `core.language` import block (lines 39-46) with:
```python
from pylegend.core.language import (
    now,
    today,
    current_user,
)
```

Remove from `__all__` (lines 51-80): `"LegacyApiTdsClient"`, `"legacy_api_tds_client"`, `"agg"`, `"olap_rank"`, `"olap_agg"`.

---

### `pylegend/samples/__init__.py` (config/public-api — remove pandas_api)

**Current state** (lines 16-22):
```python
from pylegend.samples import legendql_api
from pylegend.samples import pandas_api

__all__: PyLegendSequence[str] = [
    "legendql_api",
    "pandas_api",
]
```

**After edit:**
```python
from pylegend.samples import legendql_api

__all__: PyLegendSequence[str] = [
    "legendql_api",
]
```

---

### `pylegend/core/language/__init__.py` (config/public-api — remove legacy_api exports)

**Lines to remove** (lines 77-115):
```python
from pylegend.core.language.legacy_api.legacy_api_tds_row import LegacyApiTdsRow
from pylegend.core.language.legacy_api.aggregate_specification import LegacyApiAggregateSpecification, agg
from pylegend.core.language.legacy_api.legacy_api_custom_expressions import (
    LegacyApiOLAPGroupByOperation,
    LegacyApiOLAPAggregation,
    LegacyApiOLAPRank,
    olap_agg,
    olap_rank,
)
```

Remove from `__all__` (lines 121-215): `"LegacyApiTdsRow"`, `"LegacyApiAggregateSpecification"`, `"agg"`, `"LegacyApiOLAPGroupByOperation"`, `"LegacyApiOLAPAggregation"`, `"LegacyApiOLAPRank"`, `"olap_agg"`, `"olap_rank"`.

---

### `pylegend/core/tds/tds_frame.py` (model/abstract — remove SQL abstractions)

**Lines to remove entirely:**
- Line 15: `import importlib`
- Line 17: `import pandas as pd`
- Line 24: `from pylegend.core.database.sql_to_string import SqlToStringGenerator`
- Line 26: `from pylegend.extensions.tds.result_handler import PandasDfReadConfig`
- Lines 28-29: `postgres_ext = ...` constant and `importlib.import_module(postgres_ext)` call
- Lines 31-35: `__all__` — remove `"FrameToSqlConfig"` entry
- Lines 38-57: Entire `FrameToSqlConfig` class
- Lines 99-100: `to_sql_query()` abstract method
- Lines 106-127: `execute_frame()`, `execute_frame_to_string()`, `execute_frame_to_pandas_df()` abstract methods
- Lines 129-141: `to_pandas_df()` and `to_pandas()` convenience methods

**Pattern: `FrameToPureConfig` class stays intact** (lines 60-82). All retained methods follow the existing `FrameToPureConfig` pattern — no new class structure needed.

---

### `pylegend/core/tds/abstract/frames/base_tds_frame.py` (model/abstract — remove SQL methods)

**Lines to remove:**
- Line 16: `import pandas as pd`
- Lines 23-27: SQL metamodel and SQL config imports
- Line 29: `FrameToSqlConfig` import (keep `FrameToPureConfig`)
- Lines 35-38: `ToPandasDfResultHandler`, `PandasDfReadConfig` imports
- Lines 59-61: `to_sql_query_object()` abstract method
- Lines 67-70: `to_sql_query()` concrete method
- Lines 106-128: `execute_frame()`, `execute_frame_to_string()`, `execute_frame_to_pandas_df()` concrete methods

**Pattern for retained methods** (lines 73-104): `get_legend_client()`, `to_pure()` abstract, `to_pure_query()` concrete — these stay unchanged.

---

### `pylegend/core/tds/abstract/frames/applied_function_tds_frame.py` (model/abstract — remove SQL abstract)

**Lines to remove:**
- Line 19: `from pylegend.core.sql.metamodel import QuerySpecification`
- Line 21: `from pylegend.core.tds.tds_frame import FrameToSqlConfig`
- Lines 39-40: `to_sql()` abstract method in `AppliedFunction`
- Lines 70-71: `to_sql_query_object()` concrete method in `AppliedFunctionTdsFrame`

**Pattern for retained class** (lines 32-82 minus above): `AppliedFunction` retains `name()`, `to_pure()` (with RuntimeError default), `base_frame()`, `tds_frame_parameters()`, `calculate_columns()`, `validate()`. `AppliedFunctionTdsFrame` retains `__init__`, `to_pure()`, `get_all_tds_frames()`.

---

### `pylegend/core/request/legend_client.py` (service — remove SQL methods)

**Methods to remove** (lines 72-98):
```python
def get_sql_string_schema(
        self,
        sql: str
) -> PyLegendSequence[TdsColumn]:
    ...  # lines 72-84

def execute_sql_string(
        self,
        sql: str,
        chunk_size: PyLegendOptional[int] = None
) -> ResponseReader:
    ...  # lines 86-98
```

**Retained pattern** (lines 100+): `get_pure_string_schema()` and `execute_pure_string()` and all private helpers stay unchanged. No import changes needed in `legend_client.py` since SQL types were not imported at the top level there.

---

### `pylegend/core/project_cooridnates.py` (model — remove sql_params)

**Lines to remove** (lines 21-24):
```python
from pylegend.core.sql.metamodel import (
    NamedArgumentExpression,
    StringLiteral,
)
```

**Method to remove from all 4 classes** — `sql_params()` abstract (line 37-38) and all 3 concrete implementations (lines 60-69, 92-102, 115-125).

**Pattern for retained class structure**: Each subclass (`VersionedProjectCoordinates`, `WorkspaceProjectCoordinates`, `PersonalWorkspaceProjectCoordinates`, `GroupWorkspaceProjectCoordinates`) retains only its `__init__` and `get_*()` accessor methods. `ProjectCoordinates` abstract base loses its only abstract method — it becomes a pure marker base class (no `@abstractmethod` left, just `ABCMeta`). Remove `abc.abstractmethod` import from `abc` import if no longer used; keep `ABCMeta`.

---

### `pylegend/core/language/shared/primitives/primitive.py` (model/abstract — remove SQL abstract)

**Lines to remove** (lines 26-41):
```python
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
...
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
```

**Replace `grammar_method` import** (line 40):
```python
# OLD:
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
# NEW:
from pylegend.utils.grammar_method import grammar_method
```

**Remove abstract method** (lines 55-61):
```python
@abstractmethod
def to_sql_expression(
        self,
        frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
        config: FrameToSqlConfig
) -> Expression:
    pass
```

Also remove `PyLegendDict` from `pylegend._typing` imports if no longer used after this removal — check whether `PyLegendDict` appears elsewhere in the file (it does in `in_list`, so keep it).

**Pattern for retained content**: All `@grammar_method`-decorated dunder methods (`__eq__`, `__ne__`, `is_empty`, etc.) stay intact. Only `to_sql_expression` abstract method and its two imports are cut.

---

### All shared primitive subclasses (9 files in `core/language/shared/primitives/`)

Files: `boolean.py`, `integer.py`, `float.py`, `decimal.py`, `number.py`, `string.py`, `date.py`, `datetime.py`, `strictdate.py`

**Uniform edit pattern per file:**
1. Remove `from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method` — replace with `from pylegend.utils.grammar_method import grammar_method`
2. Remove `from pylegend.core.sql.metamodel import ...` import block
3. Remove `from pylegend.core.tds.tds_frame import FrameToSqlConfig` import
4. Remove `to_sql_expression()` concrete method implementation

**Pattern for identifying `to_sql_expression` method:** These methods always have signature:
```python
def to_sql_expression(
        self,
        frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
        config: FrameToSqlConfig
) -> Expression:
    ...
```
Remove the entire method body. Keep `to_pure_expression()` implementation unchanged.

---

### All `core/language/shared/operations/` files (13 files)

Files: `binary_expression.py`, `boolean_operation_expressions.py`, `collection_operation_expressions.py`, `date_operation_expressions.py`, `decimal_operation_expressions.py`, `float_operation_expressions.py`, `integer_operation_expressions.py`, `nary_expression.py`, `nullary_expression.py`, `number_operation_expressions.py`, `primitive_operation_expressions.py`, `string_operation_expressions.py`, `unary_expression.py`

**Uniform pattern** (shown for `binary_expression.py` lines 26-31 and 42-45):
```python
# REMOVE these imports in each file:
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    # ... other SQL types specific to each file
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
# Also remove from pylegend.core.sql.metamodel_extension import ...

# REMOVE instance variables like:
__to_sql_func: PyLegendCallable[
    [Expression, Expression, PyLegendDict[str, QuerySpecification], FrameToSqlConfig],
    Expression
]

# REMOVE to_sql_expression() method:
def to_sql_expression(
        self,
        frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
        config: FrameToSqlConfig
) -> Expression:
    ...
```

**Keep intact:** `to_pure_expression()` method and all Pure-related infrastructure.

---

### `pylegend/core/language/shared/expression.py` and related files

Files: `expression.py`, `column_expressions.py`, `literal_expressions.py`, `variable_expressions.py`, `tds_row.py`, `pylegend_custom_expressions.py`

**Pattern:** Same as operations files — remove `from pylegend.core.sql.metamodel import ...` blocks, remove `FrameToSqlConfig` imports, remove `to_sql_expression()` abstract/concrete implementations. Keep `to_pure_expression()`.

---

### Each of 17 LegendQL function files (`core/tds/legendql_api/frames/functions/`)

**Canonical pattern** (from `legendql_api_head_function.py` lines 20-27 vs. after):

Before (imports to remove):
```python
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LongLiteral,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
```

After (only keep):
```python
from pylegend.core.tds.tds_frame import FrameToPureConfig
```

Method to remove (lines 48-56):
```python
def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
    base_query = self.__base_frame.to_sql_query_object(config)
    ...
    return new_query
```

Method to keep (lines 58-60):
```python
def to_pure(self, config: FrameToPureConfig) -> str:
    return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
            f"->limit({self.__row_count})")
```

Also remove `PyLegendList` from `pylegend._typing` import if it was only used by the SQL-path type annotations (check each file individually).

---

### `pylegend/extensions/tds/abstract/legend_service_input_frame.py` (service — remove SQL method)

**Lines to remove** (lines 26-38):
```python
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame,
    FrameToSqlConfig,   # <-- remove this name only
    FrameToPureConfig,
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    TableFunction, Select, AllColumns, FunctionCall, QualifiedName,
    NamedArgumentExpression, StringLiteral, AliasedRelation,
    SingleColumn, QualifiedNameReference, Expression,
)
```

Replace the import of `FrameToSqlConfig` with just `FrameToPureConfig` in that import tuple.

**Remove method** starting at line 59: `to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification`.

Keep `to_pure()` method and all other methods (`get_pattern()`, `get_project_coordinates()`, `set_initialized()`, etc.).

---

### `pylegend/extensions/tds/result_handler/__init__.py` (config — remove pandas handler)

**Current state** (lines 15-26):
```python
from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import (
    ToPandasDfResultHandler,
    PandasDfReadConfig
)
...
__all__: PyLegendSequence[str] = [
    "ToPandasDfResultHandler",
    "PandasDfReadConfig",
]
```

**After edit:** Remove both lines entirely. `__all__` becomes empty list `[]` unless other result handlers are added to this `__init__`. (Check if CSV, string, JSON result handlers are also re-exported here — if so, keep their exports.)

---

### `pyproject.toml` (config — remove dependencies)

**Pattern** (from RESEARCH.md Pattern 3):

Remove from `dependencies` table:
- `"pandas>=1.0.0 ; python_full_version < '3.12'"`
- `"pandas>=2.1.1 ; python_full_version >= '3.12'"`
- `"numpy>=1.20.0 ; python_full_version < '3.12'"`
- `"numpy>=1.26.0 ; python_full_version >= '3.12'"`
- `"testcontainers>=3.0.0"` (move to dev group instead)

Remove from `[dependency-groups] dev`:
- `"pandas-stubs>=1.5.0"`
- `"mockito>=1.0.0"`
- `"sqlalchemy>=2.0.0"`
- `"pg8000>=1.0.0"`
- `"pymysql>=1.0.0"`
- `"cryptography>=40.0.0"`
- `"wrapt<2.0.0"`

Add to `[dependency-groups] dev`:
- `"testcontainers>=3.0.0"`

---

### LegendQL function test files (17 files in `tests/core/tds/legendql_api/frames/functions/`)

**Pattern per test file:**
- Remove imports: `from pylegend.core.tds.tds_frame import FrameToSqlConfig`
- Remove imports: `from pylegend.core.database.sql_to_string import ...`
- Remove class-level SQL fixture setup: `frame_to_sql_config = FrameToSqlConfig()` and `base_query = test_frame.to_sql_query(...)` lines
- Remove all test methods that call `frame.to_sql_query(...)` or assert on SQL strings
- Keep all test methods that call `frame.to_pure_query(...)` or assert on Pure strings

---

### Shared language test files (`tests/core/language/shared/primitives/*.py`)

**Pattern per test file** (from RESEARCH.md Pitfall 6):
- Remove: `frame_to_sql_config = FrameToSqlConfig()` class-level assignment
- Remove: `db_extension = SqlToStringDbExtension()` class-level assignment  
- Remove: `base_query = test_frame.to_sql_query_object(frame_to_sql_config)` class-level call
- Remove: all test methods that reference `base_query` or call `to_sql_expression()`
- Keep: all test methods that call `to_pure_expression()` or use Pure infrastructure

---

### `tests/core/request/test_legend_client.py` and `test_legend_client_e2e.py`

**Pattern:** Remove test methods/classes that invoke `execute_sql_string()` or `get_sql_string_schema()`. Keep test methods for `execute_pure_string()`, `get_pure_string_schema()`, `parse_model()`, `compile_model()`.

---

## Shared Patterns

### Apache 2.0 Header (ALL modified files)
**Source:** Every existing file in codebase
**Apply to:** `pylegend/utils/grammar_method.py` and all edited files — preserve existing headers
```python
# Copyright 2023 Goldman Sachs    # (use existing year, or 2026 for new files)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
```

### `__all__` Maintenance
**Source:** Every module in `pylegend/`
**Apply to:** Every modified `__init__.py` and every module where exports are removed
- Rule: After removing a name from the import block, also remove it from `__all__`
- Rule: `__all__: PyLegendSequence[str]` must remain defined in every module

### Import Cleanliness
**Source:** `pylegend/core/tds/legendql_api/frames/functions/legendql_api_head_function.py` (lines 15-28)
**Apply to:** All 17 LegendQL function files and all shared language files
- Never leave unused imports after deletions
- After removing `to_sql()`, check each import — if the imported name only appeared in `to_sql()`, remove that import too

### grammar_method Import Update
**Source:** `pylegend/core/language/shared/primitives/primitive.py` line 40
**Apply to:** All 10 shared primitive files (`primitive.py` + 9 subclasses)
```python
# OLD (delete this):
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
# NEW (add this):
from pylegend.utils.grammar_method import grammar_method
```

## No Analog Found

All deletions (entire directory trees) have no analog needed — they are removed wholesale.

| Deleted Tree | Role | Reason No Analog |
|---|---|---|
| `pylegend/core/tds/legacy_api/` | frame-hierarchy | Deleted entirely, not replaced |
| `pylegend/core/tds/pandas_api/` | frame-hierarchy | Deleted entirely; `grammar_method` extracted first |
| `pylegend/core/language/legacy_api/` | language-layer | Deleted entirely |
| `pylegend/core/language/pandas_api/` | language-layer | Deleted entirely |
| `pylegend/core/sql/` | SQL-AST | Deleted entirely |
| `pylegend/core/database/` | SQL-string | Deleted entirely |
| `pylegend/extensions/database/` | SQL-string ext | Deleted entirely |
| `pylegend/extensions/tds/legacy_api/` | extension frames | Deleted entirely |
| `pylegend/extensions/tds/pandas_api/` | extension frames | Deleted entirely |
| `pylegend/samples/pandas_api/` | samples | Deleted entirely |
| `pylegend/legacy_api_tds_client.py` | client factory | Deleted entirely |
| `pylegend/core/tds/sql_query_helpers.py` | utility | Deleted entirely |
| `pylegend/extensions/tds/result_handler/to_pandas_df_result_handler.py` | result handler | Deleted entirely |

## Execution Order (Wave Dependency)

The planner MUST enforce this order to avoid import failures:

1. **Wave 1 — Move `grammar_method` FIRST:** Create `pylegend/utils/grammar_method.py`; update all 10 shared primitive files to import from new location. Only then delete `pandas_api/`.
2. **Wave 2 — Delete entire trees:** `legacy_api/`, `pandas_api/`, `core/sql/`, `core/database/`, `extensions/database/`, `extensions/tds/legacy_api/`, `extensions/tds/pandas_api/`, `samples/pandas_api/`, individual file deletions.
3. **Wave 3 — Surgical edits on retained files:** Remove SQL methods/imports from `tds_frame.py`, `base_tds_frame.py`, `applied_function_tds_frame.py`, `legend_client.py`, `project_cooridnates.py`, `primitive.py`, all subclasses, all operation expression files, all LegendQL function files, all abstract extension bases, all LegendQL extension input frames, `__init__.py` files.
4. **Wave 4 — Test cleanup:** Remove SQL assertions from retained test files.
5. **Wave 5 — Config:** Edit `pyproject.toml`.

**Smoke test between waves:** `python -c "import pylegend"` after each wave.

## Metadata

**Analog search scope:** `pylegend/utils/`, `pylegend/core/tds/`, `pylegend/core/language/`, `pylegend/core/request/`, `pylegend/extensions/tds/`
**Files scanned:** 18 source files read directly
**Pattern extraction date:** 2026-06-01
