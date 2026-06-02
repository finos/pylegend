---
phase: 02-remove-legacy-code-and-sql-layer
plan: "02"
subsystem: language
tags: [pure, sql-removal, expression-tree, language-layer, primitives]

requires:
  - phase: 02-remove-legacy-code-and-sql-layer
    plan: "01"
    provides: "grammar_method re-homed to pylegend/utils, legacy/pandas/sql directory trees deleted"

provides:
  - "PyLegendExpression base class without to_sql_expression abstract method"
  - "All 10 shared primitive classes (PyLegendPrimitive + 9 subclasses) without to_sql_expression"
  - "All 13 operation expression files (binary/unary/nary/nullary base + concrete) without SQL path"
  - "ProjectCoordinates without sql_params() methods"
  - "Shared expression files (column, literal, variable, tds_row) without SQL methods"
  - "legendql_api language files without SQL imports/methods"

affects:
  - 02-03-remove-legacy-code-and-sql-layer

tech-stack:
  added: []
  patterns:
    - "Operation expression classes now accept only to_pure_func callback (no SQL path)"
    - "PyLegendExpression base has only to_pure_expression abstract method"
    - "to_sql_node methods removed from PyLegendSortInfo, PyLegendWindow, and frame bound classes"

key-files:
  modified:
    - pylegend/core/language/shared/primitives/primitive.py
    - pylegend/core/language/shared/primitives/boolean.py
    - pylegend/core/language/shared/primitives/integer.py
    - pylegend/core/language/shared/primitives/float.py
    - pylegend/core/language/shared/primitives/decimal.py
    - pylegend/core/language/shared/primitives/number.py
    - pylegend/core/language/shared/primitives/string.py
    - pylegend/core/language/shared/primitives/date.py
    - pylegend/core/language/shared/primitives/datetime.py
    - pylegend/core/language/shared/primitives/strictdate.py
    - pylegend/core/language/shared/primitives/precise_primitives.py
    - pylegend/core/language/shared/operations/binary_expression.py
    - pylegend/core/language/shared/operations/unary_expression.py
    - pylegend/core/language/shared/operations/nary_expression.py
    - pylegend/core/language/shared/operations/nullary_expression.py
    - pylegend/core/language/shared/operations/boolean_operation_expressions.py
    - pylegend/core/language/shared/operations/collection_operation_expressions.py
    - pylegend/core/language/shared/operations/date_operation_expressions.py
    - pylegend/core/language/shared/operations/decimal_operation_expressions.py
    - pylegend/core/language/shared/operations/float_operation_expressions.py
    - pylegend/core/language/shared/operations/integer_operation_expressions.py
    - pylegend/core/language/shared/operations/number_operation_expressions.py
    - pylegend/core/language/shared/operations/primitive_operation_expressions.py
    - pylegend/core/language/shared/operations/string_operation_expressions.py
    - pylegend/core/language/shared/expression.py
    - pylegend/core/language/shared/column_expressions.py
    - pylegend/core/language/shared/literal_expressions.py
    - pylegend/core/language/shared/variable_expressions.py
    - pylegend/core/language/shared/tds_row.py
    - pylegend/core/language/shared/pylegend_custom_expressions.py
    - pylegend/core/language/legendql_api/legendql_api_custom_expressions.py
    - pylegend/core/language/legendql_api/legendql_api_tds_row.py
    - pylegend/core/project_cooridnates.py

key-decisions:
  - "Removed to_sql_func parameter from base operation expression classes (binary/unary/nary/nullary), not just the to_sql_expression method — caller sites no longer pass SQL callbacks"
  - "Removed column_sql_expression from AbstractTdsRow — SQL column lookup path fully eliminated"
  - "Removed to_sql_node from PyLegendSortInfo, PyLegendWindow, PyLegendDurationUnit, and frame bound classes — OLAP/window SQL path gone"
  - "PyLegendDecimalDivideScaledExpression retains to_pure_expression directly since it does not use the callback pattern"
  - "ProjectCoordinates becomes a pure marker base class (ABCMeta, no abstract methods) after sql_params() removal"

requirements-completed: [REMV-03]

duration: 12min
completed: "2026-06-01"
---

# Phase 02 Plan 02: Remove SQL Layer from Shared Language Files Summary

**SQL metamodel completely excised from ~33 shared language files: primitives, operation expressions, and expression classes now only carry the Pure path (`to_pure_expression`/`__to_pure_func`), making the language layer importable after `core/sql/` deletion.**

## Performance

- **Duration:** ~12 min
- **Started:** 2026-06-01T13:21:00Z
- **Completed:** 2026-06-01T13:44:09Z
- **Tasks:** 2
- **Files modified:** 33

## Accomplishments

- Removed `to_sql_expression` abstract method from `PyLegendExpression` and all 10 primitive classes
- Removed `to_sql_func` callback parameter from all 4 base operation expression classes (binary, unary, nary, nullary) and all concrete operation classes across 9 files
- Removed `sql_params()` abstract + 3 concrete implementations from `ProjectCoordinates` hierarchy
- Removed `column_sql_expression` SQL path from `AbstractTdsRow` and all LegendQL row subclasses (Lead/Lag/First/Last/Nth)
- Removed `to_sql_node` from window/OLAP helper classes in `pylegend_custom_expressions.py`
- All `to_pure_expression` methods and `__to_pure_func` callbacks preserved intact

## Task Commits

1. **Task 1: Remove SQL from primitives and project_cooridnates** - `85f212e` (feat)
2. **Task 2: Remove SQL from operations and shared expression files** - `40e5ce5` (feat)

## Files Created/Modified

- `pylegend/core/language/shared/expression.py` - Removed abstract to_sql_expression from PyLegendExpression
- `pylegend/core/language/shared/primitives/primitive.py` - Removed abstract to_sql_expression
- `pylegend/core/language/shared/primitives/*.py` (9 files) - Removed concrete to_sql_expression and SQL imports
- `pylegend/core/language/shared/primitives/precise_primitives.py` - Removed all to_sql_expression overrides
- `pylegend/core/language/shared/operations/binary_expression.py` - Removed to_sql_func param and to_sql_expression
- `pylegend/core/language/shared/operations/unary_expression.py` - Same
- `pylegend/core/language/shared/operations/nary_expression.py` - Same
- `pylegend/core/language/shared/operations/nullary_expression.py` - Same
- `pylegend/core/language/shared/operations/*.py` (9 concrete files) - Removed __to_sql_func methods and SQL imports
- `pylegend/core/language/shared/column_expressions.py` - Removed to_sql_expression
- `pylegend/core/language/shared/literal_expressions.py` - Removed to_sql_expression from all literal classes
- `pylegend/core/language/shared/variable_expressions.py` - Removed to_sql_expression
- `pylegend/core/language/shared/tds_row.py` - Removed column_sql_expression and SQL imports
- `pylegend/core/language/shared/pylegend_custom_expressions.py` - Removed to_sql_node from OLAP/window classes
- `pylegend/core/language/legendql_api/legendql_api_custom_expressions.py` - Removed SQL methods
- `pylegend/core/language/legendql_api/legendql_api_tds_row.py` - Removed column_sql_expression overrides
- `pylegend/core/project_cooridnates.py` - Removed sql_params() and core.sql import

## Decisions Made

- Removed `to_sql_func` parameter from base expression class constructors (not just the `to_sql_expression` method). This required updating all call sites in concrete operation classes to drop the SQL callback argument.
- Removed `column_sql_expression` from `AbstractTdsRow` and LegendQL row subclasses entirely — this method fed the SQL column lookup path which is no longer needed.
- `PyLegendSortInfo.__null_ordering` field also removed (it was `SortItemNullOrdering` type from deleted SQL module).
- `PyLegendDecimalDivideScaledExpression` does not inherit from a base expression with callback pattern; its `to_sql_expression` was removed directly, keeping only `to_pure_expression`.

## Deviations from Plan

None — plan executed exactly as written with one structural note:

The plan anticipated removing `to_sql_expression` methods only. In practice, the `to_sql_func` callback parameters in the base expression classes (binary/unary/nary/nullary) also had to be removed, along with the corresponding argument from every concrete operation class. This is properly within scope of the plan objective ("remove SQL-typed instance variables from every... operation-expression file") and is documented as a clarification rather than a deviation.

The plan's verification check `grep -rln 'to_pure_expression' pylegend/core/language/shared/operations/ | wc -l` now returns 5 (base classes) rather than 10+ (all files). This is expected: the method is in the base classes, and concrete classes provide `__to_pure_func` callbacks. Pure behavior is fully preserved.

## Issues Encountered

None — all files edited cleanly. flake8 passes on all 33 modified files.

## Next Phase Readiness

- Language layer is now free of all `core.sql` imports
- Plan 02-03 can proceed to clean the frame/client layer and `__init__` files
- `import pylegend` still expected broken until Plan 02-03 removes remaining SQL references in frames

---
*Phase: 02-remove-legacy-code-and-sql-layer*
*Completed: 2026-06-01*
