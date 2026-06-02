# Coding Conventions

**Analysis Date:** 2026-05-31

## Naming Patterns

**Files:**
- Module files use `snake_case`: `legacy_api_tds_client.py`, `tds_column.py`, `sql_to_string.py`
- Test files follow pattern `test_*.py` (e.g., `test_tds_column.py`, `test_legacy_api_tds_client.py`)

**Classes:**
- Classes use `PascalCase`: `TdsColumn`, `PrimitiveTdsColumn`, `EnumTdsColumn`, `LegacyApiTdsClient`
- Abstract base classes use `ABCMeta`: `pylegend/core/tds/tds_column.py`
- Test classes: `TestTdsColumn`, `TestLiteralExpressions`

**Functions and Methods:**
- Functions/methods use `snake_case`: `get_name()`, `copy_with_changed_name()`, `legend_service_frame()`
- Factory methods: `{type}_column()` — `integer_column()`, `float_column()`, `string_column()`
- Private instance variables: double underscore prefix `self.__name`, `self.__type`
- Public getters: `get_*()` pattern

**Variables:**
- Instance variables: `snake_case`
- Private variables: `__prefix`
- Constants: `UPPER_SNAKE_CASE` (e.g., `LOGGER`)
- Type variables: `PascalCase` (e.g., `R = PyLegendTypeVar('R')`)

## Code Style

**Formatting:**
- Max line length: 127 characters (flake8)
- Indentation: 4 spaces

**Linting:**
- Tool: `flake8`
- Custom checker: `pylegend_copyright_checker` for Apache 2.0 headers
- Configuration: `.github/workflows/actions/flake8_lint_check/action.yml`

**Type Checking:**
- Tool: `mypy` (strict mode)
- Configuration: `.github/workflows/typing/config.cfg`
- All parameters and return types must be explicitly annotated
- Custom typing aliases in `pylegend._typing`: `PyLegendList`, `PyLegendDict`, `PyLegendSequence`, `PyLegendOptional`

## Import Organization

**Order:**
1. Standard library
2. Third-party (requests, pandas, ijson, numpy)
3. Local pylegend imports

**Pattern:**
- Use custom typing module: `from pylegend._typing import PyLegendList`
- Every module defines `__all__: PyLegendSequence[str]`
- Public APIs centralized in `__init__.py` files

## Error Handling

**Patterns:**
- Generic exception wrapping: `except Exception as e: raise RuntimeError("Error message", e)`
- Two-argument `RuntimeError` for chained exceptions
- See `pylegend/core/tds/tds_column.py`

## Logging

- Module-level logger: `LOGGER = logging.getLogger(__name__)`
- Info level for lifecycle messages

## Comments

**Docstrings:**
- One-line docstrings for type-casting functions
- Example: `"""Cast to Boolean."""` in `pylegend/core/language/type_factory.py`

**TODOs:**
- Marked with `# TODO:` in `pylegend/core/database/sql_to_string/db_extension.py`

## Function Design

- All parameters type-annotated; return types explicitly declared
- Optional params: `PyLegendOptional[T]`
- Methods typically 5–30 lines, single responsibility
- Factory methods often one-liners

## Module Design

- Every module has `__all__: PyLegendSequence[str]` at top level
- Central public API at `pylegend/__init__.py`; each submodule re-exports locally

## Copyright

- Every file: Apache 2.0 header (14 lines), Goldman Sachs copyright
- Enforced by `pylegend_copyright_checker` flake8 plugin
