<!-- GSD:project-start source:PROJECT.md -->

## Project

**PyLegend 2.0**

PyLegend is a Python client library for the [FINOS Legend](https://legend.finos.org/) platform that enables users to build tabular dataset queries using a Python API, compile them to Pure (the Legend functional query language), and execute them against a Legend engine. Version 2.0 replaces the multi-API architecture with a single Ibis backend (`ibis.legend`), while preserving the LegendQL API as a backwards-compatible wrapper for existing users.

**Core Value:** Internal library teams and Legend PCT tests can build and execute TDS queries against a Legend engine using familiar Python APIs without maintaining knowledge of the underlying Pure or engine protocol.

### Constraints

- **Backwards compatibility:** LegendQL API public interface must remain stable (method names, signatures, return types on TdsFrame and its operations). Internal implementation can change freely.
- **Python support:** Maintain Python 3.9–3.14 compatibility (existing CI matrix).
- **Ibis:** Must implement as a proper Ibis backend following Ibis's backend registration protocol.
- **Simplicity:** Prefer fewer abstractions over the current three-layer architecture. Removing the SQL metamodel layer is a desired simplification.

<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->

## Technology Stack

## Languages

- Python 3.9-3.14 - Core implementation language for entire project

## Runtime

- CPython 3.9+ (primary)
- PyPy support (partial)
- uv - Modern Python package manager for dependency management
- Lockfile: `uv.lock` (present)

## Frameworks

- Requests 2.27.1+ - HTTP client for Legend API communication (`pylegend/core/request/service_client.py`)
- ijson 3.1.4+ - Streaming JSON parser for large result handling (`pylegend/core/tds/result_handler/to_csv_file_result_handler.py`)
- Pandas 1.0.0+ (Python <3.12) or 2.1.1+ (Python >=3.12) - Data manipulation and tabular operations (`pylegend/core/language/pandas_api/`)
- NumPy 1.20.0+ (Python <3.12) or 1.26.0+ (Python >=3.12) - Numerical computation
- pytest 7.0.0-9.0.0 - Test framework and runner
- pytest-cov 3.0.0+ - Coverage reporting integration with pytest
- testcontainers 3.0.0+ - Docker container management for integration tests (`pylegend/samples/local_legend_env.py`)
- uv_build 0.11.2-0.12.0 - Build backend for package compilation

## Key Dependencies

- requests 2.27.1+ - Handles all HTTP communication with Legend API, manages sessions with retry logic (`pylegend/core/request/service_client.py` uses HTTPAdapter with Retry policy)
- ijson 3.1.4+ - Streaming JSON parsing for large response bodies to avoid memory overflow
- pandas/numpy - Data transformation and numeric operations for query results
- testcontainers 3.0.0+ - Docker-based test environment setup (`pylegend/samples/local_legend_env.py` uses DockerContainer for Legend Engine)
- sqlalchemy 2.0.0+ - ORM for database schema definition and query building
- pg8000 1.0.0+ - Pure-Python PostgreSQL driver
- pymysql 1.0.0+ - Pure-Python MySQL driver
- cryptography 40.0.0+ - SSL/TLS support for database connections
- types-requests 2.28.0+ - Type stubs for requests library
- pandas-stubs 1.5.0+ - Type stubs for pandas library
- mockito 1.0.0+ - Mocking framework for unit tests

## Configuration

- Runtime configuration via Legend API client initialization (`pylegend/core/request/legend_client.py`):
- `pyproject.toml` - Project metadata, dependencies, version (1.1.1)
- `uv.lock` - Dependency lock file for reproducible builds

## Platform Requirements

- Python 3.9+ interpreter
- Docker (for testcontainers-based testing)
- uv package manager
- Python 3.9-3.14 runtime
- No Docker required
- Minimal dependencies: requests, ijson, pandas, numpy

<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->

## Conventions

## Naming Patterns

- Module files use `snake_case`: `legacy_api_tds_client.py`, `tds_column.py`, `sql_to_string.py`
- Test files follow pattern `test_*.py` (e.g., `test_tds_column.py`, `test_legacy_api_tds_client.py`)
- Classes use `PascalCase`: `TdsColumn`, `PrimitiveTdsColumn`, `EnumTdsColumn`, `LegacyApiTdsClient`
- Abstract base classes use `ABCMeta`: `pylegend/core/tds/tds_column.py`
- Test classes: `TestTdsColumn`, `TestLiteralExpressions`
- Functions/methods use `snake_case`: `get_name()`, `copy_with_changed_name()`, `legend_service_frame()`
- Factory methods: `{type}_column()` — `integer_column()`, `float_column()`, `string_column()`
- Private instance variables: double underscore prefix `self.__name`, `self.__type`
- Public getters: `get_*()` pattern
- Instance variables: `snake_case`
- Private variables: `__prefix`
- Constants: `UPPER_SNAKE_CASE` (e.g., `LOGGER`)
- Type variables: `PascalCase` (e.g., `R = PyLegendTypeVar('R')`)

## Code Style

- Max line length: 127 characters (flake8)
- Indentation: 4 spaces
- Tool: `flake8`
- Custom checker: `pylegend_copyright_checker` for Apache 2.0 headers
- Configuration: `.github/workflows/actions/flake8_lint_check/action.yml`
- Tool: `mypy` (strict mode)
- Configuration: `.github/workflows/typing/config.cfg`
- All parameters and return types must be explicitly annotated
- Custom typing aliases in `pylegend._typing`: `PyLegendList`, `PyLegendDict`, `PyLegendSequence`, `PyLegendOptional`

## Import Organization

- Use custom typing module: `from pylegend._typing import PyLegendList`
- Every module defines `__all__: PyLegendSequence[str]`
- Public APIs centralized in `__init__.py` files

## Error Handling

- Generic exception wrapping: `except Exception as e: raise RuntimeError("Error message", e)`
- Two-argument `RuntimeError` for chained exceptions
- See `pylegend/core/tds/tds_column.py`

## Logging

- Module-level logger: `LOGGER = logging.getLogger(__name__)`
- Info level for lifecycle messages

## Comments

- One-line docstrings for type-casting functions
- Example: `"""Cast to Boolean."""` in `pylegend/core/language/type_factory.py`
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

<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->

## Architecture

## Pattern

## Layers

## Data Flow

## Key Entry Points

- `pylegend/legendql_api_tds_client.py` — LegendQL client factory
- `pylegend/legacy_api_tds_client.py` — Legacy API client factory
- `pylegend/core/tds/pandas_api/frames/pandas_api_input_tds_frame.py` — Pandas API frame constructors

## Abstractions

- `PyLegendTdsFrame` — base class for all frame types
- `SqlToStringGenerator` — base class for vendor SQL generators; dispatches by DB type
- `ResultHandler` — interface for streaming response parsing
- `LegendClient` — HTTP client abstraction supporting multiple auth schemes

## Where to Add New Code

- Create `pylegend/core/language/{api_name}/` with expression builders
- Create `pylegend/core/tds/{api_name}/frames/` with frame implementations
- Create extension entry point in `pylegend/extensions/tds/{api_name}/`
- Create `pylegend/extensions/database/vendors/{vendor_name}/{vendor_name}_sql_to_string.py`
- Register in `SqlToStringGenerator.find_sql_to_string_generator_for_db_type()`
- Create result handler in `pylegend/extensions/tds/result_handler/{format}_result_handler.py`
- Extend `ResultHandler` interface

<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->

## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->

## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:

- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->

<!-- GSD:profile-start -->

## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
