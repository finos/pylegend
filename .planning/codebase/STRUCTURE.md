# Directory Structure

**Analysis Date:** 2026-05-31

## Layout

```
pylegend/
├── __init__.py                   # Public API exports
├── legendql_api_tds_client.py    # LegendQL client entry point
├── legacy_api_tds_client.py      # Legacy API client entry point
├── _typing.py                    # Custom typing aliases
├── core/                         # Core abstractions
│   ├── language/                 # Type system, primitives, expressions (3 API implementations)
│   ├── tds/                      # Frame abstractions & result handlers
│   ├── request/                  # HTTP client, auth, service communication
│   ├── sql/                      # SQL metamodel classes
│   ├── database/                 # SQL generation base classes
│   └── project_cooridnates.py    # Project coordinate types
├── extensions/                   # Vendor-specific & advanced implementations
│   ├── database/vendors/         # Postgres SQL generator
│   └── tds/                      # Advanced frame types, result handlers
├── samples/                      # Usage examples for each API
└── utils/                        # Utilities (port generation, class helpers)

tests/
├── conftest.py                   # Pytest fixtures, Legend test server setup
├── test_legacy_api_tds_client.py
├── core/                         # Tests mirroring pylegend/core/
├── extensions/                   # Tests mirroring pylegend/extensions/
├── resources/                    # Test data & Legend server config
│   └── legend/
│       ├── server/               # Legend SQL Server JAR
│       └── *.json                # Metadata files
└── utils/

.github/workflows/
├── build-ci.yml                  # Main CI matrix (lint, type-check, test, build, docker, docs)
└── actions/
    ├── flake8_lint_check/
    ├── pytest/
    └── typing/
```

## Key Locations

| Purpose | Path |
|---------|------|
| Public Python API | `pylegend/__init__.py` |
| LegendQL client | `pylegend/legendql_api_tds_client.py` |
| Legacy client | `pylegend/legacy_api_tds_client.py` |
| Core TDS frames | `pylegend/core/tds/` |
| SQL metamodel | `pylegend/core/sql/` |
| SQL generators | `pylegend/extensions/database/vendors/` |
| Result handlers | `pylegend/extensions/tds/result_handler/` |
| HTTP/auth | `pylegend/core/request/` |
| Test fixtures | `tests/conftest.py` |
| Test data | `tests/resources/` |

## Naming Conventions

- Source files: `snake_case.py`
- Test files: `test_{module_name}.py`
- Classes: `PascalCase`, API-prefixed for disambiguation (e.g., `LegendQLApiLegendServiceInputFrame`)
- Abstract base classes: use `ABCMeta`
- Private instance variables: `__double_underscore` prefix
- Public getters: `get_*()` pattern
- Module exports: every `__init__.py` defines `__all__: PyLegendSequence[str]`
