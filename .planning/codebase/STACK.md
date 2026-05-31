# Technology Stack

**Analysis Date:** 2026-05-31

## Languages

**Primary:**
- Python 3.9-3.14 - Core implementation language for entire project

## Runtime

**Environment:**
- CPython 3.9+ (primary)
- PyPy support (partial)

**Package Manager:**
- uv - Modern Python package manager for dependency management
- Lockfile: `uv.lock` (present)

## Frameworks

**Core:**
- Requests 2.27.1+ - HTTP client for Legend API communication (`pylegend/core/request/service_client.py`)
- ijson 3.1.4+ - Streaming JSON parser for large result handling (`pylegend/core/tds/result_handler/to_csv_file_result_handler.py`)

**Data Processing:**
- Pandas 1.0.0+ (Python <3.12) or 2.1.1+ (Python >=3.12) - Data manipulation and tabular operations (`pylegend/core/language/pandas_api/`)
- NumPy 1.20.0+ (Python <3.12) or 1.26.0+ (Python >=3.12) - Numerical computation

**Testing:**
- pytest 7.0.0-9.0.0 - Test framework and runner
- pytest-cov 3.0.0+ - Coverage reporting integration with pytest
- testcontainers 3.0.0+ - Docker container management for integration tests (`pylegend/samples/local_legend_env.py`)

**Build/Dev:**
- uv_build 0.11.2-0.12.0 - Build backend for package compilation

## Key Dependencies

**Critical:**
- requests 2.27.1+ - Handles all HTTP communication with Legend API, manages sessions with retry logic (`pylegend/core/request/service_client.py` uses HTTPAdapter with Retry policy)
- ijson 3.1.4+ - Streaming JSON parsing for large response bodies to avoid memory overflow
- pandas/numpy - Data transformation and numeric operations for query results

**Infrastructure:**
- testcontainers 3.0.0+ - Docker-based test environment setup (`pylegend/samples/local_legend_env.py` uses DockerContainer for Legend Engine)

**Database Testing (dev-only):**
- sqlalchemy 2.0.0+ - ORM for database schema definition and query building
- pg8000 1.0.0+ - Pure-Python PostgreSQL driver
- pymysql 1.0.0+ - Pure-Python MySQL driver
- cryptography 40.0.0+ - SSL/TLS support for database connections

**Type Checking (dev-only):**
- types-requests 2.28.0+ - Type stubs for requests library
- pandas-stubs 1.5.0+ - Type stubs for pandas library

**Testing (dev-only):**
- mockito 1.0.0+ - Mocking framework for unit tests

## Configuration

**Environment:**
- Runtime configuration via Legend API client initialization (`pylegend/core/request/legend_client.py`):
  - Host and port configuration
  - Authentication scheme selection (LocalhostEmptyAuthScheme, HeaderTokenAuthScheme, CookieAuthScheme)
  - Secure HTTP flag (HTTPS/HTTP)
  - API path prefix (default: `/api`)
  - Retry count configuration (default: 2)

**Build:**
- `pyproject.toml` - Project metadata, dependencies, version (1.1.1)
- `uv.lock` - Dependency lock file for reproducible builds

## Platform Requirements

**Development:**
- Python 3.9+ interpreter
- Docker (for testcontainers-based testing)
- uv package manager

**Production:**
- Python 3.9-3.14 runtime
- No Docker required
- Minimal dependencies: requests, ijson, pandas, numpy

---

*Stack analysis: 2026-05-31*
