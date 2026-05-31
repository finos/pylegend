# External Integrations

**Analysis Date:** 2026-05-31

## APIs & External Services

**Legend Engine:**
- Legend data management platform - Core service for query execution and data management
  - SDK/Client: `LegendClient` in `pylegend/core/request/legend_client.py`
  - Communication: HTTP REST API (Requests library)
  - Default port: 80 or 443 (configurable)
  - Default path: `/api` (configurable)

**Legend Engine Endpoints:**
- SQL Execution (`sql/v1/execution/execute`, `sql/v1/execution/schema`) - Execute SQL queries and retrieve schema
- Grammar API (`pure/v1/grammar/grammarToJson/model`) - Parse and compile Legend models
- Model Compilation (`pure/v1/grammar/jsonToGrammar/model`) - Convert model JSON to grammar format

## Data Storage

**Databases:**
- PostgreSQL - Via extension module (`pylegend/extensions/database/vendors/postgres/postgres_sql_to_string.py`)
  - Connection: testcontainers PostgreSQL for testing
  - Client: sqlalchemy 2.0.0+ (dev), pg8000 1.0.0+ (dev)
  - SQL generation: Custom SQL-to-string generator for database dialect
  
- MySQL - Via dev dependencies
  - Client: pymysql 1.0.0+ (dev)

- General database support framework in `pylegend/core/database/sql_to_string/` for extensible SQL generation per database type

**File Storage:**
- Local filesystem only - CSV and JSON file result handlers
  - CSV export: `pylegend/core/tds/result_handler/to_csv_file_result_handler.py` (uses ijson for streaming)
  - JSON export: `pylegend/core/tds/result_handler/to_json_file_result_handler.py`

**Caching:**
- None - No caching layer detected

## Authentication & Identity

**Auth Provider:**
- Custom authentication schemes in `pylegend/core/request/auth.py`:
  
  1. **LocalhostEmptyAuthScheme** - No authentication (localhost development)
     - Implementation: Returns None auth
  
  2. **HeaderTokenAuthScheme** - Token-based authentication
     - Header injection approach
     - Supports custom query parameters
     - Token provider callback pattern
  
  3. **CookieAuthScheme** - Cookie-based authentication
     - Cookie injection approach
     - Supports custom cookie parameters
     - Cookie provider callback pattern

**Auth Integration:**
- Uses `requests.auth.AuthBase` for pluggable authentication
- Configured at `LegendClient` initialization time

## Monitoring & Observability

**Error Tracking:**
- None detected in core library

**Logs:**
- Python logging module (`logging` standard library)
- LocalLegendEnv uses logging for startup messages (`pylegend/samples/local_legend_env.py`)

## CI/CD & Deployment

**Hosting:**
- PyPI - Python Package Index for package distribution
- GitHub - Source repository and release hosting

**CI Pipeline:**
- GitHub Actions (`.github/workflows/`)
  - Flake8 linting
  - MyPy type checking
  - pytest on Python 3.9-3.14, Ubuntu and Windows
  - CodeCov integration for coverage reports
  - Poetry build workflow

**Quality Gates:**
- Codecov: Coverage tracking and reporting
- WhiteSource: License compliance scanning (`.whitesource` configured)
- Sonar: Code quality analysis (`sonar-project.properties` present)
- Safety: Security vulnerability scanning (`.safety-policy.yml` configured)

**Deployment:**
- Release workflow publishes to PyPI via Poetry
- Triggered on GitHub release publication
- Uses PYPI_USERNAME and PYPI_PASSWORD secrets

## Environment Configuration

**Required env vars:**
- No hardcoded environment variables in library code
- Runtime configuration via LegendClient constructor parameters
- Legend Engine URL: Host and port must be provided explicitly

**Secrets location:**
- GitHub Actions secrets: `CODECOV_TOKEN`, `PYPI_USERNAME`, `PYPI_PASSWORD`
- No secrets committed to repository

## Webhooks & Callbacks

**Incoming:**
- None - Library is client-only

**Outgoing:**
- None - Library makes synchronous HTTP requests to Legend API

## Testing Infrastructure

**Docker Containers (via testcontainers):**
- Eclipse Temurin 11 JDK - Runs Legend Engine server JAR in tests (`pylegend/samples/local_legend_env.py`)
- PostgreSQL - For integration tests (`tests/extensions/database/vendors/postgres/test_postgres_sql_gen_e2e.py`)

**Legend Engine Test Setup:**
- Local metadata server: HTTP server hosting Legend metadata files (Python's http.server)
- Legend Engine JAR: Downloaded from Maven Central (version 4.121.0)
- Dynamic port allocation: Prevents port conflicts in parallel testing
- Max wait time: 120 seconds for service startup

---

*Integration audit: 2026-05-31*
