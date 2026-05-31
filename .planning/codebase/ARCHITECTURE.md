# Architecture

**Analysis Date:** 2026-05-31

## Pattern

Multi-API adapter with fluent frame builder for query construction. Three distinct APIs (Legacy, LegendQL, Pandas) share a common SQL compilation backend.

## Layers

1. **User Client Layer** — `LegendQLApiTdsClient`, `LegacyApiTdsClient` entry points
2. **Frame Abstraction Layer** — `PyLegendTdsFrame` abstract base with API-specific implementations
3. **Query Building Layer** — Expression types, aggregations, column/row operations
4. **SQL Compilation Layer** — SQL metamodel in `pylegend/core/sql/` with vendor-specific generators
5. **Request & Execution Layer** — `LegendClient` with auth schemes and retry logic
6. **Response Processing Layer** — Streaming result handlers (CSV, Pandas DataFrame, string)

## Data Flow

User builds frame chain (immutable, lazy operations) → `.to_pandas()` invoked → Frame compiles to SQL metamodel → SQL string generated per database vendor → `LegendClient` POSTs to Legend server → Streaming response parsed by `ResultHandler` → Output returned

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

**New Query API:**
- Create `pylegend/core/language/{api_name}/` with expression builders
- Create `pylegend/core/tds/{api_name}/frames/` with frame implementations
- Create extension entry point in `pylegend/extensions/tds/{api_name}/`

**New Database Vendor:**
- Create `pylegend/extensions/database/vendors/{vendor_name}/{vendor_name}_sql_to_string.py`
- Register in `SqlToStringGenerator.find_sql_to_string_generator_for_db_type()`

**New Output Format:**
- Create result handler in `pylegend/extensions/tds/result_handler/{format}_result_handler.py`
- Extend `ResultHandler` interface
