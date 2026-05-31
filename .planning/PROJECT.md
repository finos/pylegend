# PyLegend 2.0

## What This Is

PyLegend is a Python client library for the [FINOS Legend](https://legend.finos.org/) platform that enables users to build tabular dataset queries using a Python API, compile them to Pure (the Legend functional query language), and execute them against a Legend engine. Version 2.0 replaces the multi-API architecture with a single Ibis backend (`ibis.legend`), while preserving the LegendQL API as a backwards-compatible wrapper for existing users.

## Core Value

Internal library teams and Legend PCT tests can build and execute TDS queries against a Legend engine using familiar Python APIs without maintaining knowledge of the underlying Pure or engine protocol.

## Requirements

### Validated

- ✓ LegendQL API (`LegendQLApiTdsClient`) with TdsFrame operations (filter, restrict, extend, rename, groupBy, aggregate, join, and all current dataframe operations) — existing
- ✓ Pure expression generation via `to_pure_expression()` — existing
- ✓ Legend service and function frame inputs — existing
- ✓ HTTP client with auth schemes (`HeaderTokenAuthScheme`) — existing
- ✓ Streaming result handlers (CSV, Pandas DataFrame, string) — existing
- ✓ CI matrix across Python 3.9–3.14 on Ubuntu and Windows — existing

### Active

- [ ] Ibis backend registered and importable as `ibis.legend`
- [ ] Ibis backend compiles Ibis table expressions to Pure strings
- [ ] Ibis backend connects to Legend services (for organizations with internal instances)
- [ ] All TdsFrame operations currently in the LegendQL API continue to work via the Ibis backend
- [ ] LegendQL API reimplemented as a thin wrapper over the Ibis backend
- [ ] Legacy API removed
- [ ] Pandas API removed
- [ ] SQL compilation layer removed (not needed; Pure generation is the target output)
- [ ] Legend PCT matrix remains green after rewrite

### Out of Scope

- SQL generation — Pure generation is the only compilation target in 2.0
- Legacy API maintenance — being removed in this milestone
- Pandas API maintenance — being removed in this milestone
- Full `ibis-backends` test suite compliance — Pure/Legend is more limited than typical SQL backends; existing PyLegend operations must work, full suite compliance is deferred
- Pure output formatting options — keep generation simple, no pretty-print/indent config
- Expansion of supported operations beyond what currently works in PyLegend 1.x

## Context

- **Codebase:** See `.planning/codebase/` for full analysis. Current architecture has three parallel API layers (LegendQL, Legacy, Pandas) that all compile to a SQL metamodel, then to vendor SQL. The SQL layer is significant complexity with only the PostgreSQL vendor implemented.
- **Ibis reference:** `https://github.com/deepyaman/ibis-pandas` shows the pattern for registering a custom Ibis backend. The new backend should follow this registration pattern so it's available as `ibis.legend`.
- **PCT tests:** PyLegend appears in the Legend PCT (Protocol Conformance Test) matrix. The exact wiring is unclear but must be preserved. Investigate as part of Phase 1.
- **Pure language:** Capitalized as "Pure" (not PURE) — it is the Legend platform's functional query language, not an acronym. Verify against official Legend documentation during implementation.
- **Internal library users:** Organizations use an internal library that extends PyLegend's TdsFrame. Those users never interact with PyLegend directly — they call methods on TdsFrame subclasses. The method signatures and behavior of TdsFrame operations are the primary backwards-compatibility surface.

## Constraints

- **Backwards compatibility:** LegendQL API public interface must remain stable (method names, signatures, return types on TdsFrame and its operations). Internal implementation can change freely.
- **Python support:** Maintain Python 3.9–3.14 compatibility (existing CI matrix).
- **Ibis:** Must implement as a proper Ibis backend following Ibis's backend registration protocol.
- **Simplicity:** Prefer fewer abstractions over the current three-layer architecture. Removing the SQL metamodel layer is a desired simplification.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Remove Legacy and Pandas APIs first | Reduces scope; fewer API layers to reconcile during Ibis backend design | — Pending |
| Ibis backend as primary implementation | Industry-standard Python dataframe API; future-proofs PyLegend for ecosystem interop | — Pending |
| LegendQL API wraps Ibis backend | Avoids maintaining two separate query-building layers | — Pending |
| Pure generation only (drop SQL) | SQL compilation was unused complexity; Legend engine accepts Pure natively | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-05-31 after initialization*
