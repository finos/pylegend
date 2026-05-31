---
phase: 01-fix-pure-foundation
plan: 01
subsystem: test-server
tags:
  - phase-01
  - test-server
  - java
  - maven
  - legend-engine
dependency_graph:
  requires: []
  provides:
    - POST /api/pure/v1/execution/execute endpoint in test server
    - POST /api/pure/v1/execution/generatePlan endpoint in test server
  affects:
    - tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java
    - tests/resources/legend/server/pylegend-sql-server/target/pylegend-sql-server-1.0-shaded.jar (gitignored)
tech_stack:
  added: []
  patterns:
    - Dropwizard JAX-RS resource registration via environment.jersey().register()
    - Maven shaded JAR build with Java 17 (JDK at rattler cache path)
key_files:
  created: []
  modified:
    - tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java
decisions:
  - Added Execute import alphabetically before SqlExecute imports (line 46)
  - Registered Execute after GrammarToJson (line 127) — same constructor arguments as documented in RESEARCH.md Pattern 4
  - JAR not committed to git (target/ is gitignored per .gitignore)
  - Maven installed via pixi global install maven (pixi was already installed at 0.63.2)
  - Build used JAVA_HOME pointing to JDK 17 at rattler cache path as specified in CONTEXT.md D-05
metrics:
  duration: "247 seconds (~4 minutes)"
  completed: "2026-05-31"
  tasks: 2
  files_modified: 1
---

# Phase 01 Plan 01: Register Execute JAX-RS Resource and Build Test Server JAR Summary

Register the Legend engine `Execute` JAX-RS resource in `PyLegendSqlServer.java` and rebuild the shaded JAR, exposing `POST /api/pure/v1/execution/execute` and `POST /api/pure/v1/execution/generatePlan` in the test server (previously returning HTTP 404).

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Register Execute JAX-RS resource in PyLegendSqlServer.java | 1600c9b | PyLegendSqlServer.java (+1 import, +1 registration) |
| 2 | Build the test server shaded JAR with Maven | (no tracked files; JAR is gitignored) | target/pylegend-sql-server-1.0-shaded.jar (built, not committed) |

## Implementation Details

### Task 1: Java Changes

**New import** (line 46, alphabetically before SqlExecute imports):
```java
import org.finos.legend.engine.query.pure.api.Execute;
```

**New registration** (line 127, after GrammarToJson registration):
```java
environment.jersey().register(new Execute(modelManager, planExecutor, routerExtensions, generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers)));
```

All four constructor arguments were already in scope within `run()`. No new local variables needed.

### Task 2: Maven Build

**Maven install:** `pixi global install maven` was run (maven was not previously installed). Maven 3.9.16 installed.

**Build command:**
```bash
JAVA_HOME=/Users/deepyaman/Library/Caches/rattler/cache/pkgs/openjdk-17.0.17-h99a4030_0/lib/jvm \
  mvn -f tests/resources/legend/server/pylegend-sql-server/pom.xml clean package -DskipTests
```

**Build duration:** 90 seconds (Maven build itself)

**JAR output:**
- Path: `tests/resources/legend/server/pylegend-sql-server/target/pylegend-sql-server-1.0-shaded.jar`
- Size: 795 MB
- SHA256: `edfb954ec6614fc13980b9af5016f8664405005598f6d5deb12c083ae750ca42`

**JAR contents verified:**
- `org/finos/legend/pylegend/PyLegendSqlServer.class` — present
- `org/finos/legend/engine/query/pure/api/Execute.class` — present

### Boot Smoke Check

Server started on dynamic port 63625 with the rebuilt JAR:

| Endpoint | Method | HTTP Status | Expected |
|----------|--------|-------------|----------|
| `/api/server/v1/info` | GET | 200 | 200 |
| `/api/pure/v1/execution/execute` | POST (empty body) | 500 | non-404 |
| `/api/pure/v1/execution/generatePlan` | POST (empty body) | 500 | non-404 |

Both Pure endpoints return 500 (not 404) — the 500 is expected because an empty `{}` body is an invalid `ExecuteInput`. The routes are registered and reachable.

`git status --porcelain tests/resources/legend/server/pylegend-sql-server/target/` produced no output — target directory remains gitignored.

## Deviations from Plan

None — plan executed exactly as written.

The `pixi global install maven` step was required (Maven was not pre-installed), as anticipated by CONTEXT.md D-04 and the plan's action description.

## Known Stubs

None. This plan only registers a JAX-RS resource and builds a JAR — no stub values or placeholder data.

## Threat Flags

None. No new network surface introduced beyond what was documented in the plan's threat model. The `Execute` endpoint is already part of the legend-engine core and was already in the shaded JAR classpath; this plan only registers it in the Dropwizard environment.

## Self-Check: PASSED

- [x] `tests/resources/legend/server/pylegend-sql-server/src/main/java/org/finos/legend/pylegend/PyLegendSqlServer.java` modified with import (line 46) and registration (line 127)
- [x] Commit 1600c9b exists
- [x] JAR built successfully (795 MB at target/pylegend-sql-server-1.0-shaded.jar)
- [x] Both `PyLegendSqlServer.class` and `Execute.class` present in JAR
- [x] Smoke check: /api/server/v1/info returns 200, /api/pure/v1/execution/execute returns 500 (non-404)
- [x] target/ directory remains gitignored
