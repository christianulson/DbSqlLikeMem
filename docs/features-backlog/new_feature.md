# Backlog Proposal: Introduce an ADO.NET Interception Pipeline

## Context

Recent feedback from the .NET data-access community suggested that the most broadly useful direction for this project may be the introduction of a composable interception layer over ADO.NET providers.

This idea is conceptually similar to the design patterns used by:

* `HttpClient` + `HttpClientHandler`
* ASP.NET middleware pipelines
* OpenTelemetry instrumentation

Instead of focusing exclusively on emulating databases in memory, the project could provide an interception layer that composes behaviors around existing providers.

## Goal

Introduce an **ADO.NET interception pipeline** that allows behaviors to be layered around `DbConnection`, `DbCommand`, and related objects.

This pipeline should allow libraries to intercept and modify database interactions in a composable way.

## Proposed Architecture

Application

↓

ADO.NET Interception Pipeline

↓

Provider

* SqlClient
* Npgsql
* MySqlConnector
* Sqlite
* others

↓

Database

* Real database
* OR DbSqlLikeMem in-memory engine

## Key Idea

Both approaches should coexist:

1. **Provider Interception Mode**

Real provider → real database

Used for:

* telemetry
* latency simulation
* fault injection
* retry experimentation
* query inspection

2. **In-Memory Engine Mode**

DbSqlLikeMem engine replaces the provider

Used for:

* deterministic unit tests
* cross-dialect SQL validation
* provider behavior simulation
* fast CI test suites

## Example Use Cases

### Fault Injection

Simulate:

* connection drops
* transient errors
* command failures

Useful for testing retry logic.

### Latency Simulation

Artificial delays on command execution to test timeouts and resilience.

### Query Inspection

Intercept commands to:

* log SQL
* analyze query patterns
* produce recommendations

### Telemetry

Integrate with:

* OpenTelemetry
* DiagnosticListener
* Activity tracing

## Implementation Exploration

Possible components:

* `DbConnectionInterceptor`
* `DbCommandInterceptor`
* `DbExecutionPipeline`
* `DbConnectionHandler` (similar to `DelegatingHandler`)

Example concept:

Application

↓

LoggingHandler

↓

FaultInjectionHandler

↓

LatencySimulationHandler

↓

ProviderAdapter

↓

Real provider OR DbSqlLikeMem

## First Targets

Initial provider support could focus on the most common ADO.NET providers:

* SqlClient
* Npgsql
* MySqlConnector
* Sqlite

Additional providers could be added by the community.

## Expected Benefits

* Extensible provider instrumentation
* Controlled fault injection
* Improved testing of resilience logic
* Better diagnostics and observability
* Optional integration with the DbSqlLikeMem engine

## Open Questions

* What is the minimal abstraction layer needed over DbConnection?
* How can provider-specific features be supported without breaking portability?
* Should the pipeline integrate with existing .NET diagnostics infrastructure?

## Outcome

If successful, this could evolve into a general-purpose **ADO.NET interception framework** that complements the existing DbSqlLikeMem engine rather than replacing it.
