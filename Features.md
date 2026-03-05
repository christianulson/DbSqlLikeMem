# Features by Database and Version

> Looking for Portuguese? See [Funcionalidades.md](Funcionalidades.md).

> This file is kept for compatibility and quick NuGet/GitHub navigation. The canonical and fully maintained version lives at [`docs/old/providers-and-features.md`](docs/old/providers-and-features.md).

## Quick links

- [Portuguese version](Funcionalidades.md)
- [Canonical provider and feature documentation](docs/old/providers-and-features.md)
- [Getting started guide](docs/getting-started.md)
- [Root repository overview](README.md)

## Provider and simulated-version matrix

| Database | Package | Simulated versions |
| --- | --- | --- |
| MySQL | `DbSqlLikeMem.MySql` | 3, 4, 5, 8 |
| SQL Server | `DbSqlLikeMem.SqlServer` | 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022 |
| SQL Azure | `DbSqlLikeMem.SqlAzure` | 100, 110, 120, 130, 140, 150, 160, 170 |
| Oracle | `DbSqlLikeMem.Oracle` | 7, 8, 9, 10, 11, 12, 18, 19, 21, 23 |
| PostgreSQL (Npgsql) | `DbSqlLikeMem.Npgsql` | 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| SQLite | `DbSqlLikeMem.Sqlite` | 3 |
| DB2 | `DbSqlLikeMem.Db2` | 8, 9, 10, 11 |

## Common capabilities (all providers)

- Provider-specific ADO.NET mock connections/commands/transactions.
- SQL parser and executor for common DDL/DML paths.
- `WHERE` expressions with `AND`/`OR`, `IN`, `LIKE`, `IS NULL`, and parameters.
- `GROUP BY`/`HAVING` with aggregate functions (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) and alias-aware paths.
- `CASE WHEN` in projections and grouped scenarios.
- `CREATE VIEW` / `CREATE OR REPLACE VIEW` support.
- `CREATE TEMPORARY TABLE` support, including `AS SELECT` variants.
- Fluent schema definition and deterministic data seeding helpers.
- Standardized mock collation/coercion rules for deterministic text and numeric-string comparisons.

## Integration layers used in real test stacks

- Dapper-compatible query/command flows.
- EF Core integration packages with open-connection factories by provider.
- LinqToDB integration packages with open-connection factories by provider.
- NHibernate compatibility via `UserSuppliedConnectionProvider` and provider contract tests.

## Execution-plan diagnostics and telemetry (mock)

- Per-command execution plans available from `LastExecutionPlan` and `LastExecutionPlans`.
- Core metrics include `EstimatedCost`, `InputTables`, `EstimatedRowsRead`, `ActualRows`, `SelectivityPct`, `RowsPerMs`, and `ElapsedMs`.
- Plan output includes warning and recommendation metadata for troubleshooting in tests (for example, warning codes and index recommendations).

## Analytical SQL features (implemented in parser/executor)

- Window ranking/distribution functions such as `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK`, and `CUME_DIST`.
- Value window functions such as `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`.
- Window frame clauses with `ROWS`, `RANGE`, and `GROUPS` in supported dialect paths.

## Transaction and concurrency model (deterministic mock)

| Provider | Savepoint | Release savepoint | Isolation levels |
| --- | --- | --- | --- |
| MySQL | Yes | Yes | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQL Server | Yes | No (explicit not-supported behavior) | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQL Azure | Yes | Yes | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| Oracle | Yes | Yes | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| PostgreSQL (Npgsql) | Yes | Yes | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQLite | Yes | Yes | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| DB2 | Yes | Yes | `ReadCommitted`, `RepeatableRead`, `Serializable` |

- Savepoint operations use snapshots for consistent intermediate rollback semantics.
- `Commit` and `Rollback` follow deterministic snapshot cleanup/restore behavior.
- Concurrent operations are guarded by sync-root locking when `ThreadSafe = true`.

## Stored procedure execution (mock contract)

- `CommandType.StoredProcedure` execution with signature validation.
- Parameter directions: `Input`, `Output`, `InputOutput`, and `ReturnValue`.
- Required-input direction and nullability checks with clear exceptions.
- Dapper-compatible stored-procedure execution flow.

## Database-specific highlights

### MySQL

- `INSERT ... ON DUPLICATE KEY UPDATE`: supported.
- Index hints (`USE/IGNORE/FORCE INDEX`) parsed, with supported execution semantics and validations.

### SQL Server

- Version-aware dialect behavior in provider package.
- `RELEASE SAVEPOINT` intentionally standardized as unsupported.

### SQL Azure

- Version-aware Azure SQL behavior in provider package.
- Dedicated compatibility behavior for SQL Azure command/transaction flows.

### Oracle

- Version-aware dialect behavior in provider package.

### PostgreSQL (Npgsql)

- Version-aware dialect behavior in provider package.

### SQLite

- `WITH`/CTE: available (>= 3).
- `ON DUPLICATE KEY UPDATE`: not supported (SQLite uses `ON CONFLICT`).
- Null-safe operator `<=>`: not supported.
- JSON operators `->` and `->>`: supported in dialect parser.

### DB2

- `WITH`/CTE: available (>= 8).
- `MERGE`: available (>= 9).
- `FETCH FIRST`: supported.
- `LIMIT/OFFSET`: not supported in DB2 dialect.
- `ON DUPLICATE KEY UPDATE`: not supported.
- Null-safe operator `<=>`: not supported.
- JSON operators `->` and `->>`: not supported.
- Triggers on non-temporary tables are supported via `TableMock` (before/after insert, update, delete).
- Temporary tables (connection/global) do not execute triggers.

## Extensions (VS Code and Visual Studio)

The extensions support both classic test-generation and application-artifact workflows:

- Generate test classes (existing main action).
- Generate model classes.
- Generate repository classes.
- Configure templates from top action buttons.
- Consistency checks with visual status for missing/divergent/synced artifacts.

### Template tokens

- `{{ClassName}}`, `{{ObjectName}}`, `{{Schema}}`, `{{ObjectType}}`, `{{DatabaseType}}`, `{{DatabaseName}}`.

## Known limitations (current)

- `RETURNING` / `OUTPUT` do not fully materialize returned datasets across all dialects yet.
- `OPENJSON` currently runs in a simplified scalar subset (no full tabular projection with `WITH (...)`).
- `NULLS FIRST/LAST` remains dialect-gated and may throw explicit not-supported errors where unavailable.
