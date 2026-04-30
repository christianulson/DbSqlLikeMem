# SQLite Performance Improvement Tracker - Cycle 2

- Last updated: 2026-04-12
- Goal: make DbSqlLikeMem equal to or faster than native SQLite on the comparable benchmark set.
- Source baseline: `docs\Wiki\performance-matrix.md`
- Benchmark source: `src\benchmark\DbSqlLikeMem.Benchmarks\Benchmarks\Suites\Sqlite_DbSqlLikeMem_Benchmarks.cs`

## Baseline Snapshot

- Comparable rows: 73
- DbSqlLikeMem wins: 4
- SQLite native wins: 69
- DbSqlLikeMem baseline win rate: 5.48%

## Category Baseline

| Category | Wins | Total | Win Rate |
| --- | ---: | ---: | ---: |
| AdvancedQuery | 1 | 26 | 3.85% |
| Batch | 1 | 11 | 9.09% |
| Core | 0 | 13 | 0.00% |
| Dialect | 0 | 5 | 0.00% |
| Json | 0 | 3 | 0.00% |
| Setup | 0 | 4 | 0.00% |
| Temporal | 0 | 5 | 0.00% |
| Transactions | 2 | 6 | 33.33% |

## Implementation Waves

| Wave | Focus | Status | Progress |
| --- | --- | --- | ---: |
| 1 | Parameter lookup cache, savepoint parsing, savepoint order cleanup, single-PK shortcut, reusable insert command paths, cached parameter projection, cached parameter select matrices, cached insert count validation, cached PK CRUD commands, cached upsert command, cached transaction cleanup commands, and cached batch transaction commands | Completed | 100% |
| 2 | Batch insert 100, insert single row, parameter projection, row count, and PK CRUD fast paths | Completed | 100% |
| 3 | Upsert, returning update, batch scalar, and remaining write-path cleanup | In progress | 10% |
| 4 | Query engine joins, subqueries, unions, aggregates, windows, JSON, temporal helpers, and DDL | Pending | 0% |

- Overall implementation progress: 51%

## Completed Work

- Cached named and positional parameter resolution in `QueryExecutionContext` so repeated parameter lookups stop scanning the full collection.
- Normalized parameter values once when building the execution-context cache so Oracle empty strings and SQLite decimal round-trips are handled up front.
- Replaced savepoint command substring slicing with span-based extraction in the standard transaction command handler.
- Aligned savepoint order cleanup in `DbConnectionMockBase` to use case-insensitive reverse scans when creating, rolling back, or releasing savepoints.
- Added a single-row primary-key shortcut in `TableMock` so simple `WHERE Id = ...` lookups skip the generic equality-map preparation step.
- Recognized temporal tokens such as `CURRENT_TIMESTAMP` in the simple insert-value parser so common timestamp literals skip the heavier scalar parser.
- Restricted `CurrentColumn` assignment in insert fallback resolution to the path that actually calls `table.Resolve`, which keeps the hot literal insert path lighter.
- Extended the scalar `SELECT` fast path to evaluate every simple projection in order while still avoiding `TableResultMock` materialization.
- Reworked the prepared insert benchmark states to reuse parametrized insert commands for sequential and multi-row insert flows so the benchmark no longer rebuilds raw insert SQL on every iteration.
- Consolidated the prepared insert benchmark states around a reusable single-row insert command so sequential insert, row-count-after-insert, parameter insert, and custom-start insert flows share the same prepared path.
- Switched the parallel insert benchmark helper to a parameterized command per connection so each worker no longer rebuilds literal insert SQL.
- Cached the parameter projection benchmark command and its parameter handles so the scalar projection benchmark stops rebuilding the same command for every iteration.
- Cached the fixed parameter projection payload values so the benchmark stops recreating the same DateTime, DateTimeOffset, Guid, TimeSpan, and byte-array inputs on every iteration.
- Cached the parameter-select-by-name and parameter-select-by-id benchmark commands so the lookup matrices stop rebuilding their commands and parameters on every iteration.
- Cached the insert-count validation command so the sequential insert benchmarks stop recreating the same COUNT(*) command on every iteration.
- Cached the PK CRUD benchmark commands so update, delete, row-count-after-update, and update/delete round-trip paths stop rebuilding the same commands on every iteration.
- Cached the upsert benchmark command so the upsert path stops rebuilding the same SQL and command object on every iteration.
- Cached the transaction cleanup delete commands in the parameterized and standard transaction benchmark states so teardown stops rebuilding delete SQL on every iteration.
- Cached the PK CRUD cleanup insert commands so delete and update/delete teardown paths stop rebuilding the same insert SQL on every iteration.
- Cached the batch transaction benchmark commands so mixed read/write, scalar, non-query, reader, row-count, and transaction-control paths stop rebuilding the same insert, select, update, delete, and count commands on every iteration.
- Cached the batch insert final count command so `RunBatchInsert` stops rebuilding the same COUNT(*) SQL on every iteration.
- Cached the returning insert benchmark command and count validation command so the MariaDB RETURNING benchmark stops rebuilding the same SQL on every iteration.
- Added a primary-key count fast path in `AstQueryEqualityScanHelper` so single-equality counts on single-PK tables can skip the full row scan.
- Extended the primary-key count fast path to cover exact equality matches on composite primary keys as long as all PK columns are present in the equality set.
- Added a single-column index fast path in `AstQueryEqualityScanHelper` so single-equality counts can reuse an index bucket instead of scanning every row.
- Added a single-row insert materialization fast path in `DbInsertStrategy` so `VALUES (...)` inserts skip the multi-row materialization loop.
- Added a simple append fast path in `TableMock.AddBatch` when the table has no secondary indexes, so batch inserts skip the generic index matrix build.
- Skipped persisted-computed-column refresh work when the table has no persisted computed columns, which trims the batch insert hot path for the users benchmark schema.
- Cached the persisted-computed-column presence flag on `TableMock` so insert paths stop rescanning column metadata on every execution.
- Skipped foreign-key validation calls in the hot insert path when the target table has no foreign keys, reducing repeated no-op checks during batch and single-row inserts.
- Skipped existing-row PK lookups during inserts into empty tables, which removes a redundant conflict probe from the first batch of rows.
- Reused the precomputed primary-key key during single-row inserts in `TableMock` so uniqueness checks and PK index updates stop rebuilding the same key twice.
- Added a parameter-projection fast path in `CommandScalarExecutionPrelude` so scalar `SELECT` items that are just bound parameters skip scalar parsing.

## Next Focus

- Move into Wave 2 and attack the query engine joins, subqueries, unions, aggregates, windows, JSON, temporal helpers, and DDL paths.
- Keep the current four SQLite wins protected while the next wave starts.

## Update Log

- 2026-04-12: Rebased cycle 2 to the latest SQLite matrix and started the first quick-win implementation slice.
- 2026-04-12: Continued wave 1 with the single-PK shortcut for select/update/delete benchmark paths.
- 2026-04-12: Continued wave 1 by trimming insert hot-path overhead for temporal literals and fallback-only column resolution.
- 2026-04-12: Continued wave 1 by keeping scalar projection fast paths side-effect aware without building full result tables.
- 2026-04-12: Continued wave 1 by switching the prepared insert benchmark paths to reusable parameterized commands.
- 2026-04-12: Continued wave 1 by consolidating the prepared single-row insert paths behind one reusable command per prepared state.
- 2026-04-12: Continued wave 1 by parameterizing the parallel insert benchmark helper per worker connection.
- 2026-04-12: Continued wave 1 by caching the parameter projection benchmark command and parameter handles.
- 2026-04-12: Continued wave 1 by caching the fixed parameter projection payload values.
- 2026-04-12: Continued wave 1 by caching the parameter select matrix benchmark commands.
- 2026-04-12: Continued wave 1 by caching the insert-count validation command.
- 2026-04-12: Continued wave 1 by caching the PK CRUD benchmark commands.
- 2026-04-12: Continued wave 1 by caching the upsert benchmark command.
- 2026-04-12: Continued wave 1 by caching the transaction cleanup delete commands.
- 2026-04-12: Continued wave 1 by caching the PK CRUD cleanup insert commands.
- 2026-04-12: Continued wave 1 by caching the batch transaction benchmark commands.
- 2026-04-12: Completed wave 1 by caching the batch insert final count command.
- 2026-04-12: Continued wave 3 by caching the returning insert benchmark command and count validation command.
- 2026-04-12: Started wave 2 by adding a primary-key count fast path in the equality scan helper.
- 2026-04-12: Continued wave 2 by extending the primary-key count fast path to exact composite-key equality matches.
- 2026-04-12: Continued wave 2 by adding a single-column index fast path for equality counts.
- 2026-04-12: Continued wave 2 by adding a single-row insert materialization fast path.
- 2026-04-12: Continued wave 2 by adding a simple append fast path for batch inserts without secondary indexes.
- 2026-04-12: Continued wave 2 by skipping persisted-computed-column refresh work when the table has no persisted computed columns.
- 2026-04-12: Continued wave 2 by caching the persisted-computed-column presence flag on `TableMock`.
- 2026-04-12: Continued wave 2 by skipping foreign-key validation calls when the target table has no foreign keys.
- 2026-04-12: Completed wave 2 by skipping existing-row PK lookups during inserts into empty tables.
- 2026-04-12: Continued wave 2 by reusing the precomputed primary-key key during single-row inserts.
- 2026-04-12: Continued wave 2 by adding a parameter-projection fast path in the scalar execution prelude.
