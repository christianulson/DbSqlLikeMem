# SQLite Performance Improvement Tracker

- Last updated: 2026-04-12
- Goal: make DbSqlLikeMem equal to or faster than native SQLite on the comparable benchmark set.
- Source baseline: `docs\Wiki\performance-matrix.md`
- Benchmark source: `src\benchmark\DbSqlLikeMem.Benchmarks\Benchmarks\Suites\Sqlite_DbSqlLikeMem_Benchmarks.cs`
- Detailed progress log: `docs\features-backlog\sqlite-performance-improvement-tracker.md`

## Baseline Snapshot

- Comparable rows: 73
- DbSqlLikeMem wins: 6
- SQLite native wins: 67
- DbSqlLikeMem baseline win rate: 8.22%

## Category Baseline

| Category | Wins | Total | Win Rate |
| --- | ---: | ---: | ---: |
| AdvancedQuery | 1 | 26 | 3.85% |
| Batch | 1 | 11 | 9.09% |
| Core | 1 | 13 | 7.69% |
| Dialect | 0 | 5 | 0.00% |
| Json | 0 | 3 | 0.00% |
| Setup | 1 | 4 | 25.00% |
| Temporal | 0 | 5 | 0.00% |
| Transactions | 2 | 6 | 33.33% |

## Implementation Waves

| Wave | Focus | Status | Progress |
| --- | --- | --- | ---: |
| 1 | TableMock caches, ordered columns, identity cache, and raw index access in hot paths | Completed | 100% |
| 2 | Insert, update, delete, and batch execution fast paths | Completed | 100% |
| 3 | Query execution fast paths for joins, subqueries, unions, aggregates, and windows | Completed | 100% |
| 4 | String aggregate, JSON, temporal helpers, and transaction journal tuning | Completed | 100% |

- Overall implementation progress: 100%

## Completed Work

- Cached hot `TableMock` wrappers, ordered column access, and identity-column lookups.
- Reduced repeated scans in insert, update, delete, upsert, and batch execution paths.
- Added query execution fast paths for joins, subqueries, `UNION`, `DISTINCT`, and indexed counts.
- Tightened window partition sorting and correlated subquery cache-key handling.
- Optimized string aggregate, JSON, temporal, and transaction helper paths.

## Next Focus

- Benchmark the completed optimization set against the SQLite matrix and keep the wiki mirror in sync with the latest results.
- Watch for any regressions in the benchmark matrix before starting the next performance slice.

## Update Log

- 2026-04-12: Completed wave 3 with the remaining query execution fast paths aligned to the SQLite benchmark matrix.
- 2026-04-12: Completed wave 4 with the remaining string aggregate, JSON, temporal, and transaction helper optimizations.
