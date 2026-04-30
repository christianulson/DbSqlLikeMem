# SQLite Performance Improvement Tracker

- Last updated: 2026-04-12
- Goal: make DbSqlLikeMem equal to or faster than native SQLite on the comparable benchmark set.
- Source baseline: `docs\Wiki\performance-matrix.md`
- Benchmark source: `src\benchmark\DbSqlLikeMem.Benchmarks\Benchmarks\Suites\Sqlite_DbSqlLikeMem_Benchmarks.cs`

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

- Cached the public read-only wrappers in `TableMock` so hot paths stop allocating them repeatedly.
- Added ordered column access and identity-column caching for insert and clone operations.
- Switched hot PK and index lookups to raw/internal collections where the concrete table type is already known.
- Updated seed and clone paths to reuse ordered column metadata instead of sorting columns repeatedly.
- Added a self-referencing foreign key cache so batch insert avoids scanning foreign key metadata on every call.
- Fast-pathed insert value resolution for literal and parameter AST nodes before falling back to the generic resolver.
- Replaced `REPLACE` conflict discovery with direct PK and unique-index lookups instead of row-by-row scans.
- Switched update unique-key change detection to the cached unique-index list.
- Tightened PK-equivalent index-name resolution to use direct ordinal column lookup.
- Resolved target-side `WHERE` equality filters once per statement in update/delete-from-select paths.
- Removed remaining `First(...)`/`FirstOrDefault(...)` fallbacks from the hot insert and update key-lookup helpers.
- Replaced sequential FK scan lambdas with tight loops in delete and schema validation paths.
- Cached FK lookup plans by target-table index version so insert/delete validation stops rediscovering the same lookup index on every row.
- Removed `Split(...)` and `Select(...)` allocations from the hot insert row builders and moved per-row limits out of the inner loops.
- Replaced the remaining delete parallel FK scan LINQ with an explicit `Parallel.For` loop and removed LINQ fallbacks from `TableMock` PK lookup and `WHERE` parsing helpers.
- Removed the remaining `Split(...).Select(...).Take(2)` parsing from hot insert/duplicate-name helpers so excluded/value column resolution stays allocation-light.
- Replaced the insert-replace conflict materialization with loops and removed the last `Split('.').Last()` helpers from insert expression parsing.
- Replaced the simple `WHERE` parser with a span-based fast path and moved PK shortcut matching to a dictionary lookup by normalized column name.
- Removed the remaining row-snapshot allocation from delete matching when affected-row snapshots are not requested.
- Reused the pending unique-key buffer in batch insert conflict tracking so the hot path stops allocating one key array per row.
- Replaced the `UPDATE/DELETE ... FROM/USING` equality-list parsing with span-based scanning and removed the regex split from the join-condition extractor.
- Switched update key-change detection to a `HashSet` lookup so the indexed-key path stops doing linear `Contains` scans.
- Precomputed target column indexes and reserved output capacity for `INSERT ... SELECT` row projection.
- Reduced `SetColValue` overhead on the common literal/parameter path by only setting `CurrentColumn` for the fallback resolver branch.
- Added a simple quoted-string fast path to insert value normalization so common literals skip the tokenizer.
- Applied the same quoted-string fast path to insert conflict/assignment normalization paths.
- Added a comma-split fast path for raw parser helpers when the block contains no quotes or nested parentheses.
- Added a shared simple-value parser that fast-paths quoted strings, booleans, null, parameters, hex literals, and common numerics.
- Removed repeated trimming in the hot simple-value helpers by adding trimmed variants for insert values and assignment parsing.
- Added one-pass comma splitting for quote-free raw blocks and a single-item fast path for comma-free blocks.
- Applied trimmed simple-value normalization to ON CONFLICT DO UPDATE assignments as well.
- Switched the hot raw clause readers to slice the original SQL text instead of rebuilding strings from token buffers.
- Switched comma-separated raw item parsing to slice the original SQL text instead of rebuilding strings from token buffers.
- Memoized window-spec cache keys by reference and pre-sized the key builder to cut repeated work in window-slot grouping.
- Removed the single-column array allocation from window partition key building so common window partitions stay lighter per row.
- Cached the ordered outer-row field view used by correlated subquery cache-key building so repeated EXISTS/IN/COUNT lookups reuse the same field list.
- Removed redundant cache lookups in the subquery evaluation helpers so repeated EXISTS/IN/COUNT access stays on the single get-or-add path.
- Cached the correlated subquery canonical SQL per `SqlSelectQuery` so repeated EXISTS/COUNT pre-aggregation paths stop rebuilding the same normalized query shape.
- Cached the filtered outer-row field view per subquery SQL so repeated correlated cache-key building stops re-running the same identifier matching work.
- Added a single-column fast path for correlated composite-key building so common EXISTS/COUNT comparisons skip the StringBuilder path.
- Added a lazy single-column ORDER BY cache path in window partition sorting so `ROW_NUMBER`/`NTILE` avoid precomputing unused order values.
- Cached correlated subquery cache keys per `EvalRow` so repeated lookup/comparison calls reuse the same final key string.
- Added a single-column peer-group path in window partition execution so ranking and percentile functions compare one value directly.
- Added a single-order frame-range cache path in window partition execution so peer-group and frame calculations reuse one value per row.
- Simplified single-order window partition sorting so it no longer materializes order-value arrays that the downstream window cache does not need.
- Added a single-equality left join lookup path so common anti-join and equi-join patterns stop scanning the entire right side for each left row.
- Reused the shared lookup key formatter in the join fast path so the equality lookup path stays aligned with the correlated subquery cache key encoding.
- Reused the index lookup path for simple equality-based scalar subquery counts and EXISTS checks so filtered subqueries stop scanning the whole source when an index already matches the predicate.
- Added a direct simple-column fast path for `IN` subquery value materialization so the benchmark can skip `ExecuteSelect` and project values straight from the source rows.
- Added a simple `UNION` count fast path for two-part projections so `UNION ALL` and `UNION DISTINCT` count benchmarks can avoid building the combined result table.
- Added a dedicated `UNION DISTINCT` count branch that deduplicates rows with a hash set instead of materializing a combined result table.
- Reused the indexed row-count helpers inside the union and correlated subquery fast paths so count-style execution stops rebuilding intermediate row sets.
- Added a simple first-column subquery fast path for plain column and identifier projections so correlated value lookups can reuse indexed source rows when available.
- Added a scalar `COUNT(*)` subquery fast path for simple equality-only cases so count evaluation can reuse indexed source rows before falling back to full subquery execution.
- Added partition-aware row-count helpers on `Source` so indexed counts can skip row enumeration while still honoring requested partition filters.
- Reduced correlated EXISTS/COUNT cache-key synthesis so the pre-aggregation path reuses canonical SQL memoization instead of rebuilding lookup state keys repeatedly.
- Extended the indexed count shortcut to honor MySQL index hints and primary-key equivalents before falling back to a full scan.
- Routed the shared subquery source builder through the index helper so correlated row materialization can skip a full scan when a filtered index path is available.
- Added a single-column fast path in `ApplyDistinct` so one-column `DISTINCT` projections skip the generic multi-column key builder.
- Added a cached JSON path normalization fast path so repeated JSON extraction calls stop re-normalizing the same path string.
- Cached zero-argument temporal function results per execution context so repeated `CURRENT_TIMESTAMP` / `NOW` / `GETDATE` calls stop re-evaluating the same provider-specific value.
- Removed the redundant uppercase conversion from temporal unit resolution so `DATEADD`/`TIMESTAMPADD` paths stop normalizing units twice.
- Reused a cached `StringBuilder` in string aggregate output paths so `GROUP_CONCAT` / `STRING_AGG` / `LISTAGG` stop allocating a fresh builder on every execution.
- Split the hot string aggregate append loops by separator presence so common custom-separator paths stop paying an inner-branch check on every appended row.
- Added a direct simple-path JSON lookup fast path so `JSON path read` can skip the generic path spec and resolver pipeline for property/index chains.
- Replaced the simple JSON array index parser with a manual digit scan so bracketed index paths skip `int.TryParse` on the hot path.
- Removed the unnecessary clone from scalar JSON path reads so `JSON scalar read` can convert the looked-up element directly.
- Removed redundant locking from transaction wrapper methods so commit/savepoint/rollback operations do not lock the same monitor twice.
- Replaced savepoint list lambdas with direct loops so rollback/release paths stop allocating delegate-based searches.
- Removed the extra clone from JSON element reads so `JSON_QUERY`/`JSON_VALUE` style path lookups can inspect the matched element directly.
- Extracted constant string aggregate separators directly from the AST so `STRING_AGG` / `GROUP_CONCAT` / `LISTAGG` skip the first-row evaluator when the separator is literal.
- Skipped separator appends entirely when string aggregate uses an empty separator so `LISTAGG` and similar paths stop paying a no-op append per item.
- Pre-sized the distinct hash set for string aggregates so `STRING_AGG DISTINCT` and similar grouped deduplication paths avoid rehashing as the group grows.
- Removed the rollback savepoint index array allocation so restoring indexes after journal replay can iterate the touched table set directly.
- Increased the string aggregate capacity estimate so large-group concatenation paths expand the builder less often.
- Replaced the global temporary table clone LINQ projections with loop-based column and row materialization to cut allocation on transaction/temp-table copies.
- Reused direct dictionary copying for temporary-table row clones and avoided array spreading when cloning index include columns.
- Removed redundant trimming and substring work from the simple-value parser and insert row materializer so already normalized batch values skip extra per-item scans.
- Simplified `ON CONFLICT WHERE` parameter resolution to reuse the execution-context resolver instead of scanning the parameter collection manually.
- Aligned `ON DUPLICATE KEY UPDATE` expression evaluation with the same central parameter resolver so the upsert hot path stops duplicating parameter scans.
- Removed the intermediate candidate array from `TryResolveParameter` so the shared parameter resolver now does a single scan without per-call allocation.
- Removed the positional-parameter list allocation from `TryResolveNextPositionalParameter` so `?` placeholders resolve with a single pass over the bound parameters.
- Reused the same `WHERE` context across rows in `UPDATE` and `DELETE` when the simple predicate has no positional placeholders, cutting per-row `Fork()` churn.
- Avoided an unconditional `Trim()` in `UPDATE SET` resolution so already normalized assignment text skips an extra per-item scan.
- Replaced the query-index coverage check with an explicit loop and removed the LINQ-based key projection from the non-`TableMock` index lookup path.
- Added a direct row-count fast path for indexed `COUNT(*)`-style queries so the executor can count matching rows without materializing row dictionaries.
- Replaced the brace-list JSON path normalization with a manual segment loop so common JSON path rewrites skip LINQ allocations.
- Replaced temporal `Trim()` calls with span-based comparisons in Firebird zero-arg detection and offset parsing.
- Replaced the connection-string `DATA SOURCE` parser and savepoint-name normalization with lower-overhead span-based paths.
- Replaced the brace-list JSON normalizer with a manual builder and removed LINQ from imported schema selection.
- Reworked JSON path normalization to stay on spans from input trimming through the `RETURNING` case.
- Kept JSON path parser normalization consistent by materializing the spec only after `lax/strict` trimming.
- Removed the unconditional `Trim()` from debug-trace statement contextualization when the statement is already normalized.

## Next Focus

- Benchmark the completed optimization set against the SQLite matrix and keep the wiki mirror in sync with the latest results.
- Watch for any regressions in the benchmark matrix before starting the next performance slice.

## Update Log

- 2026-04-11: Created baseline tracker and completed the first structural optimization wave.
- 2026-04-11: Continued wave 2 with direct REPLACE conflict lookup, insert literal/parameter fast paths, and cached self-referencing FK detection.
- 2026-04-11: Continued wave 2 again with resolved WHERE conditions for update/delete-from-select and loop-based identity/key fallback lookups.
- 2026-04-11: Continued wave 2 with loop-based FK scan validation to remove LINQ overhead from sequential delete/schema checks.
- 2026-04-11: Continued wave 2 with versioned FK lookup-plan caching to avoid rediscovering matching indexes during row validation.
- 2026-04-11: Continued wave 2 with manual insert column token scanning and tighter insert-select row projection loops.
- 2026-04-11: Continued wave 2 with loop-based delete parallel FK validation, parsing cleanup in `TableMock`, and span-based qualified-name parsing in insert helpers.
- 2026-04-11: Continued wave 2 with loop-based insert-replace conflict materialization and unqualified-name parsing helpers in insert expression evaluation.
- 2026-04-11: Continued wave 2 with span-based `WHERE` parsing and dictionary-backed PK shortcut matching for simple filtered lookups.
- 2026-04-11: Continued wave 2 with delete snapshot elimination in the matching path and a reusable batch conflict-key buffer in insert.
- 2026-04-11: Continued wave 2 with span-based parsing for update/delete-from-select join filters and hash-set based update index-change detection.
- 2026-04-11: Continued wave 2 with precomputed `INSERT ... SELECT` target indexes and output capacity reservation.
- 2026-04-11: Continued wave 2 with a lighter `SetColValue` fast path for literal and parameter inserts.
- 2026-04-11: Continued wave 2 with a quoted-string fast path in insert value normalization and insert conflict assignment normalization.
- 2026-04-11: Continued wave 2 with a simple raw comma splitter fast path for quote-free and parenthesis-free parser blocks.
- 2026-04-11: Continued wave 2 with a shared simple-value parser fast path for common insert and assignment literals.
- 2026-04-11: Continued wave 2 with trimmed simple-value helper paths to avoid repeated whitespace normalization in hot insert flows.
- 2026-04-11: Continued wave 2 with a single-item comma splitter fast path and trimmed normalization for ON CONFLICT DO UPDATE assignments.
- 2026-04-12: Continued wave 2 with raw SQL slice readers for hot clause extraction paths to avoid token-buffer string reconstruction.
- 2026-04-12: Completed wave 2 with slice-based comma-separated raw item parsing and moved the tracker focus to query execution fast paths.
- 2026-04-12: Started wave 3 with window-spec key memoization and a single-column partition-key fast path.
- 2026-04-12: Continued wave 3 with correlated-subquery cache-field reuse for repeated EXISTS/IN/COUNT lookups.
- 2026-04-12: Continued wave 3 with single-path cache access in subquery lookup/comparison helpers.
- 2026-04-12: Continued wave 3 with correlated canonical-SQL memoization for EXISTS/COUNT pre-aggregation paths.
- 2026-04-12: Continued wave 3 with per-subquery filtered outer-field memoization for correlated cache-key building.
- 2026-04-12: Continued wave 3 with a single-column correlated key fast path for EXISTS/COUNT lookups.
- 2026-04-12: Continued wave 3 with a lazy single-column window sort path for `ROW_NUMBER`/`NTILE`.
- 2026-04-12: Continued wave 3 with per-row correlated subquery key memoization.
- 2026-04-12: Continued wave 3 with a single-column peer-group path for ranking and percentile windows.
- 2026-04-12: Continued wave 3 with a single-order frame-range cache path for window peer-group and frame evaluation.
- 2026-04-12: Continued wave 3 with single-order window sorting that skips unused order-value array materialization.
- 2026-04-12: Continued wave 3 with a single-equality left join lookup path for common anti-join and equi-join workloads.
- 2026-04-12: Continued wave 3 with the shared lookup key formatter wired into the left-join equality path.
- 2026-04-12: Continued wave 3 with index-aware equality scans for scalar subquery counts and EXISTS checks.
- 2026-04-12: Continued wave 3 with a direct simple-column fast path for `IN` subquery value materialization.
- 2026-04-12: Continued wave 3 with a direct two-part union count fast path for `UNION ALL` and `UNION DISTINCT`.
- 2026-04-12: Continued wave 3 with a single-column fast path for `DISTINCT` projections.
- 2026-04-12: Continued wave 3 with a direct indexed count fast path for correlated scalar subquery counting.
- 2026-04-12: Continued wave 3 with `IReadOnlyList` reuse in correlated EXISTS/COUNT pre-aggregation loops.
- 2026-04-12: Continued wave 3 with `IReadOnlyList` reuse in both correlated count accumulation branches.
- 2026-04-12: Started wave 4 with cached JSON path normalization for repeated extraction calls.
- 2026-04-12: Continued wave 4 with per-execution caching for zero-argument temporal function results.
- 2026-04-12: Continued wave 4 with a lower-overhead temporal unit resolver for date-arithmetic functions.
- 2026-04-12: Continued wave 4 with a cached `StringBuilder` path for string aggregation output.
- 2026-04-12: Continued wave 4 with a direct JSON simple-path lookup fast path for property and index chains.
- 2026-04-12: Continued wave 4 with a no-clone path for scalar JSON reads.
- 2026-04-12: Continued wave 4 with redundant transaction-wrapper locking removed from commit/savepoint/rollback paths.
- 2026-04-12: Continued wave 4 with loop-based savepoint cleanup for rollback/release paths.
- 2026-04-12: Continued wave 4 with no-clone JSON element reads for JSON query/value evaluation paths.
- 2026-04-12: Continued wave 4 with direct AST extraction for constant string aggregate separators.
- 2026-04-12: Continued wave 4 with a no-op separator fast path for empty-separator string aggregates.
- 2026-04-12: Continued wave 4 with pre-sized distinct deduplication for string aggregates.
- 2026-04-12: Continued wave 4 with separator-aware string aggregate append loops and a manual JSON array-index parser.
- 2026-04-12: Continued wave 4 with direct touched-table iteration for rollback index restoration.
- 2026-04-12: Continued wave 4 with a higher-capacity estimate for large string aggregates.
- 2026-04-12: Continued wave 4 with loop-based global temporary table cloning to remove LINQ from transaction copy paths.
- 2026-04-12: Continued wave 4 with direct dictionary cloning for temporary-table rows and a no-spread include path for cloned indexes.
- 2026-04-12: Continued the batch insert slice with precomputed target columns and trim-free fast paths for already normalized simple values.
- 2026-04-12: Continued the batch insert slice again by routing `ON CONFLICT WHERE` parameter resolution through the shared execution-context resolver.
- 2026-04-12: Continued the batch insert slice once more by routing `ON DUPLICATE KEY UPDATE` parameter resolution through the shared execution-context resolver.
- 2026-04-12: Continued the shared execution path by removing the parameter-candidate array allocation from the central parameter resolver.
- 2026-04-12: Continued the shared execution path again by removing the positional-parameter list allocation from the central resolver.
- 2026-04-12: Continued the CRUD slice by reusing the same `WHERE` context across `UPDATE` and `DELETE` rows when no positional placeholders are present.
- 2026-04-12: Continued the CRUD slice again by skipping the unconditional trim in `UPDATE SET` resolution when the assignment text is already normalized.
- 2026-04-12: Continued wave 3 with loop-based query index coverage checks and manual non-`TableMock` key projection in the index lookup path.
- 2026-04-12: Continued wave 3 with a direct indexed row-count fast path that skips row materialization for `COUNT(*)`-style queries.
- 2026-04-12: Continued wave 3 with indexed row-count helpers reused by the union and correlated subquery execution paths.
- 2026-04-12: Continued wave 3 with a simple first-column subquery fast path backed by indexed source rows when possible.
- 2026-04-12: Continued wave 3 with a scalar `COUNT(*)` subquery fast path for simple equality-only cases.
- 2026-04-12: Continued wave 3 with a dedicated `UNION DISTINCT` count branch that deduplicates rows with a hash set.
- 2026-04-12: Continued wave 3 with partition-aware row-count helpers on `Source` for indexed counts.
- 2026-04-12: Continued wave 3 with lighter correlated EXISTS/COUNT cache-key synthesis around canonical SQL memoization.
- 2026-04-12: Continued wave 3 with hinted indexed counts that still honor MySQL index hints and primary-key equivalents.
- 2026-04-12: Continued wave 3 with an index-backed shared subquery source builder for correlated row materialization.
- 2026-04-12: Completed wave 3 with the remaining query execution fast paths aligned to the SQLite benchmark matrix.
- 2026-04-12: Continued wave 4 with manual brace-list JSON path normalization to remove LINQ allocations from path rewrites.
- 2026-04-12: Continued wave 4 with span-based temporal trimming so zero-arg Firebird detection and offset parsing skip extra allocations.
- 2026-04-12: Continued wave 4 with span-based connection-string and savepoint normalization to reduce repeated scans in transaction setup paths.
- 2026-04-12: Continued wave 4 with a builder-based JSON normalizer and loop-based imported schema selection to remove remaining LINQ paths.
- 2026-04-12: Continued wave 4 with span-based JSON path normalization from input trimming through the `RETURNING` case.
- 2026-04-12: Continued wave 4 with consistent JSON path spec materialization after `lax/strict` trimming.
- 2026-04-12: Continued wave 4 with trim-free debug-trace statement contextualization when the statement already arrives normalized.
- 2026-04-12: Continued wave 4 with span-based JSON path token parsing that skips materializing the whole modified path text.
- 2026-04-12: Completed wave 4 with span-based JSON path spec parsing in the common JSON helper and deferred normalization materialization to the end of the parse.
