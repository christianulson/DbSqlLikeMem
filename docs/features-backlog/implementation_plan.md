# Performance Optimization Plan

The previous optimizations made in Phase 1 (Tasks 2, 3, 4) severely deteriorated performance because of how the new `_pkIndex` and [IndexDef](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs#16-42) structures were coded. They rely on `string.Concat` to build composite keys for every row insertion, deletion, and lookup (`TableMock.BuildPkKey`, `IndexDef.BuildIndexKey`). This creates intense garbage collection pressure and CPU overhead, making operations like Batch Inserts, Updates, and Deletes significantly slower than SQLite.

This new plan focuses on reverting the negative impact of Phase 1 and sequentially optimizing the remaining components tracked in [docs\Wiki\performance-matrix.md](file:///c:/Projects/DbSqlLikeMem/docs/Wiki/performance-matrix.md) outperforming SQLite Native.

## User Review Required

> [!WARNING]  
> The [IndexDef](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs#16-42) public interface `IReadOnlyDictionary<string, ...>` must be modified. `string` keys will be replaced by a structured [IndexKey](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs#335-349) type. This is an internal-level breaking change but essential to stop string allocation on every index jump.

> [!IMPORTANT]
> The fixes will be made iteratively, starting with the Core/Batch regression fix (Phase 1 Fix), then Batch optimizations, Advanced Queries, and so on.

---

## Proposed Changes

### 1. Fix Phase 1 Regression (Core & Batch Indexes Allocations)

**Goal:** Eliminate `string` allocations in [TableMock.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs) and [IndexDef.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs) on every DML operation and lookup.

#### [NEW] `src/DbSqlLikeMem/Models/IndexKey.cs`

- Create `readonly record struct IndexKey(object?[] Values)` implementing `IEquatable<IndexKey>`.
- Add an `IndexKeyComparer : IEqualityComparer<IndexKey>` that correctly calculates hash codes and equates arrays (considering `byte[]`, strings, numbers).

#### [MODIFY] [src/DbSqlLikeMem/Models/TableMock.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs)

- Change `_pkIndex` from `Dictionary<string, int>` to `Dictionary<IndexKey, int>`.
- Update [BuildPkKey(row)](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs#118-136) to return an [IndexKey](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs#335-349) mapped directly from the row values instead of concatenating strings.

#### [MODIFY] [src/DbSqlLikeMem/Models/IndexDef.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs)

- Change class definition to implement `IReadOnlyDictionary<IndexKey, IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>>`.
- Change `_items` dictionary to `Dictionary<IndexKey, ...>`.
- Update [BuildIndexKey(row)](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs#335-349) to return an [IndexKey](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs#335-349).
- Create a [Lookup(IndexKey)](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs#409-426) to retrieve records without string conversion.

### 2. Optimize Batch Insert / Upsert (Batch)

**Goal:** Further optimize batch processing to match or beat `SQLite Native`'s 421μs for Insert Batch 100.

#### [MODIFY] [src/DbSqlLikeMem/Strategies/DbInsertStrategy.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Strategies/DbInsertStrategy.cs)

- Detect contiguous inserts for the same table. Collect them into a `List<Dictionary<int, object?>>`.
- Route multiple inserts to `TableMock.AddBatch()` efficiently without redundant table lookups.

### 3. Caching (AST / Compilation)

**Goal:** Accelerate core queries (CTE, Select) and overall execution.

#### [NEW] `src/DbSqlLikeMem/Parser/SqlAstCache.cs`

- Implement a `ConcurrentDictionary<string, SqlQuery>` cache to prevent full script parsing when the query string is identical.

#### [NEW] `src/DbSqlLikeMem/Extensions/PropertyAccessorCache.cs`

- Compile and cache `Expression.Lambda` property getters to drastically speed up `AddItem<T>` materialization.

### 4. Optimize Temporal & JSON Functions

**Goal:** Match SQLite Native in JSON and Date operations.

#### [MODIFY] [src/DbSqlLikeMem/Query/QueryJsonFunctionHelper.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Query/QueryJsonFunctionHelper.cs)

- Implement a `ConcurrentDictionary<string, JsonPathSpec>` to reuse paths.

#### [MODIFY] [src/DbSqlLikeMem/Compatibility/SqlTemporalFunctionEvaluator.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Compatibility/SqlTemporalFunctionEvaluator.cs)

- Replace LINQ `.Any()` searches on known identifier arrays with `HashSet<string>.Contains()`.

### 5. String Aggregation (Dialect)

**Goal:** Beat SQLite's fast aggregations.

#### [MODIFY] `src/DbSqlLikeMem/Execution/...` (String Aggregation evaluation paths)

- Pre-allocate `StringBuilder` sizes for aggregations.
- Replace `Distinct()` LINQ filter with `HashSet<string>` usage during sequence iteration.

### 6. Transactions Overhead

**Goal:** Minimize latency on Savepoints / Rollbacks.

#### [MODIFY] `src/DbSqlLikeMem/Models/TableMock.cs`

- Introduce a transaction log `List<ChangeLogEntry> _changeLog` to record precise row modifications (`ChangeKind`, `rowIndex`) during a savepoint.
- During rollback, invert the operations from `_changeLog` instead of cloning the entire row dataset via `.Backup()`.

---

## Verification Plan

### Automated Tests

- The user will execute the internal test suite manually via `dotnet test`.
- I will structure changes specifically mapped by tests to maintain the `EN` / `PT` XML doc rules from `AGENTS.md`.

### Manual Verification

1. User validates no compile errors with `dotnet build`.
2. User runs the benchmark suite (`benchmark\DbSqlLikeMem.Benchmarks\Benchmarks\Suites`) locally to observe `DbSqlLikeMem` performing equal to or faster than `SQLite Native`.
