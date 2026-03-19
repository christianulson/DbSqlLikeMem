# Performance Optimization Implementation

## Phase 0 – Row Limit Caching (COMPLETED)

- [x] Refactor Row Limit AST to use [SqlExpr](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Parser/ExprAst.cs#2-3).
- [x] Support parameters in TOP/LIMIT/FETCH for better caching.

## Phase 1 Fix – Index Allocations (Core & Batch Regression)

- [x] Create [IndexKey](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexKey.cs#10-211) struct optimized for 1-3 columns (ALLOCATION-FREE).
- [x] Refactor `TableMock.BuildPkKey` to use optimized [IndexKey](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexKey.cs#10-211) constructors.
- [x] Refactor [IndexDef.cs](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexDef.cs) to use optimized [IndexKey](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexKey.cs#10-211) building.
- [x] Refactor `TableMock.AddBatch` to reuse [IndexKey](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/IndexKey.cs#10-211) computations.

## Phase 2 – Batch & RETURNING (Completed)

- [x] Optimize [AddBatch](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs#752-850) flow in [DbInsertStrategy](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Strategies/DbInsertStrategy.cs#11-1279) / [TableMock](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs#11-1715).
- [x] Optimize [SnapshotRow](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs#259-265) (Lazy/Conditional) and unify DML strategies.
- [x] Implement `LazyRowSnapshot` wrapper to avoid dictionary allocation for triggers (or reuse direct dictionaries).

## Phase 3 – Caching (Completed)

- [x] Create `SqlAstCache` mechanism.
- [x] Create `PropertyAccessorCache`.

## Phase 4 – Domain-Specific (Temporal, JSON, Dialect) (Completed)

- [x] Optimize JSON path caching.
- [x] Optimize Temporal functions lookups.
- [x] Improve String Aggregations (StringBuilders/HashSets).
- [x] Profile window functions (LAG, ROW_NUMBER).

## Phase 5 – Transactions (Completed)

- [x] Implement `ChangeLog` for [TableMock](file:///c:/Projects/DbSqlLikeMem/src/DbSqlLikeMem/Models/TableMock.cs#11-1715) to replace full clones on savepoints.
