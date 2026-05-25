using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class UnionExecutionHelper
{
    internal static TableResultMock ExecuteUnion(
        this QueryExecutionContext context,
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit,
        string? sqlContextForErrors,
        Func<SqlSelectQuery, TableResultMock> executeSelect,
        Func<TableResultMock, SqlSelectQuery, IDictionary<string, Source>, QueryDebugTraceBuilder?, TableResultMock> applyOrderAndLimit,
        Func<SqlSelectQuery, int> countKnownInputTables)
    {
        var sw = Stopwatch.StartNew();
        QueryDebugTraceBuilder? debugTrace = context.Connection.IsDebugTraceCaptureEnabled
            ? new QueryDebugTraceBuilder(SqlConst.UNION)
            : null;

        if (parts is null || parts.Count == 0)
            throw new InvalidOperationException("UNION: nenhuma query.");

        if (allFlags is null)
            throw new InvalidOperationException("UNION: allFlags null.");

        if (allFlags.Count != Math.Max(0, parts.Count - 1))
            throw new InvalidOperationException($"UNION: allFlags.Count inválido. parts={parts.Count}, allFlags={allFlags.Count}");

        var tables = new TableResultMock[parts.Count];
        if (parts.Count <= 2)
        {
            for (var i = 0; i < parts.Count; i++)
                tables[i] = executeSelect(parts[i]);
        }
        else
        {
            Parallel.For(0, parts.Count, i => tables[i] = executeSelect(parts[i]));
        }

        var totalRows = tables.Sum(static table => table.Count);
        debugTrace?.AddStep(
            "UnionInputs",
            totalRows,
            tables.Length,
            TimeSpan.Zero,
            QueryDebugTraceFormattingHelper.FormatUnionInputsDebugDetails(parts, allFlags));

        var result = new TableResultMock
        {
            Columns = tables[0].Columns,
            JoinFields = new List<Dictionary<string, object?>>(totalRows)
        };

        if (context.TryUseSimpleUnionAllProjectionPath(tables, allFlags, out var fastUnionResult))
        {
            result = fastUnionResult;
            return context.FinalizeUnionResult(
                parts,
                allFlags,
                orderBy,
                rowLimit,
                countKnownInputTables,
                result,
                sw,
                debugTrace,
                applyOrderAndLimit);
        }

        context.ValidateUnionTables(tables, sqlContextForErrors);

        result.Columns = AreUnionColumnMetadataIdentical(tables)
            ? tables[0].Columns
            : MergeUnionColumnMetadata(tables);
        result.Capacity = totalRows;

        var needsDistinct = allFlags.Any(flag => !flag);
        var seenRows = needsDistinct ? new HashSet<Dictionary<int, object?>>(new SqlRowDictionaryComparer(context)) : null;

        AppendUnionRows(tables[0], result, seenRows);
        for (var i = 1; i < tables.Length; i++)
        {
            if (allFlags[i - 1])
            {
                AppendUnionRows(tables[i], result, null);
                continue;
            }

            AppendDistinctUnionRows(tables[i], result, seenRows!);
        }

        debugTrace?.AddStep(
            "UnionCombine",
            totalRows,
            result.Count,
            TimeSpan.Zero,
            QueryDebugTraceFormattingHelper.FormatUnionCombineDebugDetails(parts, allFlags));

        return context.FinalizeUnionResult(
            parts,
            allFlags,
            orderBy,
            rowLimit,
            countKnownInputTables,
            result,
            sw,
            debugTrace,
            applyOrderAndLimit);
    }

    private static void ValidateUnionTables(
        this QueryExecutionContext context,
        IReadOnlyList<TableResultMock> tables,
        string? sqlContextForErrors)
    {
        var resultColumns = tables[0].Columns;
        for (var i = 0; i < tables.Count; i++)
        {
            if (tables[i].Columns.Count != resultColumns.Count)
            {
                var msg =
                    $"UNION: número de colunas incompatível. " +
                    $"Primeiro={resultColumns.Count}, SELECT[{i}]={tables[i].Columns.Count}.";
                if (!string.IsNullOrWhiteSpace(sqlContextForErrors))
                    msg += "\nSQL: " + sqlContextForErrors;

                throw new InvalidOperationException(msg);
            }

            context.ValidateUnionColumnTypes(resultColumns, tables[i].Columns, i, sqlContextForErrors);
        }
    }

    private static void AppendUnionRows(
        TableResultMock sourceTable,
        TableResultMock targetTable,
        HashSet<Dictionary<int, object?>>? seenRows)
    {
        for (var rowIndex = 0; rowIndex < sourceTable.Count; rowIndex++)
        {
            var row = sourceTable[rowIndex];
            if (seenRows is not null && !seenRows.Add(row))
                continue;

            targetTable.Add(row);
            targetTable.JoinFields.Add(GetJoinFieldsForUnionRow(sourceTable, rowIndex));
        }
    }

    private static void AppendDistinctUnionRows(
        TableResultMock sourceTable,
        TableResultMock targetTable,
        HashSet<Dictionary<int, object?>> seenRows)
        => AppendUnionRows(sourceTable, targetTable, seenRows);

    private static TableResultMock FinalizeUnionResult(
        this QueryExecutionContext context,
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit,
        Func<SqlSelectQuery, int> countKnownInputTables,
        TableResultMock result,
        Stopwatch sw,
        QueryDebugTraceBuilder? debugTrace,
        Func<TableResultMock, SqlSelectQuery, IDictionary<string, Source>, QueryDebugTraceBuilder?, TableResultMock> applyOrderAndLimit)
    {
        if ((orderBy?.Count ?? 0) > 0 || rowLimit is not null)
        {
            var finalQ = new SqlSelectQuery(
                Ctes: [],
                Distinct: false,
                DistinctOn: [],
                SelectItems: [],
                Joins: [],
                Where: null,
                OrderBy: orderBy ?? [],
                RowLimit: rowLimit,
                GroupBy: [],
                Having: null);

            var ctes = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase);
            result = applyOrderAndLimit(result, finalQ, ctes, debugTrace);
        }

        sw.Stop();

        var unionMetrics = new SqlPlanRuntimeMetrics(
            InputTables: parts.Sum(countKnownInputTables),
            EstimatedRowsRead: parts.Sum(context.EstimateRowsRead),
            ActualRows: result.Count,
            ElapsedMs: sw.ElapsedMilliseconds);
        var runtimeContext = context.BuildPlanRuntimeContext();

        if (context.CaptureExecutionPlans)
        {
            var plan = SqlExecutionPlanFormatter.FormatUnion(
                parts,
                allFlags,
                orderBy,
                rowLimit,
                unionMetrics,
                runtimeContext);
            result.ExecutionPlan = plan;
            context.Connection.RegisterExecutionPlan(plan);
        }

        context.Connection.SetLastSelectRows(result.Count);
        if (debugTrace is not null)
            context.Connection.RegisterDebugTrace(debugTrace.Build());

        return result;
    }

    private static bool TryUseSimpleUnionAllProjectionPath(
        this QueryExecutionContext context,
        IReadOnlyList<TableResultMock> tables,
        IReadOnlyList<bool> allFlags,
        out TableResultMock result)
    {
        result = null!;

        if (tables.Count == 0
            || allFlags.Any(static flag => !flag))
        {
            return false;
        }

        if (tables[0].Columns.Count == 0)
            return false;

        if (!context.TryValidateUnionAllProjectionTables(tables, out var useFirstColumns))
            return false;

        var totalRows = tables.Sum(static table => table.Count);
        var fastResult = new TableResultMock
        {
            Columns = useFirstColumns
                ? tables[0].Columns
                : MergeUnionColumnMetadata(tables),
            JoinFields = new List<Dictionary<string, object?>>(totalRows),
            Capacity = totalRows
        };

        foreach (var table in tables)
        {
            for (var rowIndex = 0; rowIndex < table.Count; rowIndex++)
            {
                fastResult.Add(table[rowIndex]);
                fastResult.JoinFields.Add(GetJoinFieldsForUnionRow(table, rowIndex));
            }
        }

        result = fastResult;
        return true;
    }

    private static List<TableResultColMock> MergeUnionColumnMetadata(IReadOnlyList<TableResultMock> tables)
    {
        if (tables.Count == 0)
            return [];

        var merged = new List<TableResultColMock>(tables[0].Columns.Count);
        for (var index = 0; index < tables[0].Columns.Count; index++)
        {
            var first = tables[0].Columns[index];
            var dbType = first.DbType;
            var isNullable = first.IsNullable;

            for (var setIndex = 1; setIndex < tables.Count; setIndex++)
            {
                var current = tables[setIndex].Columns[index];
                dbType = MergeUnionDbType(dbType, current.DbType);
                isNullable |= current.IsNullable;
            }

            merged.Add(new TableResultColMock(
                first.TableAlias,
                first.ColumnAlias,
                first.ColumnName,
                first.ColumIndex,
                dbType,
                isNullable,
                first.IsJsonFragment));
        }

        return merged;
    }

    private static bool AreUnionColumnMetadataIdentical(IReadOnlyList<TableResultMock> tables)
    {
        if (tables.Count <= 1)
            return true;

        var firstColumns = tables[0].Columns;
        for (var tableIndex = 1; tableIndex < tables.Count; tableIndex++)
        {
            var currentColumns = tables[tableIndex].Columns;
            if (currentColumns.Count != firstColumns.Count)
                return false;

            for (var columnIndex = 0; columnIndex < firstColumns.Count; columnIndex++)
            {
                var left = firstColumns[columnIndex];
                var right = currentColumns[columnIndex];
                if (!string.Equals(left.TableAlias, right.TableAlias, StringComparison.OrdinalIgnoreCase)
                    || !string.Equals(left.ColumnAlias, right.ColumnAlias, StringComparison.OrdinalIgnoreCase)
                    || !string.Equals(left.ColumnName, right.ColumnName, StringComparison.OrdinalIgnoreCase)
                    || left.ColumIndex != right.ColumIndex
                    || left.DbType != right.DbType
                    || left.IsNullable != right.IsNullable
                    || left.IsJsonFragment != right.IsJsonFragment)
                {
                    return false;
                }
            }
        }

        return true;
    }

    private static bool TryValidateUnionAllProjectionTables(
        this QueryExecutionContext context,
        IReadOnlyList<TableResultMock> tables,
        out bool useFirstColumns)
    {
        useFirstColumns = true;
        if (tables.Count <= 1)
            return true;

        var first = tables[0].Columns;
        if (first.Count == 0)
            return false;

        for (var setIndex = 1; setIndex < tables.Count; setIndex++)
        {
            var current = tables[setIndex].Columns;
            if (current.Count != first.Count)
                return false;

            for (var columnIndex = 0; columnIndex < first.Count; columnIndex++)
            {
                var left = first[columnIndex];
                var right = current[columnIndex];
                if (!context.Dialect.AreUnionColumnTypesCompatible(left.DbType, right.DbType))
                    return false;

                if (useFirstColumns
                    && (!string.Equals(left.TableAlias, right.TableAlias, StringComparison.OrdinalIgnoreCase)
                    || !string.Equals(left.ColumnAlias, right.ColumnAlias, StringComparison.OrdinalIgnoreCase)
                    || !string.Equals(left.ColumnName, right.ColumnName, StringComparison.OrdinalIgnoreCase)
                    || left.ColumIndex != right.ColumIndex
                    || left.IsNullable != right.IsNullable
                    || left.IsJsonFragment != right.IsJsonFragment))
                {
                    useFirstColumns = false;
                }
            }
        }

        return true;
    }

    private static Dictionary<string, object?> GetJoinFieldsForUnionRow(
        TableResultMock table,
        int rowIndex)
    {
        if (rowIndex >= 0 && rowIndex < table.JoinFields.Count)
            return table.JoinFields[rowIndex];

        return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    }

    private static DbType MergeUnionDbType(DbType left, DbType right)
    {
        if (left == right)
            return left;

        if (left == DbType.Object)
            return right;

        if (right == DbType.Object)
            return left;

        static bool IsFloating(DbType type)
            => type is DbType.Single or DbType.Double;

        static bool IsDecimalLike(DbType type)
            => type is DbType.Decimal or DbType.VarNumeric or DbType.Currency;

        static bool IsIntegerLike(DbType type)
            => type is DbType.Byte or DbType.SByte
                or DbType.Int16 or DbType.UInt16
                or DbType.Int32 or DbType.UInt32
                or DbType.Int64 or DbType.UInt64;

        if ((IsFloating(left) && (IsFloating(right) || IsDecimalLike(right) || IsIntegerLike(right)))
            || (IsFloating(right) && (IsDecimalLike(left) || IsIntegerLike(left))))
        {
            return DbType.Double;
        }

        if (IsDecimalLike(left) && (IsDecimalLike(right) || IsIntegerLike(right)))
            return DbType.Decimal;

        if (IsDecimalLike(right) && IsIntegerLike(left))
            return DbType.Decimal;

        if (IsIntegerLike(left) && IsIntegerLike(right))
            return left is DbType.Int64 or DbType.UInt64 || right is DbType.Int64 or DbType.UInt64
                ? DbType.Int64
                : DbType.Int32;

        return left;
    }
}
