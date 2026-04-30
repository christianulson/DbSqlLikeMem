namespace DbSqlLikeMem;

internal static class SelectPlanIndexRecommendationHelper
{
    internal static IReadOnlyList<SqlIndexRecommendation> Build(
        QueryExecutionContext context,
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
    {
        if (metrics.EstimatedRowsRead <= 0)
            return [];

        if (metrics.EstimatedRowsRead < 3)
            return [];

        var sourceMap = BuildSourceMap(query);
        if (sourceMap.Count == 0)
            return [];

        var filterColumnsByTable = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        CollectColumns(query.Where, sourceMap, filterColumnsByTable);
        foreach (var join in query.Joins)
            CollectColumns(join.On, sourceMap, filterColumnsByTable);

        var orderColumnsByTable = BuildOrderByColumns(query, sourceMap);
        var recommendations = new List<SqlIndexRecommendation>();
        var tableNames = GetDistinctPhysicalTableNames(sourceMap);

        foreach (var tableName in tableNames)
        {
            var table = FindTable(context.Connection, tableName);
            if (table is null)
                continue;

            filterColumnsByTable.TryGetValue(tableName, out var filterCols);
            orderColumnsByTable.TryGetValue(tableName, out var orderCols);

            var keyCols = BuildSuggestedKeyColumns(filterCols, orderCols);
            if (keyCols.Count == 0)
                continue;

            if (HasMatchingIndex(table, keyCols) || HasPrimaryKeyPrefix(table, keyCols))
                continue;

            var ddl = $"CREATE INDEX IX_{table.TableName}_{string.Join("_", keyCols)} ON {table.TableName} ({string.Join(", ", keyCols)});";
            var reason = BuildRecommendationReason(filterCols, orderCols, keyCols);

            var estimatedAfter = EstimateRowsReadAfterIndex(metrics.EstimatedRowsRead, filterCols?.Count ?? 0, orderCols?.Count ?? 0);
            var confidence = CalculateRecommendationConfidence(metrics, estimatedAfter, filterCols?.Count ?? 0, orderCols?.Count ?? 0);
            recommendations.Add(new SqlIndexRecommendation(
                table.TableName,
                ddl,
                reason,
                confidence,
                metrics.EstimatedRowsRead,
                estimatedAfter));
        }

        return recommendations;
    }

    private static List<string> GetDistinctPhysicalTableNames(
        IReadOnlyDictionary<string, SqlTableSource> sourceMap)
    {
        var names = new List<string>(sourceMap.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var source in sourceMap.Values)
        {
            if (string.IsNullOrWhiteSpace(source.Name))
                continue;

            if (seen.Add(source!.Name!))
                names.Add(source.Name!);
        }

        names.Sort(StringComparer.OrdinalIgnoreCase);
        return names;
    }

    private static List<string> BuildSuggestedKeyColumns(
        List<string>? filterCols,
        List<string>? orderCols)
    {
        var keyCols = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void AddRange(List<string>? columns)
        {
            if (columns is null)
                return;

            foreach (var col in columns)
            {
                if (seen.Add(col))
                    keyCols.Add(col);
            }
        }

        AddRange(filterCols);
        AddRange(orderCols);
        return keyCols;
    }

    private static bool HasMatchingIndex(ITableMock table, IReadOnlyList<string> keyCols)
    {
        foreach (var idx in table is TableMock tableMock ? tableMock.IndexesRaw.Values : table.Indexes.Values)
        {
            if (idx.KeyCols.Count < keyCols.Count)
                continue;

            var matches = true;
            for (var i = 0; i < keyCols.Count; i++)
            {
                if (!string.Equals(idx.KeyCols[i], keyCols[i], StringComparison.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                return true;
        }

        return false;
    }

    private static bool HasPrimaryKeyPrefix(ITableMock table, IReadOnlyList<string> keyCols)
    {
        if (table is TableMock tableMock)
        {
            var pkIndexArray = tableMock.PkIndexArray;
            if (pkIndexArray.Length < keyCols.Count)
                return false;

            for (var i = 0; i < keyCols.Count; i++)
            {
                if (!string.Equals(tableMock.GetColumnByIndex(pkIndexArray[i]).Name, keyCols[i], StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
        }

        var primaryKeyIndexes = table.PrimaryKeyIndexes;
        if (primaryKeyIndexes.Count == 0)
            return false;

        var pkByOrdinal = new List<string>(primaryKeyIndexes.Count);
        foreach (var column in table.Columns.Values)
        {
            if (!primaryKeyIndexes.Contains(column.Index))
                continue;

            pkByOrdinal.Add(column.Name);
        }

        if (pkByOrdinal.Count < keyCols.Count)
            return false;

        for (var i = 0; i < keyCols.Count; i++)
        {
            if (!string.Equals(pkByOrdinal[i], keyCols[i], StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private static string BuildRecommendationReason(
        List<string>? filterCols,
        List<string>? orderCols,
        IReadOnlyList<string> keyCols)
    {
        var hasFilter = filterCols is { Count: > 0 };
        var hasOrder = orderCols is { Count: > 0 };

        if (hasFilter && hasOrder)
            return SqlExecutionPlanMessages.ReasonFilterAndOrder(
                string.Join(", ", filterCols!),
                string.Join(", ", orderCols!),
                string.Join(", ", keyCols));

        if (hasFilter)
            return SqlExecutionPlanMessages.ReasonFilterOnly(string.Join(", ", filterCols!));

        return SqlExecutionPlanMessages.ReasonOrderOnly(string.Join(", ", orderCols ?? []));
    }

    private static long EstimateRowsReadAfterIndex(long estimatedRowsRead, int filterColumnCount, int orderColumnCount)
    {
        if (estimatedRowsRead <= 0)
            return 0;

        var reductionFactor = 1d;
        if (filterColumnCount > 0)
            reductionFactor *= Math.Pow(0.35d, Math.Min(3, filterColumnCount));

        if (orderColumnCount > 0)
            reductionFactor *= 0.80d;

        var estimated = (long)Math.Ceiling(estimatedRowsRead * reductionFactor);
        return Math.Max(1, Math.Min(estimatedRowsRead, estimated));
    }

    private static int CalculateRecommendationConfidence(
        SqlPlanRuntimeMetrics metrics,
        long estimatedRowsReadAfter,
        int filterColumnCount,
        int orderColumnCount)
    {
        if (metrics.EstimatedRowsRead <= 0)
            return 0;

        var gainPct = (double)(metrics.EstimatedRowsRead - estimatedRowsReadAfter) / metrics.EstimatedRowsRead * 100d;
        var score = 50d;
        score += Math.Min(30d, gainPct * 0.4d);
        if (filterColumnCount > 0)
            score += 10d;
        if (orderColumnCount > 0)
            score += 5d;
        if (metrics.EstimatedRowsRead >= 100)
            score += 5d;

        var rounded = (int)Math.Round(score);
        if (rounded < 1) return 1;
        if (rounded > 99) return 99;
        return rounded;
    }

    private static Dictionary<string, SqlTableSource> BuildSourceMap(SqlSelectQuery query)
    {
        var map = new Dictionary<string, SqlTableSource>(StringComparer.OrdinalIgnoreCase);

        AddPhysicalSource(query.Table, map);
        foreach (var join in query.Joins)
            AddPhysicalSource(join.Table, map);

        return map;
    }

    private static void AddPhysicalSource(
        SqlTableSource? source,
        Dictionary<string, SqlTableSource> map)
    {
        if (source is null
            || source.Name is null
            || source.Derived is not null
            || source.DerivedUnion is not null)
            return;

        map[source.Name] = source;
        if (!string.IsNullOrWhiteSpace(source.Alias))
            map[source.Alias!] = source;
    }

    private static Dictionary<string, List<string>> BuildOrderByColumns(
        SqlSelectQuery query,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap)
    {
        var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var singlePhysicalTableName = TryGetSinglePhysicalTableName(sourceMap);

        foreach (var order in query.OrderBy)
        {
            var token = order.Raw?.Trim();
            if (string.IsNullOrWhiteSpace(token))
                continue;

            if (!TryResolveColumn(token!, sourceMap, out var tableName, out var columnName))
            {
                if (singlePhysicalTableName is null)
                    continue;

                tableName = singlePhysicalTableName;
                columnName = token!;
            }

            if (!result.TryGetValue(tableName, out var list))
            {
                list = [];
                result[tableName] = list;
            }

            if (!list.Contains(columnName, StringComparer.OrdinalIgnoreCase))
                list.Add(columnName);
        }

        return result;
    }

    private static void CollectColumns(
        SqlExpr? expr,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap,
        Dictionary<string, List<string>> columnsByTable)
    {
        if (expr is null)
            return;

        switch (expr)
        {
            case BinaryExpr b:
                CollectColumns(b.Left, sourceMap, columnsByTable);
                CollectColumns(b.Right, sourceMap, columnsByTable);
                break;
            case UnaryExpr u:
                CollectColumns(u.Expr, sourceMap, columnsByTable);
                break;
            case InExpr i:
                CollectColumns(i.Left, sourceMap, columnsByTable);
                foreach (var item in i.Items)
                    CollectColumns(item, sourceMap, columnsByTable);
                break;
            case LikeExpr l:
                CollectColumns(l.Left, sourceMap, columnsByTable);
                CollectColumns(l.Pattern, sourceMap, columnsByTable);
                CollectColumns(l.Escape, sourceMap, columnsByTable);
                break;
            case IsNullExpr n:
                CollectColumns(n.Expr, sourceMap, columnsByTable);
                break;
            case BetweenExpr between:
                CollectColumns(between.Expr, sourceMap, columnsByTable);
                CollectColumns(between.Low, sourceMap, columnsByTable);
                CollectColumns(between.High, sourceMap, columnsByTable);
                break;
            case FunctionCallExpr f:
                foreach (var arg in f.Args)
                    CollectColumns(arg, sourceMap, columnsByTable);
                break;
            case CallExpr c:
                foreach (var arg in c.Args)
                    CollectColumns(arg, sourceMap, columnsByTable);
                break;
            case JsonAccessExpr j:
                CollectColumns(j.Target, sourceMap, columnsByTable);
                break;
            case RowExpr r:
                foreach (var item in r.Items)
                    CollectColumns(item, sourceMap, columnsByTable);
                break;
            case CaseExpr c:
                if (c.BaseExpr is not null)
                    CollectColumns(c.BaseExpr, sourceMap, columnsByTable);
                foreach (var wt in c.Whens)
                {
                    CollectColumns(wt.When, sourceMap, columnsByTable);
                    CollectColumns(wt.Then, sourceMap, columnsByTable);
                }
                if (c.ElseExpr is not null)
                    CollectColumns(c.ElseExpr, sourceMap, columnsByTable);
                break;
            case ColumnExpr col:
                TryAddColumn(col.Qualifier + "." + col.Name, sourceMap, columnsByTable);
                break;
            case IdentifierExpr id:
                TryAddColumn(id.Name, sourceMap, columnsByTable);
                break;
        }
    }

    private static void TryAddColumn(
        string token,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap,
        Dictionary<string, List<string>> columnsByTable)
    {
        if (!TryResolveColumn(token, sourceMap, out var tableName, out var columnName))
        {
            var singlePhysicalTableName = TryGetSinglePhysicalTableName(sourceMap);
            if (singlePhysicalTableName is null)
                return;

            tableName = singlePhysicalTableName;
            columnName = token;
        }

        if (!columnsByTable.TryGetValue(tableName, out var cols))
        {
            cols = [];
            columnsByTable[tableName] = cols;
        }

        if (!cols.Contains(columnName, StringComparer.OrdinalIgnoreCase))
            cols.Add(columnName);
    }

    private static string? TryGetSinglePhysicalTableName(
        IReadOnlyDictionary<string, SqlTableSource> sourceMap)
    {
        string? uniqueTableName = null;
        foreach (var currentSource in sourceMap.Values)
        {
            if (string.IsNullOrWhiteSpace(currentSource.Name))
                continue;

            if (uniqueTableName is null)
            {
                uniqueTableName = currentSource.Name;
                continue;
            }

            if (!string.Equals(uniqueTableName, currentSource.Name, StringComparison.OrdinalIgnoreCase))
                return null;
        }

        return uniqueTableName;
    }

    private static bool TryResolveColumn(
        string token,
        IReadOnlyDictionary<string, SqlTableSource> sourceMap,
        out string tableName,
        out string columnName)
    {
        tableName = string.Empty;
        columnName = string.Empty;

        var parts = token.Split('.').Select(_ => _.Trim()).Where(_ => !string.IsNullOrWhiteSpace(_)).ToArray();
        if (parts.Length == 0)
            return false;

        if (parts.Length == 1)
        {
            string? uniqueTableName = null;
            foreach (var currentSource in sourceMap.Values)
            {
                if (string.IsNullOrWhiteSpace(currentSource.Name))
                    continue;

                if (uniqueTableName is null)
                {
                    uniqueTableName = currentSource.Name;
                    continue;
                }

                if (!string.Equals(uniqueTableName, currentSource.Name, StringComparison.OrdinalIgnoreCase))
                    return false;
            }

            if (string.IsNullOrWhiteSpace(uniqueTableName))
                return false;

            tableName = uniqueTableName!;
            columnName = parts[0];
            return true;
        }

        var sourceKey = parts[^2];
        if (!sourceMap.TryGetValue(sourceKey, out var source) || string.IsNullOrWhiteSpace(source.Name))
            return false;

        tableName = source.Name!;
        columnName = parts[^1];
        return true;
    }

    private static ITableMock? FindTable(
        DbConnectionMockBase connection,
        string tableName)
    {
        if (connection.TryGetTable(tableName, out var directTable)
            && directTable is not null)
        {
            return directTable;
        }

        foreach (var schema in connection.Db.Values)
        {
            if (schema.TryGetTable(tableName, out var schemaTable)
                && schemaTable is not null)
            {
                return schemaTable;
            }
        }

        return null;
    }
}
