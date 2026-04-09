namespace DbSqlLikeMem;

internal static class SelectPlanProjectionHelper
{
    private static void EnsureListCapacity<T>(List<T> list, int desiredCapacity)
    {
        if (desiredCapacity <= list.Capacity)
            return;

        list.Capacity = desiredCapacity;
    }

    internal static bool IncludeExtraColumns(
        AstQueryExecutorBase.EvalRow? sampleRow,
        List<TableResultColMock> columns,
        HashSet<string> usedAliases,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        string rawExpression,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        // Avoid allocating intermediate strings when scanning "alias.*" projections.
        var startIndex = 0;
        var endIndex = rawExpression.Length - 1;
        while (startIndex <= endIndex && char.IsWhiteSpace(rawExpression[startIndex]))
            startIndex++;
        while (endIndex >= startIndex && char.IsWhiteSpace(rawExpression[endIndex]))
            endIndex--;

        var trimmedLength = endIndex - startIndex + 1;
        if (trimmedLength < 3 || rawExpression[endIndex] != '*')
            return true;

        var dotIndex = rawExpression.LastIndexOf('.', endIndex, trimmedLength);
        if (dotIndex <= startIndex)
            return true;

        var prefixStart = startIndex;
        var prefixEnd = dotIndex - 1;
        while (prefixStart <= prefixEnd && char.IsWhiteSpace(rawExpression[prefixStart]))
            prefixStart++;
        while (prefixEnd >= prefixStart && char.IsWhiteSpace(rawExpression[prefixEnd]))
            prefixEnd--;
        while (prefixStart <= prefixEnd && rawExpression[prefixStart] == '`')
            prefixStart++;
        while (prefixEnd >= prefixStart && rawExpression[prefixEnd] == '`')
            prefixEnd--;

        if (prefixStart > prefixEnd)
            return true;

        var prefix = rawExpression.Substring(prefixStart, prefixEnd - prefixStart + 1);
        if (prefix.Length == 0)
            return true;

        if (sampleRow is null)
            return true;

        if (!TryGetSourceByAlias(sampleRow, prefix, out var source))
            return true;

        if (source is null)
            return true;

        AppendSourceColumns(columns, usedAliases, evaluators, source, resolveColumn);
        return false;
    }

    internal static bool TryGetSourceByAlias(
        AstQueryExecutorBase.EvalRow sampleRow,
        string alias,
        out AstQueryExecutorBase.Source? source)
        => sampleRow.Sources.TryGetValue(alias, out source);

    internal static bool ExpandSelectAsterisk(
        List<TableResultColMock> columns,
        HashSet<string> usedAliases,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        AstQueryExecutorBase.EvalRow? sampleRow,
        string rawExpression,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        if (rawExpression != "*")
            return true;

        if (sampleRow is null)
            return false;

        // Pre-allocate to the final size once, avoiding repeated Capacity growth by source.
        // This is a hot path in projection expansion for multi-source queries.
        var extraColumnCount = 0;
        foreach (var s in sampleRow.Sources.Values)
            extraColumnCount += s.ColumnNames.Count;

        if (extraColumnCount > 0)
        {
            EnsureListCapacity(columns, columns.Count + extraColumnCount);
            EnsureListCapacity(evaluators, evaluators.Count + extraColumnCount);
        }

        foreach (var source in sampleRow.Sources.Values)
            AppendSourceColumns(columns, usedAliases, evaluators, source, resolveColumn);

        return false;
    }

    internal static string GetSelectProjectionTableAlias(SqlSelectQuery query)
    {
        var table = query.Table;
        return table?.Alias ?? table?.TableFunction?.Name ?? table?.Name ?? string.Empty;
    }

    internal static string InferColumnAlias(string rawExpression)
    {
        var normalized = rawExpression.Trim();
        var dotIndex = normalized.LastIndexOf('.');
        if (dotIndex >= 0 && dotIndex + 1 < normalized.Length)
            return normalized[(dotIndex + 1)..].Trim().Trim('`');

        return normalized.Trim('`');
    }

    internal static string MakeUniqueAlias(
        HashSet<string> usedAliases,
        string preferredAlias,
        string tableAlias)
    {
        if (usedAliases.Add(preferredAlias))
            return preferredAlias;

        var alternativeAlias = $"{tableAlias}_{preferredAlias}";
        if (usedAliases.Add(alternativeAlias))
            return alternativeAlias;

        var suffix = 2;
        while (true)
        {
            var candidateAlias = $"{alternativeAlias}_{suffix}";
            if (usedAliases.Add(candidateAlias))
                return candidateAlias;

            suffix++;
        }
    }

    internal static TableResultColMock CreateSelectPlanColumn(
        string tableAlias,
        string columnAlias,
        int columnIndex,
        DbType dbType,
        bool isNullable = true,
        bool isJsonFragment = false)
        => new(
            tableAlias: tableAlias,
            columnAlias: columnAlias,
            columnName: columnAlias,
            columIndex: columnIndex,
            dbType: dbType,
            isNullable: isNullable,
            isJsonFragment: isJsonFragment);

    private static void AppendSourceColumns(
        List<TableResultColMock> columns,
        HashSet<string> usedAliases,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        AstQueryExecutorBase.Source source,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        var columnNames = source.ColumnNames;
        var columnCount = columnNames.Count;
        if (columnCount == 0)
            return;

        EnsureListCapacity(columns, columns.Count + columnCount);
        EnsureListCapacity(evaluators, evaluators.Count + columnCount);

        var sourceAlias = source.Alias;
        var columnIndex = columns.Count;
        for (var i = 0; i < columnCount; i++)
        {
            var columnName = columnNames[i];
            var capturedColumnName = columnName;

            var alias = MakeUniqueAlias(usedAliases, columnName, sourceAlias);

            var dbType = DbType.Object;
            var isNullable = true;
            var isJsonFragment = false;
            if (source.TryGetColumnMetadata(columnName, out var metadata) && metadata is not null)
            {
                dbType = metadata.DbType;
                isNullable = metadata.IsNullable;
                isJsonFragment = metadata.IsJsonFragment;
            }

            columns.Add(new TableResultColMock(sourceAlias, alias, columnName, columnIndex, dbType, isNullable, isJsonFragment));
            evaluators.Add((row, group) => resolveColumn(sourceAlias, capturedColumnName, row));
            columnIndex++;
        }
    }
}
