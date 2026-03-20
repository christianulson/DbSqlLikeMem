namespace DbSqlLikeMem;

internal static class SelectPlanProjectionHelper
{
    internal static bool IncludeExtraColumns(
        AstQueryExecutorBase.EvalRow? sampleRow,
        List<TableResultColMock> columns,
        HashSet<string> usedAliases,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        string rawExpression,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        var trimmedExpression = rawExpression.Trim();
        if (trimmedExpression.Length < 3 || trimmedExpression[^1] != '*')
            return true;

        var dotIndex = trimmedExpression.LastIndexOf('.');
        if (dotIndex <= 0)
            return true;

        var prefix = trimmedExpression[..dotIndex].Trim().Trim('`');
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

        foreach (var source in sampleRow.Sources.Values)
            AppendSourceColumns(columns, usedAliases, evaluators, source, resolveColumn);

        return false;
    }

    internal static string GetSelectProjectionTableAlias(SqlSelectQuery query)
        => query.Table?.Alias ?? query.Table?.TableFunction?.Name ?? query.Table?.Name ?? string.Empty;

    internal static string InferColumnAlias(string rawExpression)
    {
        var normalized = rawExpression.Trim();
        var dotIndex = normalized.LastIndexOf('.');
        if (dotIndex >= 0 && dotIndex + 1 < normalized.Length)
            return normalized[(dotIndex + 1)..].Trim().Trim('`');

        return normalized.Trim().Trim('`');
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
        foreach (var columnName in source.ColumnNames)
        {
            var alias = MakeUniqueAlias(usedAliases, columnName, source.Alias);
            var isJsonFragment = source.TryGetColumnMetadata(columnName, out var metadata)
                && metadata is not null
                && metadata.IsJsonFragment;
            var dbType = metadata?.DbType ?? DbType.Object;
            var isNullable = metadata?.IsNullable ?? true;
            columns.Add(new TableResultColMock(source.Alias, alias, columnName, columns.Count, dbType, isNullable, isJsonFragment));
            evaluators.Add((row, group) => resolveColumn(source.Alias, columnName, row));
        }
    }
}
