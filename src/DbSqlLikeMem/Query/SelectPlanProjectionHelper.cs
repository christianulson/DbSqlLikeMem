using System.Data;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem;

internal static class SelectPlanProjectionHelper
{
    internal static bool IncludeExtraColumns(
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        List<TableResultColMock> columns,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        string rawExpression,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        var starMatch = Regex.Match(rawExpression, @"^(?<p>.+?)\s*\.\s*\*\s*$");
        if (!starMatch.Success)
            return true;

        var prefix = starMatch.Groups["p"].Value.Trim().Trim('`');
        var sample = sampleRows.FirstOrDefault();
        if (sample is null)
            return true;

        if (!sample.Sources.TryGetValue(prefix, out var source))
        {
            var matchedKey = sample.Sources.Keys.FirstOrDefault(key => key.Equals(prefix, StringComparison.OrdinalIgnoreCase));
            if (matchedKey is not null)
                source = sample.Sources[matchedKey];
        }

        if (source is null)
            return true;

        AppendSourceColumns(columns, evaluators, source, resolveColumn);
        return false;
    }

    internal static bool ExpandSelectAsterisk(
        List<TableResultColMock> columns,
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
            AppendSourceColumns(columns, evaluators, source, resolveColumn);

        return false;
    }

    internal static string GetSelectProjectionTableAlias(SqlSelectQuery query)
        => query.Table?.Alias ?? query.Table?.Name ?? string.Empty;

    internal static string InferColumnAlias(string rawExpression)
    {
        var normalized = Regex.Replace(rawExpression.Trim(), @"\s*\.\s*", ".");
        var dotIndex = normalized.LastIndexOf('.');
        if (dotIndex >= 0 && dotIndex + 1 < normalized.Length)
            return normalized[(dotIndex + 1)..].Trim().Trim('`');

        return normalized.Trim().Trim('`');
    }

    internal static string MakeUniqueAlias(
        List<TableResultColMock> columns,
        string preferredAlias,
        string tableAlias)
    {
        if (!columns.Any(column => column.ColumnAlias.Equals(preferredAlias, StringComparison.OrdinalIgnoreCase)))
            return preferredAlias;

        var alternativeAlias = $"{tableAlias}_{preferredAlias}";
        if (!columns.Any(column => column.ColumnAlias.Equals(alternativeAlias, StringComparison.OrdinalIgnoreCase)))
            return alternativeAlias;

        var suffix = 2;
        while (true)
        {
            var candidateAlias = $"{alternativeAlias}_{suffix}";
            if (!columns.Any(column => column.ColumnAlias.Equals(candidateAlias, StringComparison.OrdinalIgnoreCase)))
                return candidateAlias;

            suffix++;
        }
    }

    internal static TableResultColMock CreateSelectPlanColumn(
        string tableAlias,
        string columnAlias,
        int columnIndex,
        DbType dbType)
        => new(
            tableAlias: tableAlias,
            columnAlias: columnAlias,
            columnName: columnAlias,
            columIndex: columnIndex,
            dbType: dbType,
            isNullable: true);

    private static void AppendSourceColumns(
        List<TableResultColMock> columns,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        AstQueryExecutorBase.Source source,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        foreach (var columnName in source.ColumnNames)
        {
            var alias = MakeUniqueAlias(columns, columnName, source.Alias);
            columns.Add(new TableResultColMock(source.Alias, alias, columnName, columns.Count, DbType.Object, true));
            evaluators.Add((row, group) => resolveColumn(source.Alias, columnName, row));
        }
    }
}
