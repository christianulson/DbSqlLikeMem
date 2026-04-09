namespace DbSqlLikeMem;

internal static class SqlExecutionPlanFormatterTextHelper
{
    internal static string FormatSource(SqlTableSource? source)
        => SqlSourceFormattingHelper.FormatSource(source);

    internal static string FormatJoinLine(SqlJoin join)
    {
        var joinType = FormatJoinType(join.Type);
        var source = FormatSource(join.Table);

        if (join.Type is SqlJoinType.CrossApply or SqlJoinType.OuterApply)
            return $"- JOIN: {joinType} {source}";

        var on = SqlExprPrinter.Print(join.On);
        return $"- JOIN: {joinType} {source} ON {on}";
    }

    internal static string FormatLimit(SqlRowLimit rowLimit)
        => rowLimit switch
        {
            SqlLimitOffset l => l.Offset is not null ? $"LIMIT {SqlExprPrinter.Print(l.Count)} OFFSET {SqlExprPrinter.Print(l.Offset)}" : $"LIMIT {SqlExprPrinter.Print(l.Count)}",
            SqlTop t => $"TOP {SqlExprPrinter.Print(t.Count)}",
            SqlFetch f => f.Offset is not null ? $"FETCH {SqlExprPrinter.Print(f.Count)} OFFSET {SqlExprPrinter.Print(f.Offset)}" : $"FETCH {SqlExprPrinter.Print(f.Count)}",
            _ => rowLimit.ToString() ?? "<limit>"
        };

    private static string FormatJsonTableClauseShape(SqlJsonTableClause clause)
    {
        if (clause.NestedPaths.Count == 0)
            return "COLUMNS (...)";

        var nestedShapes = string.Join(", ", clause.NestedPaths.Select(FormatJsonTableNestedPathShape));
        return $"COLUMNS (..., {nestedShapes})";
    }

    private static string FormatJsonTableNestedPathShape(SqlJsonTableNestedPath nestedPath)
    {
        if (nestedPath.Clause.NestedPaths.Count == 0)
            return "NESTED PATH (...)";

        return $"NESTED PATH (..., {string.Join(", ", nestedPath.Clause.NestedPaths.Select(FormatJsonTableNestedPathShape))})";
    }

    private static string FormatJoinType(SqlJoinType joinType)
        => joinType switch
        {
            SqlJoinType.CrossApply => "CROSS APPLY",
            SqlJoinType.OuterApply => "OUTER APPLY",
            _ => joinType.ToString().ToUpperInvariant()
        };
}
