namespace DbSqlLikeMem;

internal static class QueryDebugTraceFormattingHelper
{
    internal static string FormatLimitDebugDetails(SqlRowLimit rowLimit)
        => rowLimit switch
        {
            SqlLimitOffset limit => (limit.Offset is not null ? $"count={SqlExprPrinter.Print(limit.Count)};offset={SqlExprPrinter.Print(limit.Offset)}" : $"count={SqlExprPrinter.Print(limit.Count)}"),
            SqlFetch fetch => (fetch.Offset is not null ? $"count={SqlExprPrinter.Print(fetch.Count)};offset={SqlExprPrinter.Print(fetch.Offset)}" : $"count={SqlExprPrinter.Print(fetch.Count)}"),
            SqlTop top => $"count={SqlExprPrinter.Print(top.Count)}",
            _ => string.Empty
        };

    internal static string FormatUnionInputsDebugDetails(IReadOnlyList<SqlSelectQuery> parts, IReadOnlyList<bool> allFlags)
    {
        var distinctCount = allFlags.Count(static flag => !flag);
        var allCount = allFlags.Count - distinctCount;
        return $"parts={parts.Count};allSegments={allCount};distinctSegments={distinctCount}";
    }

    internal static string FormatUnionCombineDebugDetails(IReadOnlyList<SqlSelectQuery> parts, IReadOnlyList<bool> allFlags)
    {
        var mode = allFlags.Any(static flag => !flag) ? "UNION DISTINCT" : "UNION ALL";
        return $"{FormatUnionInputsDebugDetails(parts, allFlags)};mode={mode}";
    }

    internal static string FormatDistinctDebugDetails(int projectionColumnCount)
        => $"columns={projectionColumnCount}";

    internal static string FormatProjectDebugDetails(IReadOnlyList<SqlSelectItem> selectItems)
    {
        var items = selectItems
            .Select(static item =>
            {
                var raw = (item.Raw ?? string.Empty).Trim();
                var alias = (item.Alias ?? string.Empty).Trim();

                if (raw.Length == 0)
                    return alias;

                return alias.Length == 0
                    ? raw
                    : $"{raw} AS {alias}";
            })
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .ToArray();

        return items.Length == 0
            ? $"columns={selectItems.Count}"
            : $"columns={selectItems.Count};items={string.Join("|", items)}";
    }

    internal static string FormatOrderByDebugDetails(IReadOnlyList<SqlOrderByItem> orderBy)
    {
        var items = orderBy
            .Select(static item =>
            {
                var raw = (item.Raw ?? string.Empty).Trim();
                if (raw.Length == 0)
                    return null;

                return raw + (item.Desc ? " DESC" : " ASC");
            })
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .ToArray();

        return items.Length == 0
            ? $"keys={orderBy.Count}"
            : $"keys={orderBy.Count};items={string.Join("|", items)}";
    }

    internal static string FormatGroupDebugDetails(SqlSelectQuery query)
        => query.GroupBy.Count == 0
            ? "aggregate"
            : $"keys={query.GroupBy.Count};items={string.Join("|", query.GroupBy)}";
}
