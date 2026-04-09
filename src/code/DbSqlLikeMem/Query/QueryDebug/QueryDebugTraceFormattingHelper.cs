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
        var mode = "UNION ALL";
        for (var i = 0; i < allFlags.Count; i++)
        {
            if (!allFlags[i])
            {
                mode = "UNION DISTINCT";
                break;
            }
        }

        return $"{FormatUnionInputsDebugDetails(parts, allFlags)};mode={mode}";
    }

    internal static string FormatDistinctDebugDetails(int projectionColumnCount)
        => $"columns={projectionColumnCount}";

    internal static string FormatProjectDebugDetails(IReadOnlyList<SqlSelectItem> selectItems)
    {
        var items = new List<string>(selectItems.Count);
        for (var i = 0; i < selectItems.Count; i++)
        {
            var item = selectItems[i];
            var raw = (item.Raw ?? string.Empty).Trim();
            var alias = (item.Alias ?? string.Empty).Trim();

            if (raw.Length == 0)
            {
                if (alias.Length > 0)
                    items.Add(alias);
                continue;
            }

            items.Add(alias.Length == 0 ? raw : $"{raw} AS {alias}");
        }

        return items.Count == 0
            ? $"columns={selectItems.Count}"
            : $"columns={selectItems.Count};items={string.Join("|", items)}";
    }

    internal static string FormatOrderByDebugDetails(IReadOnlyList<SqlOrderByItem> orderBy)
    {
        var items = new List<string>(orderBy.Count);
        for (var i = 0; i < orderBy.Count; i++)
        {
            var item = orderBy[i];
            var raw = (item.Raw ?? string.Empty).Trim();
            if (raw.Length == 0)
                continue;

            items.Add(raw + (item.Desc ? " DESC" : " ASC"));
        }

        return items.Count == 0
            ? $"keys={orderBy.Count}"
            : $"keys={orderBy.Count};items={string.Join("|", items)}";
    }

    internal static string FormatGroupDebugDetails(SqlSelectQuery query)
        => query.GroupBy.Count == 0
            ? "aggregate"
            : $"keys={query.GroupBy.Count};items={string.Join("|", query.GroupBy)}";
}
