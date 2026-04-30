namespace DbSqlLikeMem;

internal static class QueryRowLimitHelper
{
    internal static void ApplyLimit(TableResultMock result, SqlRowLimit? rowLimit, Func<SqlExpr, int> eval)
    {
        if (rowLimit is null)
            return;

        SqlExpr? takeExpr = null;
        SqlExpr? offsetExpr = null;

        switch (rowLimit)
        {
            case SqlLimitOffset limitOffset:
                takeExpr = limitOffset.Count;
                offsetExpr = limitOffset.Offset;
                break;
            case SqlFetch fetch:
                takeExpr = fetch.Count;
                offsetExpr = fetch.Offset;
                break;
            case SqlTop top:
                takeExpr = top.Count;
                break;
        }

        var take = takeExpr is null ? int.MaxValue : eval(takeExpr);
        var offset = offsetExpr is null ? 0 : eval(offsetExpr);

        if (offset <= 0 && take == int.MaxValue)
            return;

        var startIndex = Math.Min(Math.Max(offset, 0), result.Count);
        var endIndex = take == int.MaxValue
            ? result.Count
            : Math.Min(result.Count, startIndex + Math.Max(take, 0));

        if (startIndex == 0 && endIndex == result.Count)
            return;

        if (endIndex < result.Count)
            result.RemoveRange(endIndex, result.Count - endIndex);

        if (startIndex > 0)
            result.RemoveRange(0, startIndex);
    }
}
