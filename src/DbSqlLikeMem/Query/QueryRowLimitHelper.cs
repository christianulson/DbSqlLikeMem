namespace DbSqlLikeMem;

internal static class QueryRowLimitHelper
{
    internal static void ApplyLimit(TableResultMock result, SqlSelectQuery query)
    {
        int? offset = null;
        int take;
        switch (query.RowLimit)
        {
            case SqlLimitOffset limitOffset:
                offset = limitOffset.Offset;
                take = limitOffset.Count;
                break;
            case SqlFetch fetch:
                offset = fetch.Offset;
                take = fetch.Count;
                break;
            case SqlTop top:
                take = top.Count;
                break;
            default:
                return;
        }

        var skip = offset ?? 0;
        var slicedRows = result.Skip(skip).Take(take).ToList();
        result.Clear();
        foreach (var row in slicedRows)
            result.Add(row);
    }
}
