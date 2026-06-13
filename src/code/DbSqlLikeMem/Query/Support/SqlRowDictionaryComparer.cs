namespace DbSqlLikeMem;

internal sealed class SqlRowDictionaryComparer(QueryExecutionContext context)
    : IEqualityComparer<Dictionary<int, object?>>
{
    private readonly ISqlDialect dialect = context.Dialect;

    public bool Equals(Dictionary<int, object?>? x, Dictionary<int, object?>? y)
    {
        if (ReferenceEquals(x, y))
            return true;
        if (x is null || y is null || x.Count != y.Count)
            return false;

        foreach (var item in x)
        {
            if (!y.TryGetValue(item.Key, out var rightValue))
                return false;

            if (!item.Value.EqualsSql(rightValue, context))
                return false;
        }

        return true;
    }

    public int GetHashCode(Dictionary<int, object?> row)
    {
        var hc = 0;
        foreach (var entry in row)
            hc += (entry.Key.GetHashCode() * 397) ^ entry.Value.GetHashCodeSql(dialect);
        return hc;
    }
}
