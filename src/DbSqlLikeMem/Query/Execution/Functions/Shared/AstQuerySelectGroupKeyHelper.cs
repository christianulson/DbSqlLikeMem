namespace DbSqlLikeMem;

internal static class AstQuerySelectGroupKeyHelper
{
    internal static SqlExpr[] BuildGroupByKeyExpressions(
        SqlSelectQuery query,
        Func<string, SqlExpr> parseExpr)
    {
        var keyExprs = new List<SqlExpr>(query.GroupBy.Count);

        foreach (var groupByRaw in query.GroupBy)
        {
            var raw = groupByRaw.Trim();
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ord))
            {
                if (ord < 1)
                    throw new InvalidOperationException("invalid: GROUP BY ordinal must be >= 1");

                var idx = ord - 1;
                if (idx >= query.SelectItems.Count)
                    throw new InvalidOperationException($"invalid: GROUP BY ordinal {ord} out of range");

                var selectItem = query.SelectItems[idx];
                var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
                keyExprs.Add(parseExpr(exprRaw));
                continue;
            }

            keyExprs.Add(parseExpr(groupByRaw));
        }

        return [.. keyExprs];
    }
}
