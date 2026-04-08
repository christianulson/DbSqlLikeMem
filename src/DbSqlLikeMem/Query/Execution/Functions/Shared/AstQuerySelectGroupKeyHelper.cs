namespace DbSqlLikeMem;

internal static class AstQuerySelectGroupKeyHelper
{
    internal static SqlExpr[] BuildGroupByKeyExpressions(
        SqlSelectQuery query,
        Func<string, SqlExpr> parseExpr)
    {
        var groupBy = query.GroupBy;
        var selectItems = query.SelectItems;
        var groupByCount = groupBy.Count;
        var selectItemsCount = selectItems.Count;
        var keyExprs = new SqlExpr[groupByCount];

        for (var i = 0; i < groupByCount; i++)
        {
            var raw = groupBy[i];
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ord))
            {
                if (ord < 1)
                    throw new InvalidOperationException("invalid: GROUP BY ordinal must be >= 1");

                var idx = ord - 1;
                if (idx >= selectItemsCount)
                    throw new InvalidOperationException($"invalid: GROUP BY ordinal {ord} out of range");

                var selectItem = selectItems[idx];
                var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
                keyExprs[i] = parseExpr(exprRaw);
                continue;
            }

            keyExprs[i] = parseExpr(raw);
        }

        return keyExprs;
    }
}
