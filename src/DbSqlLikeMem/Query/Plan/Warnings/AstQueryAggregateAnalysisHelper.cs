namespace DbSqlLikeMem;

internal static class AstQueryAggregateAnalysisHelper
{
    internal static bool ContainsAggregate(
        SqlSelectQuery query,
        Func<string, SqlExpr> parseScalarExpr,
        Func<SqlExpr, bool> walkHasAggregate)
    {
        if (query.SelectItems is null || query.SelectItems.Count == 0)
            return false;

        foreach (var selectItem in query.SelectItems)
        {
            var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
            if (SelectItemContainsAggregate(exprRaw, parseScalarExpr, walkHasAggregate))
                return true;
        }

        return query.Having is not null && walkHasAggregate(query.Having);
    }

    private static bool SelectItemContainsAggregate(
        string exprRaw,
        Func<string, SqlExpr> parseScalarExpr,
        Func<SqlExpr, bool> walkHasAggregate)
    {
        var looksAggregatedOutsideSubqueries = AggregateExpressionInspector.LooksLikeAggregateExpression(exprRaw);
        if (!looksAggregatedOutsideSubqueries)
            return false;

#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            var parsedExpression = parseScalarExpr(exprRaw);
            return walkHasAggregate(parsedExpression);
        }
        catch (Exception e)
        {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
            Console.WriteLine($"{nameof(AstQueryAggregateAnalysisHelper)}.{nameof(ContainsAggregate)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
            Console.WriteLine(e);

            return AggregateExpressionInspector.LooksLikeAggregateExpression(exprRaw);
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }
}
