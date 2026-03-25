namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private bool ContainsAggregate(SqlSelectQuery q)
    {
        if (q.SelectItems is null || q.SelectItems.Count == 0)
            return false;

        foreach (var si in q.SelectItems)
        {
            var (exprRaw, _) = SplitTrailingAsAlias(si.Raw, si.Alias);
            if (SelectItemContainsAggregate(exprRaw))
                return true;
        }

        return q.Having is not null
            && WalkHasAggregate(q.Having);
    }

    private bool SelectItemContainsAggregate(string exprRaw)
    {
        var looksAggregatedOutsideSubqueries = AggregateExpressionInspector.LooksLikeAggregateExpression(exprRaw);
        if (!looksAggregatedOutsideSubqueries)
            return false;

#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            var parsedExpression = ParseScalarExpr(exprRaw);
            return WalkHasAggregate(parsedExpression);
        }
        catch (Exception e)
        {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
            Console.WriteLine($"{GetType().Name}.{nameof(ContainsAggregate)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
            Console.WriteLine(e);

            return AggregateExpressionInspector.LooksLikeAggregateExpression(exprRaw);
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }

    private static bool WalkHasAggregate(SqlExpr expr)
        => AggregateExpressionInspector.WalkHasAggregate(expr);
}
