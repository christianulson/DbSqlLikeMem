namespace DbSqlLikeMem;

internal static class AggregateExpressionInspector
{
    private static readonly Regex AggregateExpressionRegex = new(
        @"\b(COUNT|SUM|MIN|MAX|AVG|GROUP_CONCAT|STRING_AGG|LISTAGG)\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly HashSet<string> AggregateFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT","SUM","MIN","MAX","AVG","GROUP_CONCAT","STRING_AGG","LISTAGG"
    };

    internal static bool LooksLikeAggregateExpression(string exprRaw)
        => AggregateExpressionRegex.IsMatch(exprRaw);

    internal static bool WalkHasAggregate(SqlExpr expr) => expr switch
    {
        CallExpr call => HasAggregateFunctionCall(call.Name, call.Args),
        FunctionCallExpr function => HasAggregateFunctionCall(function.Name, function.Args),
        BinaryExpr binary => WalkHasAggregate(binary.Left) || WalkHasAggregate(binary.Right),
        UnaryExpr unary => WalkHasAggregate(unary.Expr),
        LikeExpr like => WalkHasAggregateLike(like),
        InExpr inExpression => WalkHasAggregateIn(inExpression),
        IsNullExpr isNull => WalkHasAggregate(isNull.Expr),
        QuantifiedComparisonExpr quantified => WalkHasAggregate(quantified.Left) || WalkHasAggregate(quantified.Subquery),
        ExistsExpr => true,
        RowExpr row => WalkHasAggregateSequence(row.Items),
        _ => false
    };

    private static bool HasAggregateFunctionCall(string name, IReadOnlyList<SqlExpr> args)
        => AggregateFunctionNames.Contains(name) || WalkHasAggregateSequence(args);

    private static bool WalkHasAggregateLike(LikeExpr like)
        => WalkHasAggregate(like.Left)
            || WalkHasAggregate(like.Pattern)
            || (like.Escape is not null && WalkHasAggregate(like.Escape));

    private static bool WalkHasAggregateIn(InExpr inExpression)
        => WalkHasAggregate(inExpression.Left)
            || WalkHasAggregateSequence(inExpression.Items);

    private static bool WalkHasAggregateSequence(IEnumerable<SqlExpr> expressions)
    {
        foreach (var expression in expressions)
        {
            if (WalkHasAggregate(expression))
                return true;
        }

        return false;
    }
}
