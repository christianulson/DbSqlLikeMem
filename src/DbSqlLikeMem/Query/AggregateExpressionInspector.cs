namespace DbSqlLikeMem;

internal static class AggregateExpressionInspector
{
    private static readonly Regex AggregateExpressionRegex = new(
        @"\b(COUNT|SUM|MIN|MAX|AVG|GROUP_CONCAT|STRING_AGG|LISTAGG|ANY_VALUE|BIT_AND|BIT_OR|BIT_XOR|JSON_ARRAYAGG|JSON_GROUP_OBJECT|TOTAL|MEDIAN|PERCENTILE|PERCENTILE_CONT|PERCENTILE_DISC|VAR_POP|VAR_SAMP|VARIANCE|COLLECT|CORR|CORR_K|CORR_S|COVAR_POP|COVAR_SAMP|CV|JSON_OBJECTAGG|GROUP_ID|APPROX_COUNT_DISTINCT|APPROX_COUNT_DISTINCT_AGG|APPROX_COUNT_DISTINCT_DETAIL|APPROX_MEDIAN|APPROX_PERCENTILE|APPROX_PERCENTILE_AGG|APPROX_PERCENTILE_DETAIL|REGR_AVGX|REGR_AVGY|REGR_COUNT|REGR_INTERCEPT|REGR_R2|REGR_SLOPE|REGR_SXX|REGR_SXY|REGR_SYY|STDDEV|STDDEV_POP|STDDEV_SAMP|STATS_BINOMIAL_TEST|STATS_CROSSTAB|STATS_F_TEST|STATS_KS_TEST|STATS_MODE|STATS_MW_TEST|STATS_ONE_WAY_ANOVA|STATS_T_TEST_INDEP|STATS_T_TEST_INDEPU|STATS_T_TEST_ONE|STATS_T_TEST_PAIRED|STATS_WSR_TEST|XMLAGG|RATIO_TO_REPORT|ARRAY_AGG|BOOL_AND|BOOL_OR|EVERY|JSON_AGG|JSONB_AGG|JSON_OBJECT_AGG|JSON_OBJECT_AGG_STRICT|JSON_OBJECT_AGG_UNIQUE|JSON_OBJECT_AGG_UNIQUE_STRICT|JSONB_OBJECT_AGG|JSONB_OBJECT_AGG_STRICT|JSONB_OBJECT_AGG_UNIQUE|JSONB_OBJECT_AGG_UNIQUE_STRICT)\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly HashSet<string> AggregateFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT","SUM","MIN","MAX","AVG","GROUP_CONCAT","STRING_AGG","LISTAGG",
        "ANY_VALUE","BIT_AND","BIT_OR","BIT_XOR","JSON_ARRAYAGG","JSON_GROUP_OBJECT","TOTAL",
        "MEDIAN","PERCENTILE","PERCENTILE_CONT","PERCENTILE_DISC","VAR_POP","VAR_SAMP","VARIANCE",
        "COLLECT","CORR","CORR_K","CORR_S","COVAR_POP","COVAR_SAMP","CV","JSON_OBJECTAGG","GROUP_ID",
        "APPROX_COUNT_DISTINCT","APPROX_COUNT_DISTINCT_AGG","APPROX_COUNT_DISTINCT_DETAIL","APPROX_MEDIAN","APPROX_PERCENTILE","APPROX_PERCENTILE_AGG","APPROX_PERCENTILE_DETAIL",
        "REGR_AVGX","REGR_AVGY","REGR_COUNT","REGR_INTERCEPT","REGR_R2","REGR_SLOPE","REGR_SXX","REGR_SXY","REGR_SYY",
        "STDDEV","STDDEV_POP","STDDEV_SAMP","STATS_BINOMIAL_TEST","STATS_CROSSTAB","STATS_F_TEST","STATS_KS_TEST","STATS_MODE","STATS_MW_TEST","STATS_ONE_WAY_ANOVA",
        "STATS_T_TEST_INDEP","STATS_T_TEST_INDEPU","STATS_T_TEST_ONE","STATS_T_TEST_PAIRED","STATS_WSR_TEST","XMLAGG","RATIO_TO_REPORT",
        "ARRAY_AGG","BOOL_AND","BOOL_OR","EVERY","JSON_AGG","JSONB_AGG",
        "JSON_OBJECT_AGG","JSON_OBJECT_AGG_STRICT","JSON_OBJECT_AGG_UNIQUE","JSON_OBJECT_AGG_UNIQUE_STRICT",
        "JSONB_OBJECT_AGG","JSONB_OBJECT_AGG_STRICT","JSONB_OBJECT_AGG_UNIQUE","JSONB_OBJECT_AGG_UNIQUE_STRICT"
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
