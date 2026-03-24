namespace DbSqlLikeMem;

internal static class QueryPlanWarningHelper
{
    private static readonly IQueryPlanWarningRule[] _rules =
    [
        new OrderByWithoutLimitWarningRule(),
        new LowSelectivityWarningRule(),
        new SelectStarWarningRule(),
        new NoWhereHighReadWarningRule(),
        new DistinctHighReadWarningRule()
    ];

    internal static IReadOnlyList<SqlPlanWarning> BuildPlanWarnings(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
    {
        if (metrics.EstimatedRowsRead < 100)
            return [];

        var warnings = new List<SqlPlanWarning>();
        foreach (var rule in _rules)
        {
            var warning = rule.Evaluate(query, metrics);
            if (warning is not null)
                warnings.Add(warning);
        }

        return warnings;
    }
}
