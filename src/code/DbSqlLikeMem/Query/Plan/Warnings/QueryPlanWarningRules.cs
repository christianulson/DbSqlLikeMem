namespace DbSqlLikeMem;

internal interface IQueryPlanWarningRule
{
    SqlPlanWarning? Evaluate(SqlSelectQuery query, SqlPlanRuntimeMetrics metrics);
}

internal abstract class QueryPlanWarningRuleBase
{
    protected static string BuildTechnicalThreshold(params (string Key, IFormattable Value)[] values)
    {
        var parts = new string[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            var value = values[i];
            if (string.IsNullOrWhiteSpace(value.Key))
                throw new ArgumentException("Threshold key must be provided.", nameof(values));

            parts[i] = $"{value.Key}:{value.Value.ToString(null, CultureInfo.InvariantCulture)}";
        }

        return string.Join(";", parts);
    }
}

internal sealed class OrderByWithoutLimitWarningRule : QueryPlanWarningRuleBase, IQueryPlanWarningRule
{
    private const long HighReadThreshold = 100;

    public SqlPlanWarning? Evaluate(SqlSelectQuery query, SqlPlanRuntimeMetrics metrics)
    {
        if (metrics.EstimatedRowsRead < HighReadThreshold)
            return null;

        if (query.OrderBy.Count == 0 || query.RowLimit is not null || HasTopPrefixInProjection(query))
            return null;

        return new SqlPlanWarning(
            "PW001",
            SqlExecutionPlanMessages.WarningOrderByWithoutLimitMessage(),
            SqlExecutionPlanMessages.WarningOrderByWithoutLimitReason(metrics.EstimatedRowsRead),
            SqlExecutionPlanMessages.WarningOrderByWithoutLimitAction(),
            SqlPlanWarningSeverity.High,
            "EstimatedRowsRead",
            metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
            BuildTechnicalThreshold(("gte", HighReadThreshold)));
    }

    private static bool HasTopPrefixInProjection(SqlSelectQuery query)
    {
        if (Regex.IsMatch(
            query.RawSql,
            @"^\s*SELECT\s+(?:DISTINCT\s+)?TOP\s*(\(\s*\d+\s*\)|\d+)\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
        {
            return true;
        }

        for (var i = 0; i < query.SelectItems.Count; i++)
        {
            if (Regex.IsMatch(
                query.SelectItems[i].Raw,
                @"^\s*TOP\s*(\(\s*\d+\s*\)|\d+)\b",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            {
                return true;
            }
        }

        return false;
    }
}

internal sealed class LowSelectivityWarningRule : QueryPlanWarningRuleBase, IQueryPlanWarningRule
{
    private const double LowSelectivityThresholdPct = 60d;
    private const double VeryLowSelectivityThresholdPct = 85d;

    public SqlPlanWarning? Evaluate(SqlSelectQuery query, SqlPlanRuntimeMetrics metrics)
    {
        if (metrics.SelectivityPct < LowSelectivityThresholdPct)
            return null;

        var severity = metrics.SelectivityPct >= VeryLowSelectivityThresholdPct
            ? SqlPlanWarningSeverity.High
            : SqlPlanWarningSeverity.Warning;

        var message = severity == SqlPlanWarningSeverity.High
            ? SqlExecutionPlanMessages.WarningLowSelectivityHighImpactMessage()
            : SqlExecutionPlanMessages.WarningLowSelectivityMessage();

        return new SqlPlanWarning(
            "PW002",
            message,
            SqlExecutionPlanMessages.WarningLowSelectivityReason(metrics.SelectivityPct, metrics.EstimatedRowsRead),
            SqlExecutionPlanMessages.WarningLowSelectivityAction(),
            severity,
            "SelectivityPct",
            metrics.SelectivityPct.ToString("F2", CultureInfo.InvariantCulture),
            BuildTechnicalThreshold(("gte", LowSelectivityThresholdPct), ("highImpactGte", VeryLowSelectivityThresholdPct)));
    }
}

internal sealed class SelectStarWarningRule : QueryPlanWarningRuleBase, IQueryPlanWarningRule
{
    private const long HighReadThreshold = 100;
    private const long VeryHighReadThreshold = 1000;
    private const long CriticalReadThreshold = 5000;

    public SqlPlanWarning? Evaluate(SqlSelectQuery query, SqlPlanRuntimeMetrics metrics)
    {
        if (!HasSelectStar(query))
            return null;

        var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
            ? SqlPlanWarningSeverity.High
            : metrics.EstimatedRowsRead >= VeryHighReadThreshold
                ? SqlPlanWarningSeverity.Warning
                : SqlPlanWarningSeverity.Info;

        var message = severity switch
        {
            SqlPlanWarningSeverity.High => SqlExecutionPlanMessages.WarningSelectStarCriticalImpactMessage(),
            SqlPlanWarningSeverity.Warning => SqlExecutionPlanMessages.WarningSelectStarHighImpactMessage(),
            _ => SqlExecutionPlanMessages.WarningSelectStarMessage()
        };

        return new SqlPlanWarning(
            "PW003",
            message,
            SqlExecutionPlanMessages.WarningSelectStarReason(metrics.EstimatedRowsRead),
            SqlExecutionPlanMessages.WarningSelectStarAction(),
            severity,
            "EstimatedRowsRead",
            metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
            BuildTechnicalThreshold(("gte", HighReadThreshold), ("warningGte", VeryHighReadThreshold), ("highGte", CriticalReadThreshold)));
    }

    private static bool HasSelectStar(SqlSelectQuery query)
    {
        for (var i = 0; i < query.SelectItems.Count; i++)
        {
            var item = query.SelectItems[i];
            if (string.Equals(item.Raw?.Trim(), "*", StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}

internal sealed class NoWhereHighReadWarningRule : QueryPlanWarningRuleBase, IQueryPlanWarningRule
{
    private const long HighReadThreshold = 100;
    private const long CriticalReadThreshold = 5000;

    public SqlPlanWarning? Evaluate(SqlSelectQuery query, SqlPlanRuntimeMetrics metrics)
    {
        if (query.Where is not null || query.HasDistinctClause())
            return null;

        var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
            ? SqlPlanWarningSeverity.High
            : SqlPlanWarningSeverity.Warning;

        var message = severity == SqlPlanWarningSeverity.High
            ? SqlExecutionPlanMessages.WarningNoWhereHighReadHighImpactMessage()
            : SqlExecutionPlanMessages.WarningNoWhereHighReadMessage();

        return new SqlPlanWarning(
            "PW004",
            message,
            SqlExecutionPlanMessages.WarningNoWhereHighReadReason(metrics.EstimatedRowsRead),
            SqlExecutionPlanMessages.WarningNoWhereHighReadAction(),
            severity,
            "EstimatedRowsRead",
            metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
            BuildTechnicalThreshold(("gte", HighReadThreshold), ("highGte", CriticalReadThreshold)));
    }
}

internal sealed class DistinctHighReadWarningRule : QueryPlanWarningRuleBase, IQueryPlanWarningRule
{
    private const long HighReadThreshold = 100;
    private const long CriticalReadThreshold = 5000;

    public SqlPlanWarning? Evaluate(SqlSelectQuery query, SqlPlanRuntimeMetrics metrics)
    {
        if (!query.HasDistinctClause())
            return null;

        var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
            ? SqlPlanWarningSeverity.High
            : SqlPlanWarningSeverity.Warning;

        var message = severity == SqlPlanWarningSeverity.High
            ? SqlExecutionPlanMessages.WarningDistinctHighReadHighImpactMessage()
            : SqlExecutionPlanMessages.WarningDistinctHighReadMessage();

        return new SqlPlanWarning(
            "PW005",
            message,
            SqlExecutionPlanMessages.WarningDistinctHighReadReason(metrics.EstimatedRowsRead),
            SqlExecutionPlanMessages.WarningDistinctHighReadAction(),
            severity,
            "EstimatedRowsRead",
            metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
            BuildTechnicalThreshold(("gte", HighReadThreshold), ("highGte", CriticalReadThreshold)));
    }
}
