namespace DbSqlLikeMem;

internal static class QueryPlanWarningHelper
{
    internal static IReadOnlyList<SqlPlanWarning> BuildPlanWarnings(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
    {
        const long HighReadThreshold = 100;
        const long VeryHighReadThreshold = 1000;
        const long CriticalReadThreshold = 5000;
        const double LowSelectivityThresholdPct = 60d;
        const double VeryLowSelectivityThresholdPct = 85d;

        if (metrics.EstimatedRowsRead < HighReadThreshold)
            return [];

        var warnings = new List<SqlPlanWarning>();

        if (query.OrderBy.Count > 0 && query.RowLimit is null && !HasTopPrefixInProjection(query))
        {
            warnings.Add(new SqlPlanWarning(
                "PW001",
                SqlExecutionPlanMessages.WarningOrderByWithoutLimitMessage(),
                SqlExecutionPlanMessages.WarningOrderByWithoutLimitReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningOrderByWithoutLimitAction(),
                SqlPlanWarningSeverity.High,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold))));
        }

        if (metrics.SelectivityPct >= LowSelectivityThresholdPct)
        {
            var severity = metrics.SelectivityPct >= VeryLowSelectivityThresholdPct
                ? SqlPlanWarningSeverity.High
                : SqlPlanWarningSeverity.Warning;

            var message = severity == SqlPlanWarningSeverity.High
                ? SqlExecutionPlanMessages.WarningLowSelectivityHighImpactMessage()
                : SqlExecutionPlanMessages.WarningLowSelectivityMessage();

            warnings.Add(new SqlPlanWarning(
                "PW002",
                message,
                SqlExecutionPlanMessages.WarningLowSelectivityReason(metrics.SelectivityPct, metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningLowSelectivityAction(),
                severity,
                "SelectivityPct",
                metrics.SelectivityPct.ToString("F2", CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", LowSelectivityThresholdPct), ("highImpactGte", VeryLowSelectivityThresholdPct))));
        }

        if (HasSelectStar(query))
        {
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

            warnings.Add(new SqlPlanWarning(
                "PW003",
                message,
                SqlExecutionPlanMessages.WarningSelectStarReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningSelectStarAction(),
                severity,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold), ("warningGte", VeryHighReadThreshold), ("highGte", CriticalReadThreshold))));
        }

        if (query.Where is null && !query.Distinct)
        {
            var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
                ? SqlPlanWarningSeverity.High
                : SqlPlanWarningSeverity.Warning;

            var message = severity == SqlPlanWarningSeverity.High
                ? SqlExecutionPlanMessages.WarningNoWhereHighReadHighImpactMessage()
                : SqlExecutionPlanMessages.WarningNoWhereHighReadMessage();

            warnings.Add(new SqlPlanWarning(
                "PW004",
                message,
                SqlExecutionPlanMessages.WarningNoWhereHighReadReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningNoWhereHighReadAction(),
                severity,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold), ("highGte", CriticalReadThreshold))));
        }

        if (query.Distinct)
        {
            var severity = metrics.EstimatedRowsRead >= CriticalReadThreshold
                ? SqlPlanWarningSeverity.High
                : SqlPlanWarningSeverity.Warning;

            var message = severity == SqlPlanWarningSeverity.High
                ? SqlExecutionPlanMessages.WarningDistinctHighReadHighImpactMessage()
                : SqlExecutionPlanMessages.WarningDistinctHighReadMessage();

            warnings.Add(new SqlPlanWarning(
                "PW005",
                message,
                SqlExecutionPlanMessages.WarningDistinctHighReadReason(metrics.EstimatedRowsRead),
                SqlExecutionPlanMessages.WarningDistinctHighReadAction(),
                severity,
                "EstimatedRowsRead",
                metrics.EstimatedRowsRead.ToString(CultureInfo.InvariantCulture),
                BuildTechnicalThreshold(("gte", HighReadThreshold), ("highGte", CriticalReadThreshold))));
        }

        return warnings;
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

        return query.SelectItems.Any(selectItem => Regex.IsMatch(
            selectItem.Raw,
            @"^\s*TOP\s*(\(\s*\d+\s*\)|\d+)\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant));
    }

    private static bool HasSelectStar(SqlSelectQuery query)
        => query.SelectItems.Any(static item => string.Equals(item.Raw?.Trim(), "*", StringComparison.Ordinal));

    private static string BuildTechnicalThreshold(params (string Key, IFormattable Value)[] values)
    {
        if (values.Length == 0)
            return string.Empty;

        return string.Join(";", values.Select(static value =>
        {
            if (string.IsNullOrWhiteSpace(value.Key))
                throw new ArgumentException("Threshold key must be provided.", nameof(values));

            return $"{value.Key}:{value.Value.ToString(null, CultureInfo.InvariantCulture)}";
        }));
    }
}
