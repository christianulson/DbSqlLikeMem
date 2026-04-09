namespace DbSqlLikeMem;

internal static class SqlExecutionPlanFormatterWarningsHelper
{
    internal static void AppendPlanWarnings(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanWarningsLabel()}:");
        foreach (var warning in planWarnings)
        {
            sb.AppendLine($"  - {SqlExecutionPlanMessages.CodeLabel()}: {warning.Code}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.MessageLabel()}: {warning.Message}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.ReasonLabel()}: {warning.Reason}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.SuggestedActionLabel()}: {warning.SuggestedAction}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.SeverityLabel()}: {FormatWarningSeverity(warning.Severity)}");

            if (!string.IsNullOrWhiteSpace(warning.MetricName))
                sb.AppendLine($"    {SqlExecutionPlanMessages.MetricNameLabel()}: {warning.MetricName}");

            if (!string.IsNullOrWhiteSpace(warning.ObservedValue))
                sb.AppendLine($"    {SqlExecutionPlanMessages.ObservedValueLabel()}: {warning.ObservedValue}");

            if (!string.IsNullOrWhiteSpace(warning.Threshold))
                sb.AppendLine($"    {SqlExecutionPlanMessages.ThresholdLabel()}: {warning.Threshold}");
        }
    }

    internal static void AppendPlanRiskScore(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var score = CalculatePlanRiskScore(planWarnings);
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanRiskScoreLabel()}: {score}");
    }

    internal static void AppendPlanQualityGrade(
        StringBuilder sb,
        SqlPlanRuntimeMetrics metrics,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var riskScore = CalculatePlanRiskScore(planWarnings);
        var performanceBand = GetPlanPerformanceBand(metrics.ElapsedMs);
        var grade = CalculatePlanQualityGrade(riskScore, performanceBand);
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanQualityGradeLabel()}: {grade}");
    }

    internal static void AppendPlanWarningSummary(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var summary = string.Join(
            ";",
            planWarnings
                .OrderByDescending(static w => GetSeverityWeight(w.Severity))
                .ThenBy(static w => w.Code, StringComparer.Ordinal)
                .Select(static w => $"{w.Code}:{w.Severity}"));

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanWarningSummaryLabel()}: {summary}");
    }

    internal static void AppendPlanWarningCounts(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var high = planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.High);
        var warning = planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.Warning);
        var info = planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.Info);

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanWarningCountsLabel()}: high:{high};warning:{warning};info:{info}");
    }

    internal static void AppendPlanNoiseScore(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var noiseScore = CalculatePlanNoiseScore(planWarnings);
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanNoiseScoreLabel()}: {noiseScore}");
    }

    internal static void AppendPlanTopActions(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        var actions = BuildTopActions(indexRecommendations, planWarnings);
        if (actions.Count == 0)
            return;

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanTopActionsLabel()}: {string.Join(";", actions)}");
    }

    internal static void AppendPrimaryWarning(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var primary = planWarnings
            .OrderByDescending(static w => GetSeverityWeight(w.Severity))
            .ThenBy(static w => w.Code, StringComparer.Ordinal)
            .First();

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanPrimaryWarningLabel()}: {primary.Code}:{primary.Severity}");
    }

    internal static void AppendPlanPrimaryCauseGroup(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var primary = planWarnings
            .OrderByDescending(static w => GetSeverityWeight(w.Severity))
            .ThenBy(static w => w.Code, StringComparer.Ordinal)
            .First();

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanPrimaryCauseGroupLabel()}: {MapPrimaryCauseGroup(primary.Code)}");
    }

    internal static void AppendPlanDelta(
        StringBuilder sb,
        SqlPlanRuntimeMetrics currentMetrics,
        IReadOnlyList<SqlPlanWarning>? currentWarnings,
        SqlPlanRuntimeMetrics? previousMetrics,
        IReadOnlyList<SqlPlanWarning>? previousWarnings)
    {
        if (previousMetrics is null)
            return;

        var currentRisk = currentWarnings is { Count: > 0 } ? CalculatePlanRiskScore(currentWarnings) : 0;
        var previousRisk = previousWarnings is { Count: > 0 } ? CalculatePlanRiskScore(previousWarnings) : 0;
        var riskDelta = currentRisk - previousRisk;
        var elapsedDelta = currentMetrics.ElapsedMs - previousMetrics.ElapsedMs;

        var riskPrefix = riskDelta >= 0 ? "+" : string.Empty;
        var elapsedPrefix = elapsedDelta >= 0 ? "+" : string.Empty;
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanDeltaLabel()}: riskDelta:{riskPrefix}{riskDelta};elapsedMsDelta:{elapsedPrefix}{elapsedDelta}");
    }

    internal static void AppendPlanSeverityHint(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings,
        SqlPlanSeverityHintContext severityHintContext)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var high = planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.High);
        var warning = planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.Warning);
        var info = planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.Info);

        var score = high * 3 + warning * 2 + info;
        var threshold = severityHintContext switch
        {
            SqlPlanSeverityHintContext.Dev => 3,
            SqlPlanSeverityHintContext.Ci => 2,
            SqlPlanSeverityHintContext.Prod => 1,
            _ => 3
        };

        var level = score >= threshold * 3
            ? "High"
            : score >= threshold * 2
                ? "Warning"
                : "Info";

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanSeverityHintLabel()}: context:{severityHintContext.ToString().ToLowerInvariant()};level:{level}");
    }

    private static List<string> BuildTopActions(
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        var actions = new List<string>(3);

        if (planWarnings is { Count: > 0 })
        {
            foreach (var warning in planWarnings
                         .OrderByDescending(static w => GetSeverityWeight(w.Severity))
                         .ThenBy(static w => w.Code, StringComparer.Ordinal))
            {
                actions.Add($"{warning.Code}:{MapWarningActionKey(warning.Code)}");
                if (actions.Count == 3)
                    return actions;
            }
        }

        if (indexRecommendations is { Count: > 0 } && actions.Count < 3)
            actions.Add("IDX:CreateSuggestedIndex");

        return actions;
    }

    private static string MapWarningActionKey(string warningCode)
        => warningCode switch
        {
            "PW001" => "AddSelectiveFilter",
            "PW002" => "AddOrderByIndex",
            "PW003" => "ReduceProjectionColumns",
            "PW004" => "AddSelectiveFilter",
            "PW005" => "CreateDistinctCoveringIndex",
            _ => "ReviewWarning"
        };

    private static string MapPrimaryCauseGroup(string warningCode)
        => warningCode switch
        {
            "PW001" => "SortWithoutLimit",
            "PW002" => "LowSelectivityPredicate",
            "PW003" => "WideProjection",
            "PW004" => "ScanWithoutFilter",
            "PW005" => "DistinctOverHighRead",
            _ => "GeneralPlanRisk"
        };

    private static int GetSeverityWeight(SqlPlanWarningSeverity severity)
        => severity switch
        {
            SqlPlanWarningSeverity.Info => 1,
            SqlPlanWarningSeverity.Warning => 2,
            SqlPlanWarningSeverity.High => 3,
            _ => 0
        };

    private static int CalculatePlanRiskScore(IReadOnlyList<SqlPlanWarning> warnings)
    {
        var total = 0;
        foreach (var warning in warnings)
        {
            total += warning.Severity switch
            {
                SqlPlanWarningSeverity.Info => 10,
                SqlPlanWarningSeverity.Warning => 30,
                SqlPlanWarningSeverity.High => 50,
                _ => 0
            };
        }

        return Math.Min(100, total);
    }

    private static int CalculatePlanNoiseScore(IReadOnlyList<SqlPlanWarning> warnings)
    {
        if (warnings.Count <= 1)
            return 0;

        var duplicatedSignals = warnings
            .Where(static w => !string.IsNullOrWhiteSpace(w.MetricName) && !string.IsNullOrWhiteSpace(w.Threshold))
            .GroupBy(static w => $"{w.MetricName}:{w.Threshold}", StringComparer.Ordinal)
            .Sum(static g => Math.Max(0, g.Count() - 1));

        var score = duplicatedSignals * 100d / warnings.Count;
        return (int)Math.Round(score, MidpointRounding.AwayFromZero);
    }

    private static string CalculatePlanQualityGrade(int riskScore, string performanceBand)
    {
        var baseGrade = riskScore switch
        {
            <= 20 => 0,
            <= 50 => 1,
            <= 80 => 2,
            _ => 3
        };

        var performancePenalty = performanceBand switch
        {
            "Fast" => 0,
            "Moderate" => 1,
            _ => 2
        };

        var finalGrade = Math.Min(3, baseGrade + performancePenalty);
        return finalGrade switch
        {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D"
        };
    }

    private static string GetPlanPerformanceBand(long elapsedMs)
        => elapsedMs switch
        {
            <= 5 => "Fast",
            <= 30 => "Moderate",
            _ => "Slow"
        };

    private static string FormatWarningSeverity(SqlPlanWarningSeverity severity)
        => severity switch
        {
            SqlPlanWarningSeverity.Info => SqlExecutionPlanMessages.SeverityInfoValue(),
            SqlPlanWarningSeverity.Warning => SqlExecutionPlanMessages.SeverityWarningValue(),
            SqlPlanWarningSeverity.High => SqlExecutionPlanMessages.SeverityHighValue(),
            _ => severity.ToString()
        };
}
