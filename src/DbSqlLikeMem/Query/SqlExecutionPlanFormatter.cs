using System.Text;
using System.Text.Json;

namespace DbSqlLikeMem;

internal sealed record SqlPlanRuntimeMetrics(
    int InputTables,
    long EstimatedRowsRead,
    int ActualRows,
    long ElapsedMs)
{
    public double RowsPerMs => ElapsedMs <= 0 ? ActualRows : (double)ActualRows / ElapsedMs;
    public double SelectivityPct => EstimatedRowsRead <= 0 ? 0d : (double)ActualRows / EstimatedRowsRead * 100d;
}

internal sealed record SqlIndexRecommendation(
    string Table,
    string SuggestedIndex,
    string Reason,
    int Confidence,
    long EstimatedRowsReadBefore,
    long EstimatedRowsReadAfter)
{
    public double EstimatedGainPct
        => EstimatedRowsReadBefore <= 0
            ? 0d
            : (double)(EstimatedRowsReadBefore - EstimatedRowsReadAfter) / EstimatedRowsReadBefore * 100d;
}

internal enum SqlPlanWarningSeverity
{
    Info,
    Warning,
    High
}

internal enum SqlPlanSeverityHintContext
{
    Dev,
    Ci,
    Prod
}

internal sealed record SqlPlanWarning(
    string Code,
    string Message,
    string Reason,
    string SuggestedAction,
    SqlPlanWarningSeverity Severity,
    string? MetricName = null,
    string? ObservedValue = null,
    string? Threshold = null);

internal static class SqlExecutionPlanFormatter
{
    public static string FormatSelect(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations = null,
        IReadOnlyList<SqlPlanWarning>? planWarnings = null,
        SqlPlanRuntimeMetrics? previousMetrics = null,
        IReadOnlyList<SqlPlanWarning>? previousWarnings = null,
        SqlPlanSeverityHintContext severityHintContext = SqlPlanSeverityHintContext.Dev)
    {
        var sb = new StringBuilder();
        sb.AppendLine(SqlExecutionPlanMessages.ExecutionPlanTitle());
        sb.AppendLine($"- {SqlExecutionPlanMessages.QueryTypeLabel()}: SELECT");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedCostLabel()}: {EstimateSelectCost(query)}");

        if (query.Ctes.Count > 0)
        {
            sb.AppendLine($"- {SqlExecutionPlanMessages.CtesLabel()}: {query.Ctes.Count}");
            foreach (var cte in query.Ctes)
                sb.AppendLine($"  - {SqlExecutionPlanMessages.CteMaterializeLabel()}: {cte.Name}");
        }

        sb.AppendLine($"- FROM: {FormatSource(query.Table)}");

        if (query.Joins.Count > 0)
        {
            foreach (var join in query.Joins)
            {
                var on = SqlExprPrinter.Print(join.On);
                sb.AppendLine($"- JOIN: {join.Type.ToString().ToUpperInvariant()} {FormatSource(join.Table)} ON {on}");
            }
        }

        if (query.Where is not null)
            sb.AppendLine($"- WHERE: {SqlExprPrinter.Print(query.Where)}");

        if (query.GroupBy.Count > 0)
            sb.AppendLine($"- GROUP BY: {string.Join(", ", query.GroupBy)}");

        if (query.Having is not null)
            sb.AppendLine($"- HAVING: {SqlExprPrinter.Print(query.Having)}");

        sb.AppendLine($"- {SqlExecutionPlanMessages.ProjectionLabel()}: {query.SelectItems.Count} item(s)");

        if (query.Distinct)
            sb.AppendLine("- DISTINCT: true");

        if (query.OrderBy.Count > 0)
        {
            var order = string.Join(", ", query.OrderBy.Select(o => $"{o.Raw} {(o.Desc ? "DESC" : "ASC")}"));
            sb.AppendLine($"- ORDER BY: {order}");
        }

        if (query.RowLimit is not null)
            sb.AppendLine($"- LIMIT/TOP/FETCH: {FormatLimit(query.RowLimit)}");

        sb.AppendLine($"- {SqlExecutionPlanMessages.InputTablesLabel()}: {metrics.InputTables}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedRowsReadLabel()}: {metrics.EstimatedRowsRead}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.ActualRowsLabel()}: {metrics.ActualRows}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.SelectivityPctLabel()}: {metrics.SelectivityPct:F2}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.RowsPerMsLabel()}: {metrics.RowsPerMs:F2}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.ElapsedMsLabel()}: {metrics.ElapsedMs}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanMetadataVersionLabel()}: 1");
        AppendPlanCorrelationId(sb);

        AppendPlanFlags(sb, indexRecommendations, planWarnings);
        AppendPlanPerformanceBand(sb, metrics);
        AppendIndexRecommendations(sb, indexRecommendations);
        AppendIndexRecommendationSummary(sb, indexRecommendations);
        AppendIndexPrimaryRecommendation(sb, indexRecommendations);
        AppendIndexRecommendationEvidence(sb, indexRecommendations);
        AppendPlanWarnings(sb, planWarnings);
        AppendPlanRiskScore(sb, planWarnings);
        AppendPlanQualityGrade(sb, metrics, planWarnings);
        AppendPlanWarningSummary(sb, planWarnings);
        AppendPlanWarningCounts(sb, planWarnings);
        AppendPlanNoiseScore(sb, planWarnings);
        AppendPlanTopActions(sb, indexRecommendations, planWarnings);
        AppendPrimaryWarning(sb, planWarnings);
        AppendPlanPrimaryCauseGroup(sb, planWarnings);
        AppendPlanDelta(sb, metrics, planWarnings, previousMetrics, previousWarnings);
        AppendPlanSeverityHint(sb, planWarnings, severityHintContext);

        return sb.ToString().TrimEnd();
    }





    private static void AppendPlanCorrelationId(StringBuilder sb)
    {
        var correlationId = Guid.NewGuid().ToString("N");
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanCorrelationIdLabel()}: {correlationId}");
    }

    private static void AppendPlanPerformanceBand(
        StringBuilder sb,
        SqlPlanRuntimeMetrics metrics)
    {
        var band = GetPlanPerformanceBand(metrics.ElapsedMs);

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanPerformanceBandLabel()}: {band}");
    }

    private static void AppendPlanFlags(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        var hasWarnings = planWarnings is { Count: > 0 } ? "true" : "false";
        var hasIndexRecommendations = indexRecommendations is { Count: > 0 } ? "true" : "false";
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanFlagsLabel()}: hasWarnings:{hasWarnings};hasIndexRecommendations:{hasIndexRecommendations}");
    }

    private static void AppendIndexRecommendationSummary(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations)
    {
        if (indexRecommendations is null || indexRecommendations.Count == 0)
            return;

        var avgConfidence = indexRecommendations.Average(static r => r.Confidence);
        var maxGain = indexRecommendations.Max(static r => r.EstimatedGainPct);
        sb.AppendLine($"- {SqlExecutionPlanMessages.IndexRecommendationSummaryLabel()}: count:{indexRecommendations.Count};avgConfidence:{avgConfidence:F2};maxGainPct:{maxGain:F2}");
    }


    private static void AppendIndexPrimaryRecommendation(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations)
    {
        if (indexRecommendations is null || indexRecommendations.Count == 0)
            return;

        var primary = indexRecommendations
            .OrderByDescending(static r => r.Confidence)
            .ThenByDescending(static r => r.EstimatedGainPct)
            .ThenBy(static r => r.Table, StringComparer.Ordinal)
            .First();

        sb.AppendLine($"- {SqlExecutionPlanMessages.IndexPrimaryRecommendationLabel()}: table:{primary.Table};confidence:{primary.Confidence};gainPct:{primary.EstimatedGainPct:F2}");
    }

    private static void AppendIndexRecommendationEvidence(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations)
    {
        if (indexRecommendations is null || indexRecommendations.Count == 0)
            return;

        var evidence = string.Join("|", indexRecommendations
            .OrderByDescending(static r => r.Confidence)
            .ThenByDescending(static r => r.EstimatedGainPct)
            .ThenBy(static r => r.Table, StringComparer.Ordinal)
            .Select(static r => BuildIndexRecommendationEvidenceItem(r)));

        sb.AppendLine($"- {SqlExecutionPlanMessages.IndexRecommendationEvidenceLabel()}: {evidence}");
    }

    private static string BuildIndexRecommendationEvidenceItem(SqlIndexRecommendation recommendation)
    {
        var indexCols = ExtractSuggestedIndexColumns(recommendation.SuggestedIndex);
        return $"table:{recommendation.Table};indexCols:{indexCols};confidence:{recommendation.Confidence};gainPct:{recommendation.EstimatedGainPct:F2}";
    }

    private static string ExtractSuggestedIndexColumns(string suggestedIndex)
    {
        if (string.IsNullOrWhiteSpace(suggestedIndex))
            return "<unknown>";

        var openParen = suggestedIndex.IndexOf('(');
        if (openParen < 0)
            return "<unknown>";

        var closeParen = suggestedIndex.IndexOf(')', openParen + 1);
        if (closeParen <= openParen + 1)
            return "<unknown>";

        return suggestedIndex[(openParen + 1)..closeParen].Replace(" ", string.Empty);
    }

    private static void AppendPlanWarnings(
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


    private static void AppendPlanRiskScore(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var score = CalculatePlanRiskScore(planWarnings);
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanRiskScoreLabel()}: {score}");
    }

    private static void AppendPlanQualityGrade(
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


    private static void AppendPlanWarningSummary(
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



    private static void AppendPlanWarningCounts(
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



    private static void AppendPlanNoiseScore(
        StringBuilder sb,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        if (planWarnings is null || planWarnings.Count == 0)
            return;

        var noiseScore = CalculatePlanNoiseScore(planWarnings);
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanNoiseScoreLabel()}: {noiseScore}");
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

    private static void AppendPlanTopActions(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        var actions = BuildTopActions(indexRecommendations, planWarnings);
        if (actions.Count == 0)
            return;

        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanTopActionsLabel()}: {string.Join(";", actions)}");
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
        {
            actions.Add("IDX:CreateSuggestedIndex");
        }

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

    private static void AppendPrimaryWarning(
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


    private static void AppendPlanPrimaryCauseGroup(
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

    private static void AppendPlanDelta(
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

    private static void AppendPlanSeverityHint(
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

    public static string FormatSelectJson(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations = null,
        IReadOnlyList<SqlPlanWarning>? planWarnings = null,
        SqlPlanRuntimeMetrics? previousMetrics = null,
        IReadOnlyList<SqlPlanWarning>? previousWarnings = null,
        SqlPlanSeverityHintContext severityHintContext = SqlPlanSeverityHintContext.Dev)
    {
        var hasWarnings = planWarnings is { Count: > 0 };
        var hasIndexRecommendations = indexRecommendations is { Count: > 0 };
        var performanceBand = GetPlanPerformanceBand(metrics.ElapsedMs);

        var payload = new Dictionary<string, object?>
        {
            ["queryType"] = "SELECT",
            ["estimatedCost"] = EstimateSelectCost(query),
            ["planMetadataVersion"] = 1,
            ["planCorrelationId"] = Guid.NewGuid().ToString("N"),
            ["planFlags"] = $"hasWarnings:{(hasWarnings ? "true" : "false")};hasIndexRecommendations:{(hasIndexRecommendations ? "true" : "false")}",
            ["planPerformanceBand"] = performanceBand
        };

        if (hasWarnings)
        {
            var riskScore = CalculatePlanRiskScore(planWarnings!);
            var warningSummary = string.Join(";", planWarnings!
                .OrderByDescending(static w => GetSeverityWeight(w.Severity))
                .ThenBy(static w => w.Code, StringComparer.Ordinal)
                .Select(static w => $"{w.Code}:{w.Severity}"));

            var high = planWarnings!.Count(static w => w.Severity == SqlPlanWarningSeverity.High);
            var warning = planWarnings!.Count(static w => w.Severity == SqlPlanWarningSeverity.Warning);
            var info = planWarnings!.Count(static w => w.Severity == SqlPlanWarningSeverity.Info);

            var primary = planWarnings!
                .OrderByDescending(static w => GetSeverityWeight(w.Severity))
                .ThenBy(static w => w.Code, StringComparer.Ordinal)
                .First();

            payload["planRiskScore"] = riskScore;
            payload["planQualityGrade"] = CalculatePlanQualityGrade(riskScore, performanceBand);
            payload["planWarningSummary"] = warningSummary;
            payload["planWarningCounts"] = $"high:{high};warning:{warning};info:{info}";
            payload["planNoiseScore"] = CalculatePlanNoiseScore(planWarnings!);
            payload["planTopActions"] = string.Join(";", BuildTopActions(indexRecommendations, planWarnings));
            payload["planPrimaryWarning"] = $"{primary.Code}:{primary.Severity}";
            payload["planPrimaryCauseGroup"] = MapPrimaryCauseGroup(primary.Code);

            var hintScore = high * 3 + warning * 2 + info;
            var threshold = severityHintContext switch
            {
                SqlPlanSeverityHintContext.Dev => 3,
                SqlPlanSeverityHintContext.Ci => 2,
                SqlPlanSeverityHintContext.Prod => 1,
                _ => 3
            };
            var level = hintScore >= threshold * 3 ? "High" : hintScore >= threshold * 2 ? "Warning" : "Info";
            payload["planSeverityHint"] = $"context:{severityHintContext.ToString().ToLowerInvariant()};level:{level}";
        }

        if (previousMetrics is not null)
        {
            var currentRisk = hasWarnings ? (int)payload["planRiskScore"]! : 0;
            var previousRisk = previousWarnings is { Count: > 0 } ? CalculatePlanRiskScore(previousWarnings) : 0;
            var riskDelta = currentRisk - previousRisk;
            var elapsedDelta = metrics.ElapsedMs - previousMetrics.ElapsedMs;
            var riskPrefix = riskDelta >= 0 ? "+" : string.Empty;
            var elapsedPrefix = elapsedDelta >= 0 ? "+" : string.Empty;
            payload["planDelta"] = $"riskDelta:{riskPrefix}{riskDelta};elapsedMsDelta:{elapsedPrefix}{elapsedDelta}";
        }

        if (hasIndexRecommendations)
        {
            var avgConfidence = indexRecommendations!.Average(static r => r.Confidence);
            var maxGain = indexRecommendations!.Max(static r => r.EstimatedGainPct);
            payload["indexRecommendationSummary"] = $"count:{indexRecommendations!.Count};avgConfidence:{avgConfidence:F2};maxGainPct:{maxGain:F2}";

            var primary = indexRecommendations
                .OrderByDescending(static r => r.Confidence)
                .ThenByDescending(static r => r.EstimatedGainPct)
                .ThenBy(static r => r.Table, StringComparer.Ordinal)
                .First();
            payload["indexPrimaryRecommendation"] = $"table:{primary.Table};confidence:{primary.Confidence};gainPct:{primary.EstimatedGainPct:F2}";
            payload["indexRecommendationEvidence"] = string.Join("|", indexRecommendations
                .OrderByDescending(static r => r.Confidence)
                .ThenByDescending(static r => r.EstimatedGainPct)
                .ThenBy(static r => r.Table, StringComparer.Ordinal)
                .Select(static r => BuildIndexRecommendationEvidenceItem(r)));
        }

        return JsonSerializer.Serialize(payload);
    }


    public static string FormatUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit,
        SqlPlanRuntimeMetrics metrics)
    {
        var sb = new StringBuilder();
        sb.AppendLine(SqlExecutionPlanMessages.ExecutionPlanTitle());
        sb.AppendLine($"- {SqlExecutionPlanMessages.QueryTypeLabel()}: UNION");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedCostLabel()}: {EstimateUnionCost(parts, allFlags, orderBy, rowLimit)}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.PartsLabel()}: {parts.Count}");

        for (int i = 0; i < parts.Count; i++)
            sb.AppendLine($"  - {SqlExecutionPlanMessages.PartLabel()}[{i + 1}]: SELECT from {FormatSource(parts[i].Table)}");

        for (int i = 0; i < allFlags.Count; i++)
            sb.AppendLine($"  - {SqlExecutionPlanMessages.CombineLabel()}[{i + 1}]: {(allFlags[i] ? "UNION ALL" : "UNION DISTINCT")}");

        if ((orderBy?.Count ?? 0) > 0)
        {
            var order = string.Join(", ", orderBy!.Select(o => $"{o.Raw} {(o.Desc ? "DESC" : "ASC")}"));
            sb.AppendLine($"- ORDER BY: {order}");
        }

        if (rowLimit is not null)
            sb.AppendLine($"- LIMIT/TOP/FETCH: {FormatLimit(rowLimit)}");

        sb.AppendLine($"- {SqlExecutionPlanMessages.InputTablesLabel()}: {metrics.InputTables}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedRowsReadLabel()}: {metrics.EstimatedRowsRead}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.ActualRowsLabel()}: {metrics.ActualRows}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.SelectivityPctLabel()}: {metrics.SelectivityPct:F2}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.RowsPerMsLabel()}: {metrics.RowsPerMs:F2}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.ElapsedMsLabel()}: {metrics.ElapsedMs}");

        return sb.ToString().TrimEnd();
    }

    private static void AppendIndexRecommendations(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations)
    {
        if (indexRecommendations is null || indexRecommendations.Count == 0)
            return;

        sb.AppendLine($"- {SqlExecutionPlanMessages.IndexRecommendationsLabel()}:");
        foreach (var recommendation in indexRecommendations)
        {
            sb.AppendLine($"  - {SqlExecutionPlanMessages.TableLabel()}: {recommendation.Table}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.SuggestedIndexLabel()}: {recommendation.SuggestedIndex}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.ReasonLabel()}: {recommendation.Reason}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.ConfidenceLabel()}: {recommendation.Confidence}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.EstimatedRowsReadBeforeLabel()}: {recommendation.EstimatedRowsReadBefore}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.EstimatedRowsReadAfterLabel()}: {recommendation.EstimatedRowsReadAfter}");
            sb.AppendLine($"    {SqlExecutionPlanMessages.EstimatedGainPctLabel()}: {recommendation.EstimatedGainPct:F2}");
        }
    }

    private static string FormatSource(SqlTableSource? source)
    {
        if (source is null)
            return "<none>";

        if (source.Derived is not null)
            return $"subquery AS {source.Alias ?? "<derived>"}";

        if (source.DerivedUnion is not null)
            return $"union-subquery AS {source.Alias ?? "<derived_union>"}";

        return source.Name ?? "<unknown_table>";
    }

    private static string FormatLimit(SqlRowLimit rowLimit)
        => rowLimit switch
        {
            SqlLimitOffset l => l.Offset.HasValue ? $"LIMIT {l.Count} OFFSET {l.Offset.Value}" : $"LIMIT {l.Count}",
            SqlTop t => $"TOP {t.Count}",
            SqlFetch f => f.Offset.HasValue ? $"FETCH {f.Count} OFFSET {f.Offset.Value}" : $"FETCH {f.Count}",
            _ => rowLimit.ToString() ?? "<limit>"
        };

    private static int EstimateSelectCost(SqlSelectQuery query)
    {
        var cost = 10;
        cost += EstimateCteCost(query.Ctes);
        cost += EstimateJoinGraphCost(query.Joins);
        cost += EstimateSourceCost(query.Table);
        cost += query.Joins.Sum(static j => EstimateSourceCost(j.Table));
        if (query.Where is not null) cost += 8 + EstimatePredicateComplexityCost(query.Where);
        cost += EstimateAggregationCost(query.GroupBy, query.Having);
        cost += EstimateSortAndDedupCost(query.OrderBy, query.Distinct, query.RowLimit);
        cost += EstimateDistinctGroupByOrderByCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.RowLimit);
        cost += EstimateProjectionCost(query.SelectItems);
        cost -= EstimateRowLimitRelief(query.RowLimit);
        return Math.Max(1, cost);
    }

    /// <summary>
    /// EN: Estimates join-graph cost combining base join count, join types, predicate complexity and multi-join fan-out risk.
    /// PT: Estima custo do grafo de joins combinando quantidade base de joins, tipos de join, complexidade de predicado e risco de fan-out em múltiplos joins.
    /// </summary>
    private static int EstimateJoinGraphCost(IReadOnlyList<SqlJoin> joins)
    {
        var cost = joins.Count * 25;
        cost += joins.Sum(static j => EstimateJoinTypeCost(j.Type));
        cost += joins.Sum(static j => EstimateJoinPredicateCost(j.On));

        if (joins.Count > 1)
            cost += (joins.Count - 1) * 3;

        var expansionRiskJoins = joins.Count(static j => j.Type is SqlJoinType.Left or SqlJoinType.Right or SqlJoinType.Cross);
        if (expansionRiskJoins > 1)
            cost += (expansionRiskJoins - 1) * 4;

        return cost;
    }

    /// <summary>
    /// EN: Estimates ON-predicate complexity for joins with a small baseline evaluation overhead.
    /// PT: Estima a complexidade do predicado ON em joins com pequena sobrecarga base de avaliação.
    /// </summary>
    private static int EstimateJoinPredicateCost(SqlExpr on)
        => 1 + EstimatePredicateComplexityCost(on);

    /// <summary>
    /// EN: Estimates CTE-related overhead, combining declaration count and nested CTE query complexity.
    /// PT: Estima overhead relacionado a CTE, combinando quantidade de declarações e complexidade das consultas CTE aninhadas.
    /// </summary>
    private static int EstimateCteCost(IReadOnlyList<SqlCte> ctes)
    {
        var cost = ctes.Count * 5;
        cost += ctes.Sum(static cte => EstimateCteQueryCost(cte.Query));
        return cost;
    }

    /// <summary>
    /// EN: Estimates lightweight nested CTE query complexity to avoid over-amplifying recursive cost loops.
    /// PT: Estima complexidade leve de consulta CTE aninhada para evitar sobre-amplificação em loops recursivos de custo.
    /// </summary>
    private static int EstimateCteQueryCost(SqlSelectQuery query)
    {
        var cost = 2;
        cost += query.Joins.Count * 3;
        if (query.Where is not null)
            cost += 2 + EstimatePredicateComplexityCost(query.Where);

        cost += EstimateAggregationCost(query.GroupBy, query.Having);
        cost += EstimateSortAndDedupCost(query.OrderBy, query.Distinct, query.RowLimit);
        cost += EstimateProjectionCost(query.SelectItems);
        return Math.Max(0, cost);
    }

    /// <summary>
    /// EN: Estimates extra cost by JOIN type to represent broader row-preservation/expansion risk.
    /// PT: Estima custo extra por tipo de JOIN para representar risco maior de preservação/expansão de linhas.
    /// </summary>
    private static int EstimateJoinTypeCost(SqlJoinType joinType)
        => joinType switch
        {
            SqlJoinType.Inner => 0,
            SqlJoinType.Left => 4,
            SqlJoinType.Right => 4,
            SqlJoinType.Cross => 10,
            _ => 0
        };

    /// <summary>
    /// EN: Estimates predicate complexity cost from SQL expression tree shape.
    /// PT: Estima custo de complexidade de predicado a partir do formato da árvore de expressão SQL.
    /// </summary>
    private static int EstimatePredicateComplexityCost(SqlExpr expr)
        => expr switch
        {
            BinaryExpr b when b.Op is SqlBinaryOp.And or SqlBinaryOp.Or
                => 2 + EstimateLogicalPredicateDepthPenalty(b) + EstimatePredicateComplexityCost(b.Left) + EstimatePredicateComplexityCost(b.Right),
            BinaryExpr b => 1 + EstimatePredicateComplexityCost(b.Left) + EstimatePredicateComplexityCost(b.Right),
            LikeExpr l => 2 + EstimatePredicateComplexityCost(l.Left) + EstimatePredicateComplexityCost(l.Pattern),
            BetweenExpr b => 2 + EstimatePredicateComplexityCost(b.Expr) + EstimatePredicateComplexityCost(b.Low) + EstimatePredicateComplexityCost(b.High),
            InExpr i => 2 + EstimatePredicateComplexityCost(i.Left) + i.Items.Sum(EstimatePredicateComplexityCost) + i.Items.Count,
            ExistsExpr e => EstimateSubqueryPredicateCost(e.Subquery),
            SubqueryExpr s => EstimateSubqueryPredicateCost(s),
            FunctionCallExpr f => 1 + f.Args.Sum(EstimatePredicateComplexityCost),
            CallExpr c => 1 + c.Args.Sum(EstimatePredicateComplexityCost),
            CaseExpr c => EstimateCasePredicateComplexityCost(c),
            JsonAccessExpr j => 1 + EstimatePredicateComplexityCost(j.Target) + EstimatePredicateComplexityCost(j.Path),
            RowExpr r => 1 + r.Items.Sum(EstimatePredicateComplexityCost),
            UnaryExpr u => 1 + EstimatePredicateComplexityCost(u.Expr),
            IsNullExpr n => 1 + EstimatePredicateComplexityCost(n.Expr),
            _ => 0
        };

    /// <summary>
    /// EN: Estimates a small surcharge for deep AND/OR predicate nesting to reflect branching/evaluation complexity growth.
    /// PT: Estima uma pequena sobretaxa para aninhamento profundo de predicados AND/OR para refletir crescimento da complexidade de ramificação/avaliação.
    /// </summary>
    private static int EstimateLogicalPredicateDepthPenalty(BinaryExpr expr)
    {
        var depth = EstimateLogicalPredicateDepth(expr);
        return Math.Max(0, depth - 2);
    }

    /// <summary>
    /// EN: Computes the maximum logical (AND/OR) nesting depth inside a predicate subtree.
    /// PT: Calcula a profundidade máxima de aninhamento lógico (AND/OR) dentro de uma subárvore de predicado.
    /// </summary>
    private static int EstimateLogicalPredicateDepth(SqlExpr expr)
    {
        if (expr is not BinaryExpr b || b.Op is not (SqlBinaryOp.And or SqlBinaryOp.Or))
            return 0;

        return 1 + Math.Max(EstimateLogicalPredicateDepth(b.Left), EstimateLogicalPredicateDepth(b.Right));
    }

    /// <summary>
    /// EN: Estimates CASE-expression predicate complexity based on base expression, branches and optional ELSE expression.
    /// PT: Estima a complexidade de predicado com expressão CASE com base na expressão base, ramos e expressão ELSE opcional.
    /// </summary>
    private static int EstimateCasePredicateComplexityCost(CaseExpr c)
    {
        var cost = 2;
        if (c.BaseExpr is not null)
            cost += EstimatePredicateComplexityCost(c.BaseExpr);

        cost += c.Whens.Sum(static wt => EstimatePredicateComplexityCost(wt.When) + EstimatePredicateComplexityCost(wt.Then));

        if (c.ElseExpr is not null)
            cost += EstimatePredicateComplexityCost(c.ElseExpr);

        return cost;
    }

    /// <summary>
    /// EN: Estimates extra cost for GROUP BY/HAVING aggregation stages, including an extra coupling penalty when both appear.
    /// PT: Estima custo extra para estágios de agregação GROUP BY/HAVING, incluindo penalidade de acoplamento quando ambos aparecem.
    /// </summary>
    private static int EstimateAggregationCost(IReadOnlyList<string> groupBy, SqlExpr? having)
    {
        var cost = 0;
        if (groupBy.Count > 0)
        {
            cost += 20;
            cost += Math.Max(0, groupBy.Count - 1) * 2;
        }

        if (having is not null)
            cost += 10 + EstimatePredicateComplexityCost(having);

        if (groupBy.Count > 0 && having is not null)
            cost += 4;

        return cost;
    }

    /// <summary>
    /// EN: Estimates combined sort/distinct cost, including no-limit sort spill risk and ORDER BY + DISTINCT coupling.
    /// PT: Estima custo combinado de sort/distinct, incluindo risco de spill sem limite e acoplamento de ORDER BY + DISTINCT.
    /// </summary>
    private static int EstimateSortAndDedupCost(
        IReadOnlyList<SqlOrderByItem> orderBy,
        bool distinct,
        SqlRowLimit? rowLimit)
    {
        var cost = 0;
        if (orderBy.Count > 0)
        {
            cost += 15;
            cost += Math.Max(0, orderBy.Count - 1) * 2;
        }

        if (orderBy.Count > 0 && rowLimit is null)
            cost += 6;

        if (distinct)
            cost += 10;

        if (distinct && orderBy.Count > 0 && rowLimit is null)
            cost += 5;

        return cost;
    }

    /// <summary>
    /// EN: Estimates coupling cost for DISTINCT + GROUP BY + ORDER BY mixes because dedup, aggregation and sorting stages contend for the same row stream.
    /// PT: Estima custo de acoplamento para combinações DISTINCT + GROUP BY + ORDER BY porque estágios de deduplicação, agregação e ordenação disputam o mesmo fluxo de linhas.
    /// </summary>
    private static int EstimateDistinctGroupByOrderByCouplingCost(
        bool distinct,
        IReadOnlyList<string> groupBy,
        IReadOnlyList<SqlOrderByItem> orderBy,
        SqlRowLimit? rowLimit)
    {
        if (!distinct || groupBy.Count == 0 || orderBy.Count == 0)
            return 0;

        var cost = 8;
        cost += Math.Min(4, Math.Max(0, groupBy.Count - 1));
        cost += Math.Min(2, Math.Max(0, orderBy.Count - 1));

        if (rowLimit is null)
            cost += 2;

        return cost;
    }

    /// <summary>
    /// EN: Estimates subquery predicate overhead with a baseline nesting surcharge plus nested SELECT cost.
    /// PT: Estima overhead de predicado com subconsulta com sobretaxa base de aninhamento e custo do SELECT aninhado.
    /// </summary>
    private static int EstimateSubqueryPredicateCost(SubqueryExpr subquery)
        => 4 + EstimateSelectCost(subquery.Parsed);

    /// <summary>
    /// EN: Estimates extra cost contributed by source shape (base table, derived query, or union subquery).
    /// PT: Estima custo extra contribuído pelo formato da fonte (tabela base, consulta derivada ou subconsulta union).
    /// </summary>
    private static int EstimateSourceCost(SqlTableSource? source)
    {
        if (source is null)
            return 0;

        if (source.Derived is not null)
            return 8 + EstimateSelectCost(source.Derived);

        if (source.DerivedUnion is not null)
            return 12 + EstimateUnionCost(source.DerivedUnion.Parts, source.DerivedUnion.AllFlags, source.DerivedUnion.OrderBy, source.DerivedUnion.RowLimit);

        return 0;
    }

    /// <summary>
    /// EN: Estimates projection-related cost using projection width and lightweight SQL-shape tokens from SELECT items.
    /// PT: Estima custo da projeção usando largura da projeção e tokens leves de formato SQL nos itens do SELECT.
    /// </summary>
    private static int EstimateProjectionCost(IReadOnlyList<SqlSelectItem> selectItems)
    {
        var cost = EstimateProjectionWidthCost(selectItems);
        cost += EstimateWildcardProjectionCost(selectItems);

        foreach (var item in selectItems)
        {
            var raw = item.Raw ?? string.Empty;
            if (raw.IndexOf(" OVER ", StringComparison.OrdinalIgnoreCase) >= 0)
                cost += 12;
            if (raw.IndexOf("CASE ", StringComparison.OrdinalIgnoreCase) >= 0)
                cost += 4;
            if (raw.IndexOf("SELECT ", StringComparison.OrdinalIgnoreCase) >= 0)
                cost += 10;
            cost += EstimateAggregateProjectionFunctionCost(raw);
        }

        return cost;
    }

    /// <summary>
    /// EN: Estimates projection cost for aggregate function usage with lightweight per-function weights (COUNT/SUM/AVG).
    /// PT: Estima custo de projeção para uso de funções agregadas com pesos leves por função (COUNT/SUM/AVG).
    /// </summary>
    private static int EstimateAggregateProjectionFunctionCost(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return 0;

        var cost = 0;
        cost += CountSqlFunctionCalls(raw, "COUNT") * 2;
        cost += CountSqlFunctionCalls(raw, "SUM") * 3;
        cost += CountSqlFunctionCalls(raw, "AVG") * 4;
        return cost;
    }

    /// <summary>
    /// EN: Counts function-call occurrences using case-insensitive token matching and an identifier-boundary guard.
    /// PT: Conta ocorrências de chamada de função usando correspondência de token case-insensitive e guarda de fronteira de identificador.
    /// </summary>
    private static int CountSqlFunctionCalls(string raw, string functionName)
    {
        var token = functionName + "(";
        var count = 0;
        var index = 0;

        while (true)
        {
            index = raw.IndexOf(token, index, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
                return count;

            if (index == 0 || !IsSqlIdentifierChar(raw[index - 1]))
                count++;

            index += token.Length;
        }
    }

    /// <summary>
    /// EN: Determines whether a character can be part of a SQL identifier for token-boundary checks.
    /// PT: Determina se um caractere pode compor um identificador SQL para verificações de fronteira de token.
    /// </summary>
    private static bool IsSqlIdentifierChar(char c)
        => char.IsLetterOrDigit(c) || c is '_' or '$';

    /// <summary>
    /// EN: Estimates projection-width overhead so broader SELECT lists carry additional per-item processing cost.
    /// PT: Estima overhead de largura da projeção para que listas SELECT maiores carreguem custo adicional por item.
    /// </summary>
    private static int EstimateProjectionWidthCost(IReadOnlyList<SqlSelectItem> selectItems)
    {
        if (selectItems.Count <= 1)
            return 0;

        return selectItems.Count - 1;
    }


    /// <summary>
    /// EN: Estimates wildcard projection overhead because '*' usually expands to broader unknown column sets.
    /// PT: Estima overhead de projeção curinga porque '*' normalmente expande para conjuntos de colunas mais amplos e desconhecidos.
    /// </summary>
    private static int EstimateWildcardProjectionCost(IReadOnlyList<SqlSelectItem> selectItems)
        => selectItems.Count(static item => string.Equals(item.Raw?.Trim(), "*", StringComparison.Ordinal)) * 6;

    /// <summary>
    /// EN: Estimates cost relief from row-limit clauses, with stronger relief for tighter limits.
    /// PT: Estima alívio de custo de cláusulas de limite de linhas, com alívio maior para limites mais restritos.
    /// </summary>
    private static int EstimateRowLimitRelief(SqlRowLimit? rowLimit)
    {
        if (rowLimit is null)
            return 0;

        var (count, offset) = ExtractRowLimitCountAndOffset(rowLimit);

        var relief = count switch
        {
            <= 0 => 3,
            <= 10 => 7,
            <= 100 => 5,
            <= 1000 => 3,
            _ => 2
        };

        relief -= EstimateRowLimitOffsetPenalty(offset);
        return Math.Max(0, relief);
    }

    /// <summary>
    /// EN: Extracts logical row-limit count and offset from different SQL limit syntaxes.
    /// PT: Extrai contagem e offset lógicos de limite de linhas de diferentes sintaxes SQL de limite.
    /// </summary>
    private static (int Count, int Offset) ExtractRowLimitCountAndOffset(SqlRowLimit rowLimit)
        => rowLimit switch
        {
            SqlLimitOffset l => (l.Count, l.Offset ?? 0),
            SqlTop t => (t.Count, 0),
            SqlFetch f => (f.Count, f.Offset ?? 0),
            _ => (0, 0)
        };

    /// <summary>
    /// EN: Estimates penalty for high offsets because deep skips still require additional scan/sort work.
    /// PT: Estima penalidade para offsets altos porque saltos profundos ainda exigem trabalho adicional de scan/sort.
    /// </summary>
    private static int EstimateRowLimitOffsetPenalty(int offset)
        => offset switch
        {
            <= 0 => 0,
            <= 100 => 1,
            <= 1000 => 2,
            _ => 3
        };

    private static int EstimateUnionCost(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit)
    {
        var cost = parts.Sum(EstimateSelectCost) + 12;
        cost += allFlags.Count(flag => !flag) * 20;
        cost += EstimateSortAndDedupCost(orderBy ?? [], false, rowLimit);
        cost -= EstimateRowLimitRelief(rowLimit);
        return Math.Max(1, cost);
    }
}
