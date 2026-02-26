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
        cost += query.Ctes.Count * 5;
        cost += query.Joins.Count * 25;
        if (query.Where is not null) cost += 8;
        if (query.GroupBy.Count > 0) cost += 20;
        if (query.Having is not null) cost += 10;
        if (query.OrderBy.Count > 0) cost += 15;
        if (query.Distinct) cost += 10;
        if (query.RowLimit is not null) cost -= 3;
        return Math.Max(1, cost);
    }

    private static int EstimateUnionCost(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit)
    {
        var cost = parts.Sum(EstimateSelectCost) + 12;
        cost += allFlags.Count(flag => !flag) * 20;
        if ((orderBy?.Count ?? 0) > 0) cost += 15;
        if (rowLimit is not null) cost -= 2;
        return Math.Max(1, cost);
    }
}
