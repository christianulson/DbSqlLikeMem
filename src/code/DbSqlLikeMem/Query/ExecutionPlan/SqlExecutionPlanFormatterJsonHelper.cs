namespace DbSqlLikeMem;

internal static class SqlExecutionPlanFormatterJsonHelper
{
    internal static string FormatSelectJson(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics,
        Func<SqlSelectQuery, int> estimateSelectCost,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations = null,
        IReadOnlyList<SqlPlanWarning>? planWarnings = null,
        SqlPlanRuntimeMetrics? previousMetrics = null,
        IReadOnlyList<SqlPlanWarning>? previousWarnings = null,
        SqlPlanSeverityHintContext severityHintContext = SqlPlanSeverityHintContext.Dev,
        SqlPlanMockRuntimeContext? runtimeContext = null)
    {
        var hasWarnings = planWarnings is { Count: > 0 };
        var hasIndexRecommendations = indexRecommendations is { Count: > 0 };
        var performanceBand = GetPlanPerformanceBand(metrics.ElapsedMs);

        var payload = new Dictionary<string, object?>
        {
            ["queryType"] = SqlConst.SELECT,
            ["estimatedCost"] = estimateSelectCost(query),
            ["performanceDisclaimer"] = SqlExecutionPlanMessages.PerformanceDisclaimerMessage(),
            ["planMetadataVersion"] = 1,
            ["planCorrelationId"] = Guid.NewGuid().ToString("N"),
            ["planFlags"] = $"hasWarnings:{(hasWarnings ? "true" : "false")};hasIndexRecommendations:{(hasIndexRecommendations ? "true" : "false")}",
            ["planPerformanceBand"] = performanceBand
        };

        if (runtimeContext is not null)
            AppendMockRuntimeContextJson(payload, runtimeContext);

        var currentRisk = 0;
        if (hasWarnings)
            currentRisk = AppendWarningSummaryJson(
                payload,
                planWarnings!,
                indexRecommendations,
                performanceBand,
                severityHintContext);

        if (previousMetrics is not null)
            AppendPreviousPlanDeltaJson(payload, metrics, previousMetrics, currentRisk, previousWarnings);

        if (hasIndexRecommendations)
            AppendIndexRecommendationJson(payload, indexRecommendations!);

        return JsonSerializer.Serialize(payload);
    }

    private static void AppendMockRuntimeContextJson(
        Dictionary<string, object?> payload,
        SqlPlanMockRuntimeContext runtimeContext)
    {
        payload["mockRuntimeContext"] = $"metricsAreRelative:true;simulatedLatencyMs:{runtimeContext.SimulatedLatencyMs};dropProbability:{runtimeContext.DropProbability:F4};threadSafe:{runtimeContext.ThreadSafe.ToString().ToLowerInvariant()}";
        payload["mockMetricsAreRelative"] = runtimeContext.MetricsAreRelative;
        payload["mockSimulatedLatencyMs"] = runtimeContext.SimulatedLatencyMs;
        payload["mockDropProbability"] = runtimeContext.DropProbability;
        payload["mockThreadSafe"] = runtimeContext.ThreadSafe;
        payload["mockRuntimePerturbationActive"] = runtimeContext.SimulatedLatencyMs > 0 || runtimeContext.DropProbability > 0d;
    }

    private static int AppendWarningSummaryJson(
        Dictionary<string, object?> payload,
        IReadOnlyList<SqlPlanWarning> planWarnings,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations,
        string performanceBand,
        SqlPlanSeverityHintContext severityHintContext)
    {
        var riskScore = CalculatePlanRiskScore(planWarnings);
        var warningSummary = string.Join(";", planWarnings
            .OrderByDescending(static w => GetSeverityWeight(w.Severity))
            .ThenBy(static w => w.Code, StringComparer.Ordinal)
            .Select(static w => $"{w.Code}:{w.Severity}"));
        var warningCounts = CountWarningsBySeverity(planWarnings);
        var primary = GetPrimaryWarning(planWarnings);

        payload["planRiskScore"] = riskScore;
        payload["planQualityGrade"] = CalculatePlanQualityGrade(riskScore, performanceBand);
        payload["planWarningSummary"] = warningSummary;
        payload["planWarningCounts"] = $"high:{warningCounts.High};warning:{warningCounts.Warning};info:{warningCounts.Info}";
        payload["planNoiseScore"] = CalculatePlanNoiseScore(planWarnings);
        payload["planTopActions"] = string.Join(";", BuildTopActions(indexRecommendations, planWarnings));
        payload["planPrimaryWarning"] = $"{primary.Code}:{primary.Severity}";
        payload["planPrimaryCauseGroup"] = MapPrimaryCauseGroup(primary.Code);
        payload["planSeverityHint"] = BuildSeverityHint(severityHintContext, warningCounts);

        return riskScore;
    }

    private static void AppendPreviousPlanDeltaJson(
        Dictionary<string, object?> payload,
        SqlPlanRuntimeMetrics metrics,
        SqlPlanRuntimeMetrics previousMetrics,
        int currentRisk,
        IReadOnlyList<SqlPlanWarning>? previousWarnings)
    {
        var previousRisk = previousWarnings is { Count: > 0 } ? CalculatePlanRiskScore(previousWarnings) : 0;
        var riskDelta = currentRisk - previousRisk;
        var elapsedDelta = metrics.ElapsedMs - previousMetrics.ElapsedMs;
        var riskPrefix = riskDelta >= 0 ? "+" : string.Empty;
        var elapsedPrefix = elapsedDelta >= 0 ? "+" : string.Empty;
        payload["planDelta"] = $"riskDelta:{riskPrefix}{riskDelta};elapsedMsDelta:{elapsedPrefix}{elapsedDelta}";
    }

    private static void AppendIndexRecommendationJson(
        Dictionary<string, object?> payload,
        IReadOnlyList<SqlIndexRecommendation> indexRecommendations)
    {
        var avgConfidence = indexRecommendations.Average(static r => r.Confidence);
        var maxGain = indexRecommendations.Max(static r => r.EstimatedGainPct);
        payload["indexRecommendationSummary"] = $"count:{indexRecommendations.Count};avgConfidence:{avgConfidence:F2};maxGainPct:{maxGain:F2}";

        var orderedRecommendations = indexRecommendations
            .OrderByDescending(static r => r.Confidence)
            .ThenByDescending(static r => r.EstimatedGainPct)
            .ThenBy(static r => r.Table, StringComparer.Ordinal)
            .ToArray();

        var primary = orderedRecommendations[0];
        payload["indexPrimaryRecommendation"] = $"table:{primary.Table};confidence:{primary.Confidence};gainPct:{primary.EstimatedGainPct:F2}";
        payload["indexRecommendationEvidence"] = string.Join("|", orderedRecommendations.Select(static r => BuildIndexRecommendationEvidenceItem(r)));
    }

    private static (int High, int Warning, int Info) CountWarningsBySeverity(IReadOnlyList<SqlPlanWarning> planWarnings)
        => (
            planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.High),
            planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.Warning),
            planWarnings.Count(static w => w.Severity == SqlPlanWarningSeverity.Info));

    private static SqlPlanWarning GetPrimaryWarning(IReadOnlyList<SqlPlanWarning> planWarnings)
        => planWarnings
            .OrderByDescending(static w => GetSeverityWeight(w.Severity))
            .ThenBy(static w => w.Code, StringComparer.Ordinal)
            .First();

    private static string BuildSeverityHint(
        SqlPlanSeverityHintContext severityHintContext,
        (int High, int Warning, int Info) warningCounts)
    {
        var hintScore = warningCounts.High * 3 + warningCounts.Warning * 2 + warningCounts.Info;
        var threshold = GetSeverityThreshold(severityHintContext);
        var level = GetSeverityLevel(hintScore, threshold);
        return $"context:{severityHintContext.ToString().ToLowerInvariant()};level:{level}";
    }

    private static int GetSeverityThreshold(SqlPlanSeverityHintContext severityHintContext)
        => severityHintContext switch
        {
            SqlPlanSeverityHintContext.Dev => 3,
            SqlPlanSeverityHintContext.Ci => 2,
            SqlPlanSeverityHintContext.Prod => 1,
            _ => 3
        };

    private static string GetSeverityLevel(int hintScore, int threshold)
    {
        if (hintScore >= threshold * 3)
            return "High";

        return hintScore >= threshold * 2 ? "Warning" : "Info";
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

    private static string GetPlanPerformanceBand(long elapsedMs)
        => elapsedMs switch
        {
            <= 5 => "Fast",
            <= 30 => "Moderate",
            _ => "Slow"
        };
}
