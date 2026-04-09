namespace DbSqlLikeMem;

internal static class SqlExecutionPlanFormatterMetadataHelper
{
    internal static void AppendMetricsBlock(
        StringBuilder sb,
        SqlPlanRuntimeMetrics metrics,
        SqlPlanMockRuntimeContext? runtimeContext)
    {
        sb.AppendLine($"- {SqlExecutionPlanMessages.InputTablesLabel()}: {metrics.InputTables}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedRowsReadLabel()}: {metrics.EstimatedRowsRead}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.ActualRowsLabel()}: {metrics.ActualRows}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.SelectivityPctLabel()}: {metrics.SelectivityPct:F2}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.RowsPerMsLabel()}: {metrics.RowsPerMs:F2}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.ElapsedMsLabel()}: {metrics.ElapsedMs}");
        AppendPerformanceDisclaimer(sb);
        AppendMockRuntimeContext(sb, runtimeContext);
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanMetadataVersionLabel()}: 1");
        AppendPlanCorrelationId(sb);
        AppendPlanFlags(sb, null, null);
        AppendPlanPerformanceBand(sb, metrics);
    }

    internal static void AppendPlanCorrelationId(StringBuilder sb)
    {
        var correlationId = Guid.NewGuid().ToString("N");
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanCorrelationIdLabel()}: {correlationId}");
    }

    internal static void AppendPerformanceDisclaimer(StringBuilder sb)
        => sb.AppendLine($"- {SqlExecutionPlanMessages.PerformanceDisclaimerLabel()}: {SqlExecutionPlanMessages.PerformanceDisclaimerMessage()}");

    internal static void AppendMockRuntimeContext(StringBuilder sb, SqlPlanMockRuntimeContext? runtimeContext)
    {
        if (runtimeContext is null)
            return;

        sb.AppendLine($"- MockRuntimeContext: metricsAreRelative:true;simulatedLatencyMs:{runtimeContext.SimulatedLatencyMs};dropProbability:{runtimeContext.DropProbability:F4};threadSafe:{runtimeContext.ThreadSafe.ToString().ToLowerInvariant()}");
        if (runtimeContext.SimulatedLatencyMs > 0 || runtimeContext.DropProbability > 0d)
            sb.AppendLine("- MockRuntimePerturbationActive: true");
    }

    internal static void AppendPlanPerformanceBand(
        StringBuilder sb,
        SqlPlanRuntimeMetrics metrics)
    {
        var band = GetPlanPerformanceBand(metrics.ElapsedMs);
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanPerformanceBandLabel()}: {band}");
    }

    internal static void AppendPlanFlags(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations,
        IReadOnlyList<SqlPlanWarning>? planWarnings)
    {
        var hasWarnings = planWarnings is { Count: > 0 } ? "true" : "false";
        var hasIndexRecommendations = indexRecommendations is { Count: > 0 } ? "true" : "false";
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanFlagsLabel()}: hasWarnings:{hasWarnings};hasIndexRecommendations:{hasIndexRecommendations}");
    }

    internal static void AppendIndexRecommendations(
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

    internal static void AppendIndexRecommendationSummary(
        StringBuilder sb,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations)
    {
        if (indexRecommendations is null || indexRecommendations.Count == 0)
            return;

        var avgConfidence = indexRecommendations.Average(static r => r.Confidence);
        var maxGain = indexRecommendations.Max(static r => r.EstimatedGainPct);
        sb.AppendLine($"- {SqlExecutionPlanMessages.IndexRecommendationSummaryLabel()}: count:{indexRecommendations.Count};avgConfidence:{avgConfidence:F2};maxGainPct:{maxGain:F2}");
    }

    internal static void AppendIndexPrimaryRecommendation(
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

    internal static void AppendIndexRecommendationEvidence(
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

    internal static string BuildIndexRecommendationEvidenceItem(SqlIndexRecommendation recommendation)
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
