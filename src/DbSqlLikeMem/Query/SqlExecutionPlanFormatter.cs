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

internal sealed record SqlPlanMockRuntimeContext(
    int SimulatedLatencyMs,
    double DropProbability,
    bool ThreadSafe)
{
    public bool MetricsAreRelative => true;
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
    public static string FormatInsert(
        SqlInsertQuery query,
        SqlPlanRuntimeMetrics metrics,
        SqlPlanMockRuntimeContext? runtimeContext = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine(SqlExecutionPlanMessages.ExecutionPlanTitle());
        sb.AppendLine($"- {SqlExecutionPlanMessages.QueryTypeLabel()}: INSERT");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedCostLabel()}: {EstimateInsertCost(query)}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.TableLabel()}: {FormatSource(query.Table)}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.ProjectionLabel()}: {query.Columns.Count} item(s)");

        if (query.InsertSelect is not null)
        {
            sb.AppendLine($"- FROM: {FormatSource(query.InsertSelect.Table)}");
            foreach (var join in query.InsertSelect.Joins)
            {
                sb.AppendLine(FormatJoinLine(join));
            }

            if (query.InsertSelect.Where is not null)
                sb.AppendLine($"- WHERE: {SqlExprPrinter.Print(query.InsertSelect.Where)}");
        }
        else
        {
            sb.AppendLine($"- FROM: VALUES ({query.ValuesRaw.Count} row(s))");
        }

        if (query.HasOnDuplicateKeyUpdate)
            sb.AppendLine($"- SET: {query.OnDupAssigns.Count} item(s)");

        if (query.Returning.Count > 0)
            sb.AppendLine($"- RETURNING: {query.Returning.Count} item(s)");

        AppendMetricsBlock(sb, metrics, runtimeContext);
        return sb.ToString().TrimEnd();
    }

    public static string FormatUpdate(
        SqlUpdateQuery query,
        SqlPlanRuntimeMetrics metrics,
        SqlPlanMockRuntimeContext? runtimeContext = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine(SqlExecutionPlanMessages.ExecutionPlanTitle());
        sb.AppendLine($"- {SqlExecutionPlanMessages.QueryTypeLabel()}: UPDATE");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedCostLabel()}: {EstimateUpdateCost(query)}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.TableLabel()}: {FormatSource(query.Table)}");
        sb.AppendLine($"- SET: {query.Set.Count} item(s)");

        if (query.Where is not null)
            sb.AppendLine($"- WHERE: {SqlExprPrinter.Print(query.Where)}");
        else if (!string.IsNullOrWhiteSpace(query.WhereRaw))
            sb.AppendLine($"- WHERE: {query.WhereRaw}");

        if (query.UpdateFromSelect is not null)
            sb.AppendLine($"- FROM: {FormatSource(query.UpdateFromSelect.Table)}");

        if (query.Returning.Count > 0)
            sb.AppendLine($"- RETURNING: {query.Returning.Count} item(s)");

        AppendMetricsBlock(sb, metrics, runtimeContext);
        return sb.ToString().TrimEnd();
    }

    public static string FormatDelete(
        SqlDeleteQuery query,
        SqlPlanRuntimeMetrics metrics,
        SqlPlanMockRuntimeContext? runtimeContext = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine(SqlExecutionPlanMessages.ExecutionPlanTitle());
        sb.AppendLine($"- {SqlExecutionPlanMessages.QueryTypeLabel()}: DELETE");
        sb.AppendLine($"- {SqlExecutionPlanMessages.EstimatedCostLabel()}: {EstimateDeleteCost(query)}");
        sb.AppendLine($"- {SqlExecutionPlanMessages.TableLabel()}: {FormatSource(query.Table)}");

        if (query.Where is not null)
            sb.AppendLine($"- WHERE: {SqlExprPrinter.Print(query.Where)}");
        else if (!string.IsNullOrWhiteSpace(query.WhereRaw))
            sb.AppendLine($"- WHERE: {query.WhereRaw}");

        if (query.DeleteFromSelect is not null)
            sb.AppendLine($"- FROM: {FormatSource(query.DeleteFromSelect.Table)}");

        if (query.Returning.Count > 0)
            sb.AppendLine($"- RETURNING: {query.Returning.Count} item(s)");

        AppendMetricsBlock(sb, metrics, runtimeContext);
        return sb.ToString().TrimEnd();
    }

    public static string FormatSelect(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics,
        IReadOnlyList<SqlIndexRecommendation>? indexRecommendations = null,
        IReadOnlyList<SqlPlanWarning>? planWarnings = null,
        SqlPlanRuntimeMetrics? previousMetrics = null,
        IReadOnlyList<SqlPlanWarning>? previousWarnings = null,
        SqlPlanSeverityHintContext severityHintContext = SqlPlanSeverityHintContext.Dev,
        SqlPlanMockRuntimeContext? runtimeContext = null)
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
                sb.AppendLine(FormatJoinLine(join));
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

        if (query.ForJson is not null)
        {
            var options = new List<string> { query.ForJson.Mode.ToString().ToUpperInvariant() };
            if (query.ForJson.RootName is not null)
                options.Add($"ROOT('{query.ForJson.RootName}')");
            if (query.ForJson.IncludeNullValues)
                options.Add("INCLUDE_NULL_VALUES");
            if (query.ForJson.WithoutArrayWrapper)
                options.Add("WITHOUT_ARRAY_WRAPPER");
            sb.AppendLine($"- FOR JSON: {string.Join(", ", options)}");
        }

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

    private static void AppendMetricsBlock(
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





    private static void AppendPlanCorrelationId(StringBuilder sb)
    {
        var correlationId = Guid.NewGuid().ToString("N");
        sb.AppendLine($"- {SqlExecutionPlanMessages.PlanCorrelationIdLabel()}: {correlationId}");
    }

    private static void AppendPerformanceDisclaimer(StringBuilder sb)
        => sb.AppendLine($"- {SqlExecutionPlanMessages.PerformanceDisclaimerLabel()}: {SqlExecutionPlanMessages.PerformanceDisclaimerMessage()}");

    private static void AppendMockRuntimeContext(StringBuilder sb, SqlPlanMockRuntimeContext? runtimeContext)
    {
        if (runtimeContext is null)
            return;

        sb.AppendLine($"- MockRuntimeContext: metricsAreRelative:true;simulatedLatencyMs:{runtimeContext.SimulatedLatencyMs};dropProbability:{runtimeContext.DropProbability:F4};threadSafe:{runtimeContext.ThreadSafe.ToString().ToLowerInvariant()}");
        if (runtimeContext.SimulatedLatencyMs > 0 || runtimeContext.DropProbability > 0d)
            sb.AppendLine("- MockRuntimePerturbationActive: true");
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
        SqlPlanSeverityHintContext severityHintContext = SqlPlanSeverityHintContext.Dev,
        SqlPlanMockRuntimeContext? runtimeContext = null)
    {
        var hasWarnings = planWarnings is { Count: > 0 };
        var hasIndexRecommendations = indexRecommendations is { Count: > 0 };
        var performanceBand = GetPlanPerformanceBand(metrics.ElapsedMs);

        var payload = new Dictionary<string, object?>
        {
            ["queryType"] = "SELECT",
            ["estimatedCost"] = EstimateSelectCost(query),
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


    public static string FormatUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit,
        SqlPlanRuntimeMetrics metrics,
        SqlPlanMockRuntimeContext? runtimeContext = null)
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
        AppendPerformanceDisclaimer(sb);
        AppendMockRuntimeContext(sb, runtimeContext);

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

        if (source.TableFunction is not null)
        {
            var functionName = FormatQualifiedFunctionSource(source);
            var alias = source.Alias ?? source.TableFunction.Name;
            return alias.Equals(source.TableFunction.Name, StringComparison.OrdinalIgnoreCase)
                ? functionName
                : $"{functionName} AS {alias}";
        }

        return FormatQualifiedTableName(source);
    }

    private static string FormatQualifiedFunctionSource(SqlTableSource source)
    {
        var functionName = source.DbName is null
            ? source.TableFunction?.Name ?? "<unknown_function>"
            : $"{source.DbName}.{source.TableFunction?.Name ?? "<unknown_function>"}";

        if (source.TableFunction?.Name.Equals("STRING_SPLIT", StringComparison.OrdinalIgnoreCase) == true
            && source.TableFunction.Args.Count == 3)
        {
            return $"{functionName}(..., ..., enable_ordinal)";
        }

        if (source.TableFunction?.Name.Equals("OPENJSON", StringComparison.OrdinalIgnoreCase) == true
            && source.TableFunction.Args.Count == 2)
        {
            var pathShape = TryFormatOpenJsonPathShape(source.TableFunction.Args[1]);
            return source.OpenJsonWithClause is null
                ? $"{functionName}(..., {pathShape})"
                : $"{functionName}(..., {pathShape}) WITH (...)";
        }

        return source.OpenJsonWithClause is null
            ? $"{functionName}(...)"
            : $"{functionName}(...) WITH (...)";
    }

    private static string FormatQualifiedTableName(SqlTableSource source)
    {
        if (source.Name is null)
            return "<unknown_table>";

        return source.DbName is null
            ? source.Name
            : $"{source.DbName}.{source.Name}";
    }

    private static string TryFormatOpenJsonPathShape(SqlExpr pathExpr)
    {
        if (pathExpr is not LiteralExpr { Value: string pathText })
            return "path";

        var trimmed = pathText.Trim();
        if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            return "strict path";

        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
            return "lax path";

        return "path";
    }

    private static string FormatJoinLine(SqlJoin join)
    {
        var joinType = FormatJoinType(join.Type);
        var source = FormatSource(join.Table);

        if (join.Type is SqlJoinType.CrossApply or SqlJoinType.OuterApply)
            return $"- JOIN: {joinType} {source}";

        var on = SqlExprPrinter.Print(join.On);
        return $"- JOIN: {joinType} {source} ON {on}";
    }

    private static string FormatJoinType(SqlJoinType joinType)
        => joinType switch
        {
            SqlJoinType.CrossApply => "CROSS APPLY",
            SqlJoinType.OuterApply => "OUTER APPLY",
            _ => joinType.ToString().ToUpperInvariant()
        };

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
        cost += EstimateGroupByExpressionComplexityCost(query.GroupBy);
        cost += EstimateOrderByExpressionComplexityCost(query.OrderBy);
        cost += EstimateDistinctGroupByOrderByCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.RowLimit);
        cost += EstimateDistinctGroupByOrderByJoinCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Joins);
        cost += EstimateDistinctGroupByOrderByHavingCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Having);
        cost += EstimateDistinctGroupByOrderByHavingJoinCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Having, query.Joins);
        cost += EstimateNestedOrderByCouplingCost(query.Table, query.OrderBy, query.RowLimit);
        cost += query.Joins.Sum(join => EstimateNestedOrderByCouplingCost(join.Table, query.OrderBy, query.RowLimit));
        cost += EstimateProjectionCost(query.SelectItems);
        cost -= EstimateRowLimitRelief(query.RowLimit);
        return Math.Max(1, cost);
    }

    private static int EstimateInsertCost(SqlInsertQuery query)
    {
        var cost = 8;
        cost += query.Columns.Count * 2;
        cost += query.ValuesRaw.Count * 3;

        if (query.InsertSelect is not null)
            cost += EstimateSelectCost(query.InsertSelect);

        if (query.HasOnDuplicateKeyUpdate)
            cost += 15 + query.OnDupAssigns.Count * 4;

        if (query.Returning.Count > 0)
            cost += query.Returning.Count * 2;

        return Math.Max(1, cost);
    }

    private static int EstimateUpdateCost(SqlUpdateQuery query)
    {
        var cost = 12;
        cost += query.Set.Count * 4;

        if (query.Where is not null)
            cost += 8;

        if (query.UpdateFromSelect is not null)
            cost += EstimateSelectCost(query.UpdateFromSelect);

        if (query.Returning.Count > 0)
            cost += query.Returning.Count * 2;

        return Math.Max(1, cost);
    }

    private static int EstimateDeleteCost(SqlDeleteQuery query)
    {
        var cost = 10;

        if (query.Where is not null || !string.IsNullOrWhiteSpace(query.WhereRaw))
            cost += 8;

        if (query.DeleteFromSelect is not null)
            cost += EstimateSelectCost(query.DeleteFromSelect);

        if (query.Returning.Count > 0)
            cost += query.Returning.Count * 2;

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

        var expansionRiskJoins = joins.Count(static j => j.Type is SqlJoinType.Left or SqlJoinType.Right or SqlJoinType.Cross or SqlJoinType.OuterApply or SqlJoinType.CrossApply);
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
    private static int EstimateCteQueryCost(SqlQueryBase query)
        => query switch
        {
            SqlSelectQuery select => EstimateCteSelectQueryCost(select),
            SqlUnionQuery union => union.Parts.Sum(EstimateCteSelectQueryCost) + union.Parts.Count,
            _ => 2
        };

    private static int EstimateCteSelectQueryCost(SqlSelectQuery query)
    {
        var cost = 2;
        cost += query.Joins.Count * 3;
        cost += EstimateSourceCost(query.Table);
        cost += query.Joins.Sum(static j => EstimateSourceCost(j.Table));
        if (query.Where is not null)
            cost += 2 + EstimatePredicateComplexityCost(query.Where);

        cost += EstimateAggregationCost(query.GroupBy, query.Having);
        cost += EstimateSortAndDedupCost(query.OrderBy, query.Distinct, query.RowLimit);
        cost += EstimateGroupByExpressionComplexityCost(query.GroupBy);
        cost += EstimateOrderByExpressionComplexityCost(query.OrderBy);
        cost += EstimateDistinctGroupByOrderByCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.RowLimit);
        cost += EstimateDistinctGroupByOrderByJoinCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Joins);
        cost += EstimateDistinctGroupByOrderByHavingCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Having);
        cost += EstimateDistinctGroupByOrderByHavingJoinCouplingCost(query.Distinct, query.GroupBy, query.OrderBy, query.Having, query.Joins);
        cost += EstimateNestedOrderByCouplingCost(query.Table, query.OrderBy, query.RowLimit);
        cost += query.Joins.Sum(join => EstimateNestedOrderByCouplingCost(join.Table, query.OrderBy, query.RowLimit));
        cost += EstimateProjectionCost(query.SelectItems);
        cost -= EstimateRowLimitRelief(query.RowLimit);
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
            SqlJoinType.CrossApply => 8,
            SqlJoinType.OuterApply => 6,
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
                => 2 + EstimateLogicalPredicateDepthPenalty(b) + EstimateLogicalOperatorMixPenalty(b) + EstimatePredicateComplexityCost(b.Left) + EstimatePredicateComplexityCost(b.Right),
            BinaryExpr b => 1 + EstimatePredicateComplexityCost(b.Left) + EstimatePredicateComplexityCost(b.Right),
            LikeExpr l => 2 + EstimatePredicateComplexityCost(l.Left) + EstimatePredicateComplexityCost(l.Pattern) + (l.Escape is null ? 0 : EstimatePredicateComplexityCost(l.Escape)),
            BetweenExpr b => 2 + EstimatePredicateComplexityCost(b.Expr) + EstimatePredicateComplexityCost(b.Low) + EstimatePredicateComplexityCost(b.High),
            InExpr i => 2 + EstimatePredicateComplexityCost(i.Left) + i.Items.Sum(EstimatePredicateComplexityCost) + i.Items.Count,
            ExistsExpr e => EstimateSubqueryPredicateCost(e.Subquery),
            SubqueryExpr s => EstimateSubqueryPredicateCost(s),
            QuantifiedComparisonExpr q => 2 + EstimatePredicateComplexityCost(q.Left) + EstimateSubqueryPredicateCost(q.Subquery),
            FunctionCallExpr f => 1 + EstimateJsonPredicateFunctionPenalty(f.Name) + f.Args.Sum(EstimatePredicateComplexityCost),
            CallExpr c => 1 + EstimateJsonPredicateFunctionPenalty(c.Name) + c.Args.Sum(EstimatePredicateComplexityCost),
            CaseExpr c => EstimateCasePredicateComplexityCost(c),
            JsonAccessExpr j => 1 + EstimatePredicateComplexityCost(j.Target) + EstimatePredicateComplexityCost(j.Path),
            RowExpr r => 1 + r.Items.Sum(EstimatePredicateComplexityCost),
            UnaryExpr u => 1 + EstimatePredicateComplexityCost(u.Expr),
            IsNullExpr n => 1 + EstimatePredicateComplexityCost(n.Expr),
            RawSqlExpr r => EstimateRawPredicateTokenCost(r.Sql),
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
    /// EN: Estimates penalty when logical operator type switches between parent and child nodes (AND/OR transitions), increasing reasoning and branch complexity.
    /// PT: Estima penalidade quando o tipo de operador lógico muda entre nós pai e filho (transições AND/OR), aumentando complexidade de raciocínio e ramificação.
    /// </summary>
    private static int EstimateLogicalOperatorMixPenalty(BinaryExpr expr)
    {
        var penalty = 0;

        if (expr.Left is BinaryExpr left && left.Op is SqlBinaryOp.And or SqlBinaryOp.Or && left.Op != expr.Op)
            penalty++;

        if (expr.Right is BinaryExpr right && right.Op is SqlBinaryOp.And or SqlBinaryOp.Or && right.Op != expr.Op)
            penalty++;

        return penalty;
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
    /// EN: Estimates lightweight predicate complexity for raw SQL expressions using token counts (logical operators, subquery hints and JSON markers).
    /// PT: Estima complexidade leve de predicado para expressões SQL raw usando contagem de tokens (operadores lógicos, hints de subconsulta e marcadores JSON).
    /// </summary>
    private static int EstimateRawPredicateTokenCost(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return 0;

        var cost = 0;
        cost += CountSqlKeywordOccurrences(raw, "AND");
        cost += CountSqlKeywordOccurrences(raw, "OR");
        cost += CountLogicalOperatorTransitions(raw);
        cost += EstimateRawLogicalNestingDepthPenalty(raw);
        cost += CountSqlKeywordOccurrences(raw, "CASE") * 2;
        cost += CountSqlKeywordOccurrences(raw, "SELECT") * 5;
        cost += CountSqlKeywordOccurrences(raw, "EXISTS") * 4;
        cost += CountSqlKeywordOccurrences(raw, "IN");
        cost += CountJsonFunctionCalls(raw) * 2;
        cost += CountJsonOperatorTokens(raw) * 2;
        return cost;
    }

    /// <summary>
    /// EN: Estimates extra coupling when HAVING is applied on top of an already coupled DISTINCT + GROUP BY + ORDER BY shape.
    /// PT: Estima acoplamento extra quando HAVING é aplicado sobre um formato já acoplado de DISTINCT + GROUP BY + ORDER BY.
    /// </summary>
    private static int EstimateDistinctGroupByOrderByHavingCouplingCost(
        bool distinct,
        IReadOnlyList<string> groupBy,
        IReadOnlyList<SqlOrderByItem> orderBy,
        SqlExpr? having)
    {
        if (!distinct || groupBy.Count == 0 || orderBy.Count == 0 || having is null)
            return 0;

        var havingComplexity = EstimatePredicateComplexityCost(having);
        return 3 + Math.Min(4, Math.Max(1, havingComplexity));
    }

    /// <summary>
    /// EN: Estimates extra HAVING coupling for DISTINCT + GROUP BY + ORDER BY when joins include expansion-risk edges.
    /// PT: Estima acoplamento extra de HAVING para DISTINCT + GROUP BY + ORDER BY quando joins incluem arestas com risco de expansão.
    /// </summary>
    private static int EstimateDistinctGroupByOrderByHavingJoinCouplingCost(
        bool distinct,
        IReadOnlyList<string> groupBy,
        IReadOnlyList<SqlOrderByItem> orderBy,
        SqlExpr? having,
        IReadOnlyList<SqlJoin> joins)
    {
        if (!distinct || groupBy.Count == 0 || orderBy.Count == 0 || having is null || joins.Count == 0)
            return 0;

        var expansionRiskJoins = joins.Count(static j => j.Type is SqlJoinType.Left or SqlJoinType.Right or SqlJoinType.Cross or SqlJoinType.OuterApply or SqlJoinType.CrossApply);
        if (expansionRiskJoins <= 0)
            return 0;

        var joinPredicateComplexity = joins.Sum(static j => EstimatePredicateComplexityCost(j.On));
        return 1 + Math.Min(3, expansionRiskJoins) + Math.Min(3, joinPredicateComplexity / 2);
    }

    /// <summary>
    /// EN: Estimates extra cost for complex GROUP BY expressions (for example CASE/subquery tokens) beyond key-count cardinality.
    /// PT: Estima custo extra para expressões complexas em GROUP BY (por exemplo tokens CASE/subquery) além da cardinalidade de chaves.
    /// </summary>
    private static int EstimateGroupByExpressionComplexityCost(IReadOnlyList<string> groupBy)
    {
        var cost = 0;
        foreach (var key in groupBy)
        {
            var raw = key ?? string.Empty;
            cost += CountSqlKeywordOccurrences(raw, "CASE") * 2;
            cost += CountSqlKeywordOccurrences(raw, "SELECT") * 5;
            cost += CountJsonFunctionCalls(raw) * 2;
            cost += CountJsonOperatorTokens(raw) * 2;
        }

        return cost;
    }

    /// <summary>
    /// EN: Estimates extra cost for complex ORDER BY expressions (for example CASE/subquery/window tokens) beyond key-count cardinality.
    /// PT: Estima custo extra para expressões complexas em ORDER BY (por exemplo tokens CASE/subquery/window) além da cardinalidade de chaves.
    /// </summary>
    private static int EstimateOrderByExpressionComplexityCost(IReadOnlyList<SqlOrderByItem> orderBy)
    {
        var cost = 0;
        foreach (var item in orderBy)
        {
            var raw = item.Raw ?? string.Empty;
            cost += CountSqlKeywordOccurrences(raw, "CASE") * 2;
            cost += CountSqlKeywordOccurrences(raw, "SELECT") * 5;
            cost += CountSqlKeywordOccurrences(raw, "OVER") * 3;
            cost += CountJsonFunctionCalls(raw) * 2;
            cost += CountJsonOperatorTokens(raw) * 2;
        }

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
        cost += EstimateDistinctGroupByOrderByExpressionCouplingCost(groupBy, orderBy);

        if (rowLimit is null)
            cost += 2;
        else
        {
            var (_, offset) = ExtractRowLimitCountAndOffset(rowLimit);
            cost += EstimateDistinctGroupByOrderByOffsetPenalty(offset);
        }

        return cost;
    }

    /// <summary>
    /// EN: Estimates extra coupling pressure for DISTINCT + GROUP BY + ORDER BY when grouping/ordering expressions are structurally complex (CASE/subquery/JSON markers).
    /// PT: Estima pressão extra de acoplamento para DISTINCT + GROUP BY + ORDER BY quando expressões de agrupamento/ordenação são estruturalmente complexas (marcadores CASE/subquery/JSON).
    /// </summary>
    private static int EstimateDistinctGroupByOrderByExpressionCouplingCost(
        IReadOnlyList<string> groupBy,
        IReadOnlyList<SqlOrderByItem> orderBy)
    {
        var markers = 0;

        foreach (var key in groupBy)
        {
            var raw = key ?? string.Empty;
            markers += CountSqlKeywordOccurrences(raw, "CASE");
            markers += CountSqlKeywordOccurrences(raw, "SELECT") * 2;
            markers += CountJsonFunctionCalls(raw);
            markers += CountJsonOperatorTokens(raw);
        }

        foreach (var item in orderBy)
        {
            var raw = item.Raw ?? string.Empty;
            markers += CountSqlKeywordOccurrences(raw, "CASE");
            markers += CountSqlKeywordOccurrences(raw, "SELECT") * 2;
            markers += CountSqlKeywordOccurrences(raw, "OVER");
            markers += CountJsonFunctionCalls(raw);
            markers += CountJsonOperatorTokens(raw);
        }

        return Math.Min(8, markers);
    }

    /// <summary>
    /// EN: Estimates extra DISTINCT + GROUP BY + ORDER BY coupling when joins are present, especially expansion-risk join types.
    /// PT: Estima acoplamento extra de DISTINCT + GROUP BY + ORDER BY quando há joins, especialmente tipos de join com risco de expansão.
    /// </summary>
    private static int EstimateDistinctGroupByOrderByJoinCouplingCost(
        bool distinct,
        IReadOnlyList<string> groupBy,
        IReadOnlyList<SqlOrderByItem> orderBy,
        IReadOnlyList<SqlJoin> joins)
    {
        if (!distinct || groupBy.Count == 0 || orderBy.Count == 0 || joins.Count == 0)
            return 0;

        var expansionRiskJoins = joins.Count(static j => j.Type is SqlJoinType.Left or SqlJoinType.Right or SqlJoinType.Cross or SqlJoinType.OuterApply or SqlJoinType.CrossApply);
        var cost = 1 + Math.Min(2, joins.Count);
        cost += expansionRiskJoins switch
        {
            <= 0 => 0,
            1 => 2,
            _ => 2 + (expansionRiskJoins - 1)
        };
        var joinPredicateComplexity = joins.Sum(static j => EstimatePredicateComplexityCost(j.On));
        cost += Math.Min(4, joinPredicateComplexity / 2);

        return Math.Min(8, cost);
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

        if (source.TableFunction is not null)
            return 6;

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
            cost += CountSqlKeywordOccurrences(raw, "OVER") * 12;
            cost += CountSqlKeywordOccurrences(raw, "CASE") * 4;
            cost += CountSqlKeywordOccurrences(raw, "SELECT") * 10;
            cost += CountJsonFunctionCalls(raw) * 3;
            cost += CountJsonOperatorTokens(raw) * 3;
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
        cost += CountSqlFunctionCalls(raw, "MIN") * 3;
        cost += CountSqlFunctionCalls(raw, "MAX") * 3;
        cost += CountDistinctAggregateCalls(raw, "COUNT") * 2;
        cost += CountDistinctAggregateCalls(raw, "SUM") * 2;
        cost += CountDistinctAggregateCalls(raw, "AVG") * 2;
        cost += CountDistinctAggregateCalls(raw, "MIN") * 2;
        cost += CountDistinctAggregateCalls(raw, "MAX") * 2;
        return cost;
    }

    /// <summary>
    /// EN: Counts function-call occurrences using case-insensitive token matching and an identifier-boundary guard.
    /// PT: Conta ocorrências de chamada de função usando correspondência de token case-insensitive e guarda de fronteira de identificador.
    /// </summary>
    private static int CountSqlFunctionCalls(string raw, string functionName)
    {
        var count = 0;
        var index = 0;

        while (true)
        {
            index = raw.IndexOf(functionName, index, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
                return count;

            var endOfName = index + functionName.Length;
            if (index > 0 && IsSqlIdentifierChar(raw[index - 1]))
            {
                index = endOfName;
                continue;
            }

            var parenPos = endOfName;
            while (parenPos < raw.Length && char.IsWhiteSpace(raw[parenPos]))
                parenPos++;

            if (parenPos < raw.Length && raw[parenPos] == '(')
                count++;

            index = endOfName;
        }
    }

    /// <summary>
    /// EN: Counts aggregate calls that include DISTINCT inside function arguments, supporting optional whitespace.
    /// PT: Conta chamadas agregadas que incluem DISTINCT nos argumentos da função, com suporte a espaços opcionais.
    /// </summary>
    private static int CountDistinctAggregateCalls(string raw, string functionName)
    {
        var count = 0;
        var index = 0;

        while (true)
        {
            index = raw.IndexOf(functionName, index, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
                return count;

            var endOfName = index + functionName.Length;
            if (index > 0 && IsSqlIdentifierChar(raw[index - 1]))
            {
                index = endOfName;
                continue;
            }

            var parenPos = endOfName;
            while (parenPos < raw.Length && char.IsWhiteSpace(raw[parenPos]))
                parenPos++;

            if (parenPos >= raw.Length || raw[parenPos] != '(')
            {
                index = endOfName;
                continue;
            }

            var argPos = parenPos + 1;
            while (argPos < raw.Length && char.IsWhiteSpace(raw[argPos]))
                argPos++;

            const string distinctToken = "DISTINCT";
            if (argPos + distinctToken.Length <= raw.Length
                && raw.AsSpan(argPos, distinctToken.Length).Equals(distinctToken, StringComparison.OrdinalIgnoreCase)
                && (argPos + distinctToken.Length == raw.Length || !IsSqlIdentifierChar(raw[argPos + distinctToken.Length])))
            {
                count++;
            }

            index = endOfName;
        }
    }

    /// <summary>
    /// EN: Determines whether a character can be part of a SQL identifier for token-boundary checks.
    /// PT: Determina se um caractere pode compor um identificador SQL para verificações de fronteira de token.
    /// </summary>
    private static bool IsSqlIdentifierChar(char c)
        => char.IsLetterOrDigit(c) || c is '_' or '$';

    /// <summary>
    /// EN: Counts SQL keyword token occurrences using identifier-boundary guards and optional trailing whitespace.
    /// PT: Conta ocorrências de token de palavra-chave SQL usando guardas de fronteira de identificador e whitespace opcional à direita.
    /// </summary>
    private static int CountSqlKeywordOccurrences(string raw, string keyword)
    {
        if (string.IsNullOrWhiteSpace(raw) || string.IsNullOrWhiteSpace(keyword))
            return 0;

        var count = 0;
        var index = 0;

        while (true)
        {
            index = raw.IndexOf(keyword, index, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
                return count;

            var leftBoundaryOk = index == 0 || !IsSqlIdentifierChar(raw[index - 1]);
            var right = index + keyword.Length;
            var rightBoundaryOk = right >= raw.Length || !IsSqlIdentifierChar(raw[right]);

            if (leftBoundaryOk && rightBoundaryOk)
                count++;

            index += keyword.Length;
        }
    }

    /// <summary>
    /// EN: Counts common JSON SQL function calls used across providers.
    /// PT: Conta chamadas comuns de funções SQL de JSON usadas entre providers.
    /// </summary>
    private static int CountJsonFunctionCalls(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return 0;

        var count = 0;
        count += CountSqlFunctionCalls(raw, "JSON_VALUE");
        count += CountSqlFunctionCalls(raw, "JSON_QUERY");
        count += CountSqlFunctionCalls(raw, "JSON_EXTRACT");
        return count;
    }

    /// <summary>
    /// EN: Counts JSON path operators commonly used across SQL dialects (->, ->>, #>, #>>) in raw expression text.
    /// PT: Conta operadores de caminho JSON comuns entre dialetos SQL (->, ->>, #>, #>>) no texto bruto da expressão.
    /// </summary>
    private static int CountJsonOperatorTokens(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return 0;

        var count = 0;
        var arrowDouble = CountTokenOccurrences(raw, "->>");
        var arrowSingle = Math.Max(0, CountTokenOccurrences(raw, "->") - arrowDouble);
        var hashDouble = CountTokenOccurrences(raw, "#>>");
        var hashSingle = Math.Max(0, CountTokenOccurrences(raw, "#>") - hashDouble);
        count += arrowDouble + arrowSingle + hashDouble + hashSingle;
        return count;
    }

    /// <summary>
    /// EN: Counts non-overlapping occurrences of an exact token in raw SQL text.
    /// PT: Conta ocorrências não sobrepostas de um token exato no texto SQL bruto.
    /// </summary>
    private static int CountTokenOccurrences(string raw, string token)
    {
        if (string.IsNullOrEmpty(raw) || string.IsNullOrEmpty(token))
            return 0;

        var count = 0;
        var index = 0;
        while (true)
        {
            index = raw.IndexOf(token, index, StringComparison.Ordinal);
            if (index < 0)
                return count;

            count++;
            index += token.Length;
        }
    }

    /// <summary>
    /// EN: Estimates additional predicate complexity for JSON-oriented scalar function calls.
    /// PT: Estima complexidade adicional de predicado para chamadas de funções escalares orientadas a JSON.
    /// </summary>
    private static int EstimateJsonPredicateFunctionPenalty(string functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return 0;

        return functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
               || functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
               || functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            ? 2
            : 0;
    }

    /// <summary>
    /// EN: Estimates DISTINCT + GROUP BY + ORDER BY coupling penalty induced by OFFSET even when LIMIT is present.
    /// PT: Estima penalidade de acoplamento DISTINCT + GROUP BY + ORDER BY induzida por OFFSET mesmo quando há LIMIT.
    /// </summary>
    private static int EstimateDistinctGroupByOrderByOffsetPenalty(int offset)
        => offset switch
        {
            <= 0 => 0,
            <= 100 => 1,
            <= 1000 => 2,
            _ => 3
        };

    /// <summary>
    /// EN: Estimates additional nested sort coupling when an outer ORDER BY is applied over an already ordered derived source.
    /// PT: Estima acoplamento adicional de sort aninhado quando um ORDER BY externo é aplicado sobre uma fonte derivada já ordenada.
    /// </summary>
    private static int EstimateNestedOrderByCouplingCost(
        SqlTableSource? source,
        IReadOnlyList<SqlOrderByItem> outerOrderBy,
        SqlRowLimit? outerRowLimit)
    {
        if (source is null || outerOrderBy.Count == 0)
            return 0;

        var innerOrderByCount = GetDerivedSourceOrderByCount(source);
        if (innerOrderByCount <= 0)
            return 0;

        var cost = 2;
        cost += Math.Min(2, Math.Max(0, innerOrderByCount - 1));
        cost += Math.Min(2, Math.Max(0, outerOrderBy.Count - 1));
        cost += Math.Min(4, GetDerivedSourceOrderByExpressionComplexityCost(source));
        cost += Math.Min(3, EstimateOrderByExpressionComplexityCost(outerOrderBy));
        cost += EstimateNestedOrderByInnerOffsetPenalty(GetDerivedSourceOrderByOffset(source));
        cost -= EstimateNestedOrderByInnerLimitRelief(GetDerivedSourceOrderByLimitCount(source));

        if (outerRowLimit is null)
            cost += 2;
        else
        {
            var (_, offset) = ExtractRowLimitCountAndOffset(outerRowLimit);
            if (offset > 0)
                cost += 1;
        }

        return Math.Max(0, cost);
    }

    /// <summary>
    /// EN: Gets ORDER BY key count from a derived source shape (derived SELECT or derived UNION).
    /// PT: Obtém a quantidade de chaves ORDER BY de um formato de fonte derivada (SELECT derivado ou UNION derivado).
    /// </summary>
    private static int GetDerivedSourceOrderByCount(SqlTableSource source)
    {
        if (source.DerivedUnion is not null)
            return source.DerivedUnion.OrderBy?.Count ?? 0;

        if (source.Derived is not null)
            return source.Derived.OrderBy.Count;

        return 0;
    }

    /// <summary>
    /// EN: Gets lightweight complexity score for ORDER BY expressions defined inside a derived source shape.
    /// PT: Obtém score leve de complexidade para expressões ORDER BY definidas dentro de um formato de fonte derivada.
    /// </summary>
    private static int GetDerivedSourceOrderByExpressionComplexityCost(SqlTableSource source)
    {
        if (source.DerivedUnion is not null)
            return EstimateOrderByExpressionComplexityCost(source.DerivedUnion.OrderBy ?? []);

        if (source.Derived is not null)
            return EstimateOrderByExpressionComplexityCost(source.Derived.OrderBy);

        return 0;
    }

    /// <summary>
    /// EN: Gets the inner row-limit count used by derived ORDER BY sources so tighter inner limits can reduce nested sort coupling.
    /// PT: Obtém a contagem de limite de linhas interno usada por fontes derivadas com ORDER BY para que limites internos mais restritos reduzam o acoplamento de sort aninhado.
    /// </summary>
    private static int GetDerivedSourceOrderByLimitCount(SqlTableSource source)
    {
        if (source.DerivedUnion?.RowLimit is not null)
        {
            var (count, _) = ExtractRowLimitCountAndOffset(source.DerivedUnion.RowLimit);
            return count;
        }

        if (source.Derived?.RowLimit is not null)
        {
            var (count, _) = ExtractRowLimitCountAndOffset(source.Derived.RowLimit);
            return count;
        }

        return 0;
    }

    /// <summary>
    /// EN: Gets the inner row-limit offset used by derived ORDER BY sources so deep inner offsets can increase nested sort coupling.
    /// PT: Obtém o offset interno de limite de linhas usado por fontes derivadas com ORDER BY para que offsets internos profundos possam aumentar o acoplamento de sort aninhado.
    /// </summary>
    private static int GetDerivedSourceOrderByOffset(SqlTableSource source)
    {
        if (source.DerivedUnion?.RowLimit is not null)
        {
            var (_, offset) = ExtractRowLimitCountAndOffset(source.DerivedUnion.RowLimit);
            return offset;
        }

        if (source.Derived?.RowLimit is not null)
        {
            var (_, offset) = ExtractRowLimitCountAndOffset(source.Derived.RowLimit);
            return offset;
        }

        return 0;
    }

    /// <summary>
    /// EN: Estimates relief in nested ORDER BY coupling for tighter inner limits in ordered derived sources.
    /// PT: Estima alívio no acoplamento de ORDER BY aninhado para limites internos mais restritos em fontes derivadas ordenadas.
    /// </summary>
    private static int EstimateNestedOrderByInnerLimitRelief(int innerLimitCount)
        => innerLimitCount switch
        {
            <= 0 => 0,
            <= 10 => 2,
            <= 100 => 1,
            _ => 0
        };

    /// <summary>
    /// EN: Estimates nested ORDER BY coupling penalty for large inner offsets in ordered derived sources.
    /// PT: Estima penalidade de acoplamento de ORDER BY aninhado para offsets internos altos em fontes derivadas ordenadas.
    /// </summary>
    private static int EstimateNestedOrderByInnerOffsetPenalty(int innerOffset)
        => innerOffset switch
        {
            <= 0 => 0,
            <= 100 => 1,
            <= 1000 => 2,
            _ => 3
        };

    /// <summary>
    /// EN: Counts transitions between logical operators (AND/OR) in raw SQL text while preserving keyword boundaries.
    /// PT: Conta transições entre operadores lógicos (AND/OR) em texto SQL raw preservando fronteiras de palavra-chave.
    /// </summary>
    private static int CountLogicalOperatorTransitions(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return 0;

        var transitions = 0;
        var index = 0;
        string? previous = null;

        while (TryFindNextLogicalOperator(raw, index, out var current, out var nextIndex))
        {
            if (previous is not null && !string.Equals(previous, current, StringComparison.Ordinal))
                transitions++;

            previous = current;
            index = nextIndex;
        }

        return transitions;
    }

    /// <summary>
    /// EN: Estimates additional raw logical penalty from parenthesis nesting depth when logical operators are present.
    /// PT: Estima penalidade lógica raw adicional pela profundidade de aninhamento de parênteses quando há operadores lógicos.
    /// </summary>
    private static int EstimateRawLogicalNestingDepthPenalty(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return 0;

        var logicalOperatorCount = CountSqlKeywordOccurrences(raw, "AND") + CountSqlKeywordOccurrences(raw, "OR");
        if (logicalOperatorCount == 0)
            return 0;

        var maxDepth = 0;
        var depth = 0;
        foreach (var c in raw)
        {
            if (c == '(')
            {
                depth++;
                maxDepth = Math.Max(maxDepth, depth);
            }
            else if (c == ')')
            {
                depth = Math.Max(0, depth - 1);
            }
        }

        return Math.Max(0, maxDepth - 1);
    }

    /// <summary>
    /// EN: Finds the next logical operator token (AND/OR) and returns the token plus the next scan index.
    /// PT: Encontra o próximo token de operador lógico (AND/OR) e retorna o token junto com o próximo índice de varredura.
    /// </summary>
    private static bool TryFindNextLogicalOperator(
        string raw,
        int startIndex,
        out string op,
        out int nextIndex)
    {
        var andIndex = FindSqlKeywordIndex(raw, "AND", startIndex);
        var orIndex = FindSqlKeywordIndex(raw, "OR", startIndex);

        if (andIndex < 0 && orIndex < 0)
        {
            op = string.Empty;
            nextIndex = raw.Length;
            return false;
        }

        var useAnd = andIndex >= 0 && (orIndex < 0 || andIndex < orIndex);
        if (useAnd)
        {
            op = "AND";
            nextIndex = andIndex + 3;
            return true;
        }

        op = "OR";
        nextIndex = orIndex + 2;
        return true;
    }

    /// <summary>
    /// EN: Finds the next keyword token index using identifier-boundary guards starting from a specified position.
    /// PT: Encontra o próximo índice de token de palavra-chave usando guardas de fronteira de identificador a partir de uma posição especificada.
    /// </summary>
    private static int FindSqlKeywordIndex(string raw, string keyword, int startIndex)
    {
        if (string.IsNullOrWhiteSpace(raw) || string.IsNullOrWhiteSpace(keyword) || startIndex >= raw.Length)
            return -1;

        var index = Math.Max(0, startIndex);
        while (true)
        {
            index = raw.IndexOf(keyword, index, StringComparison.OrdinalIgnoreCase);
            if (index < 0)
                return -1;

            var leftBoundaryOk = index == 0 || !IsSqlIdentifierChar(raw[index - 1]);
            var right = index + keyword.Length;
            var rightBoundaryOk = right >= raw.Length || !IsSqlIdentifierChar(raw[right]);
            if (leftBoundaryOk && rightBoundaryOk)
                return index;

            index += keyword.Length;
        }
    }

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
        cost += EstimateUnionSetOperatorTransitionCost(allFlags);
        cost += EstimateUnionOrderByMergeFanInCost(parts.Count, orderBy ?? [], rowLimit);
        cost += EstimateSortAndDedupCost(orderBy ?? [], false, rowLimit);
        cost += EstimateOrderByExpressionComplexityCost(orderBy ?? []);
        cost -= EstimateRowLimitRelief(rowLimit);
        return Math.Max(1, cost);
    }

    /// <summary>
    /// EN: Estimates extra UNION cost when set operators alternate between ALL and DISTINCT, increasing stage-switching overhead.
    /// PT: Estima custo extra de UNION quando operadores de conjunto alternam entre ALL e DISTINCT, aumentando overhead de troca de estágio.
    /// </summary>
    private static int EstimateUnionSetOperatorTransitionCost(IReadOnlyList<bool> allFlags)
    {
        if (allFlags.Count <= 1)
            return 0;

        var transitions = 0;
        for (var i = 1; i < allFlags.Count; i++)
        {
            if (allFlags[i] != allFlags[i - 1])
                transitions++;
        }

        return transitions * 3;
    }

    /// <summary>
    /// EN: Estimates additional ORDER BY merge fan-in overhead for UNION plans with many parts.
    /// PT: Estima overhead adicional de fan-in de merge de ORDER BY para planos UNION com muitas partes.
    /// </summary>
    private static int EstimateUnionOrderByMergeFanInCost(
        int partCount,
        IReadOnlyList<SqlOrderByItem> orderBy,
        SqlRowLimit? rowLimit)
    {
        if (partCount <= 2 || orderBy.Count == 0)
            return 0;

        var cost = (partCount - 2) * 2;
        cost += Math.Min(2, Math.Max(0, orderBy.Count - 1));

        if (rowLimit is null)
            cost += 1;
        else
        {
            var (_, offset) = ExtractRowLimitCountAndOffset(rowLimit);
            if (offset > 0)
                cost += 1;
        }

        cost += Math.Min(4, EstimateOrderByExpressionComplexityCost(orderBy));
        return Math.Min(10, cost);
    }
}
