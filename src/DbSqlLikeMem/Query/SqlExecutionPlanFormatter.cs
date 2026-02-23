using System.Text;

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
        IReadOnlyList<SqlPlanWarning>? planWarnings = null)
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

        AppendIndexRecommendations(sb, indexRecommendations);
        AppendPlanWarnings(sb, planWarnings);

        return sb.ToString().TrimEnd();
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

    private static string FormatWarningSeverity(SqlPlanWarningSeverity severity)
        => severity switch
        {
            SqlPlanWarningSeverity.Info => SqlExecutionPlanMessages.SeverityInfoValue(),
            SqlPlanWarningSeverity.Warning => SqlExecutionPlanMessages.SeverityWarningValue(),
            SqlPlanWarningSeverity.High => SqlExecutionPlanMessages.SeverityHighValue(),
            _ => severity.ToString()
        };

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
