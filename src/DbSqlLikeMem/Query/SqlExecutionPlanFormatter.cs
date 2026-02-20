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

internal static class SqlExecutionPlanFormatter
{
    public static string FormatSelect(
        SqlSelectQuery query,
        SqlPlanRuntimeMetrics metrics)
    {
        var sb = new StringBuilder();
        sb.AppendLine("Execution Plan (mock)");
        sb.AppendLine("- QueryType: SELECT");
        sb.AppendLine($"- EstimatedCost: {EstimateSelectCost(query)}");

        if (query.Ctes.Count > 0)
        {
            sb.AppendLine($"- CTEs: {query.Ctes.Count}");
            foreach (var cte in query.Ctes)
                sb.AppendLine($"  - CTE Materialize: {cte.Name}");
        }

        sb.AppendLine($"- From: {FormatSource(query.Table)}");

        if (query.Joins.Count > 0)
        {
            foreach (var join in query.Joins)
            {
                var on = SqlExprPrinter.Print(join.On);
                sb.AppendLine($"- Join: {join.Type.ToString().ToUpperInvariant()} {FormatSource(join.Table)} ON {on}");
            }
        }

        if (query.Where is not null)
            sb.AppendLine($"- Filter: {SqlExprPrinter.Print(query.Where)}");

        if (query.GroupBy.Count > 0)
            sb.AppendLine($"- GroupBy: {string.Join(", ", query.GroupBy)}");

        if (query.Having is not null)
            sb.AppendLine($"- Having: {SqlExprPrinter.Print(query.Having)}");

        sb.AppendLine($"- Projection: {query.SelectItems.Count} item(s)");

        if (query.Distinct)
            sb.AppendLine("- Distinct: true");

        if (query.OrderBy.Count > 0)
        {
            var order = string.Join(", ", query.OrderBy.Select(o => $"{o.Raw} {(o.Desc ? "DESC" : "ASC")}"));
            sb.AppendLine($"- Sort: {order}");
        }

        if (query.RowLimit is not null)
            sb.AppendLine($"- Limit: {FormatLimit(query.RowLimit)}");

        sb.AppendLine($"- InputTables: {metrics.InputTables}");
        sb.AppendLine($"- EstimatedRowsRead: {metrics.EstimatedRowsRead}");
        sb.AppendLine($"- ActualRows: {metrics.ActualRows}");
        sb.AppendLine($"- SelectivityPct: {metrics.SelectivityPct:F2}");
        sb.AppendLine($"- RowsPerMs: {metrics.RowsPerMs:F2}");
        sb.AppendLine($"- ElapsedMs: {metrics.ElapsedMs}");

        return sb.ToString().TrimEnd();
    }

    public static string FormatUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy,
        SqlRowLimit? rowLimit,
        SqlPlanRuntimeMetrics metrics)
    {
        var sb = new StringBuilder();
        sb.AppendLine("Execution Plan (mock)");
        sb.AppendLine("- QueryType: UNION");
        sb.AppendLine($"- EstimatedCost: {EstimateUnionCost(parts, allFlags, orderBy, rowLimit)}");
        sb.AppendLine($"- Parts: {parts.Count}");

        for (int i = 0; i < parts.Count; i++)
            sb.AppendLine($"  - Part[{i + 1}]: SELECT from {FormatSource(parts[i].Table)}");

        for (int i = 0; i < allFlags.Count; i++)
            sb.AppendLine($"  - Combine[{i + 1}]: {(allFlags[i] ? "UNION ALL" : "UNION DISTINCT")}");

        if ((orderBy?.Count ?? 0) > 0)
        {
            var order = string.Join(", ", orderBy!.Select(o => $"{o.Raw} {(o.Desc ? "DESC" : "ASC")}"));
            sb.AppendLine($"- Sort: {order}");
        }

        if (rowLimit is not null)
            sb.AppendLine($"- Limit: {FormatLimit(rowLimit)}");

        sb.AppendLine($"- InputTables: {metrics.InputTables}");
        sb.AppendLine($"- EstimatedRowsRead: {metrics.EstimatedRowsRead}");
        sb.AppendLine($"- ActualRows: {metrics.ActualRows}");
        sb.AppendLine($"- SelectivityPct: {metrics.SelectivityPct:F2}");
        sb.AppendLine($"- RowsPerMs: {metrics.RowsPerMs:F2}");
        sb.AppendLine($"- ElapsedMs: {metrics.ElapsedMs}");

        return sb.ToString().TrimEnd();
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
