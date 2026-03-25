namespace DbSqlLikeMem;

internal static class AstQueryPlanMetricsHelper
{
    internal static SqlPlanMockRuntimeContext BuildPlanMockRuntimeContext(QueryExecutionContext context)
        => new(
            context.SimulatedLatencyMs,
            context.DropProbability,
            context.ThreadSafe);

    internal static SqlPlanRuntimeMetrics BuildPlanRuntimeMetrics(
        QueryExecutionContext context,
        SqlSelectQuery query,
        int actualRows,
        long elapsedMs)
        => new(
            InputTables: CountKnownInputTables(query),
            EstimatedRowsRead: EstimateRowsRead(context, query),
            ActualRows: actualRows,
            ElapsedMs: elapsedMs);

    internal static int CountKnownInputTables(SqlSelectQuery query)
    {
        var count = 0;
        if (query.Table is not null && HasKnownPhysicalTable(query.Table))
            count++;

        foreach (var join in query.Joins)
        {
            if (HasKnownPhysicalTable(join.Table))
                count++;
        }

        return count;
    }

    internal static long EstimateRowsRead(QueryExecutionContext context, SqlSelectQuery query)
    {
        long total = 0;

        total += GetKnownSourceRows(context, query.Table);
        foreach (var join in query.Joins)
            total += GetKnownSourceRows(context, join.Table);

        return total;
    }

    internal static bool HasKnownPhysicalTable(SqlTableSource source)
        => source.Name is not null && source.Derived is null && source.DerivedUnion is null && source.TableFunction is null;

    internal static long GetKnownSourceRows(QueryExecutionContext context, SqlTableSource? source)
    {
        if (source is null || !HasKnownPhysicalTable(source) || string.IsNullOrWhiteSpace(source.Name))
            return 0;

        if (context.Connection.TryGetTable(source.Name!, out var table) && table is not null)
            return table.Count;

        return 0;
    }
}
