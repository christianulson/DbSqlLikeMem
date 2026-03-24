namespace DbSqlLikeMem;

internal static class AstQueryPlanMetricsHelper
{
    internal static SqlPlanMockRuntimeContext BuildPlanMockRuntimeContext(DbConnectionMockBase cnn)
        => new(
            cnn.SimulatedLatencyMs,
            cnn.DropProbability,
            cnn.Db.ThreadSafe);

    internal static SqlPlanRuntimeMetrics BuildPlanRuntimeMetrics(
        DbConnectionMockBase cnn,
        SqlSelectQuery query,
        int actualRows,
        long elapsedMs)
        => new(
            InputTables: CountKnownInputTables(query),
            EstimatedRowsRead: EstimateRowsRead(cnn, query),
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

    internal static long EstimateRowsRead(DbConnectionMockBase cnn, SqlSelectQuery query)
    {
        long total = 0;

        total += GetKnownSourceRows(cnn, query.Table);
        foreach (var join in query.Joins)
            total += GetKnownSourceRows(cnn, join.Table);

        return total;
    }

    internal static bool HasKnownPhysicalTable(SqlTableSource source)
        => source.Name is not null && source.Derived is null && source.DerivedUnion is null && source.TableFunction is null;

    internal static long GetKnownSourceRows(DbConnectionMockBase cnn, SqlTableSource? source)
    {
        if (source is null || !HasKnownPhysicalTable(source) || string.IsNullOrWhiteSpace(source.Name))
            return 0;

        if (cnn.TryGetTable(source.Name!, out var table) && table is not null)
            return table.Count;

        return 0;
    }
}
