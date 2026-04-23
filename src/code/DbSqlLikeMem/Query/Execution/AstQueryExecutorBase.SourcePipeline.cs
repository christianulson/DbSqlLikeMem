namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private IEnumerable<EvalRow> BuildFrom(
        SqlTableSource? from,
        IDictionary<string, Source> ctes,
        SqlExpr? where,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (from is null)
        {
            yield return EvalRow.Empty();
            yield break;
        }

        var src = ResolveSource(from, ctes);
        if (from.PartitionNames is { Count: > 0 } requestedPartitions
            && src.Physical is TableMock partitionedTable)
        {
            src = src.WithRequestedPartitions(requestedPartitions);
        }

        src = PartitionHelper.ApplyPartitionPruning(src, where);
        var sourceRows = IndexHelper.TryRowsFromIndex(src, from, where, hasOrderBy, hasGroupBy) ?? src.Rows();
        foreach (var r in sourceRows)
            yield return AstQueryRowSourceHelper.CreateSourceEvalRow(src, r);
    }

    private void TryRecordPrimaryKeyHintMetric(
        ITableMock table,
        MySqlIndexHintPlan? hintPlan)
    {
        if (hintPlan is null || !Cnn.Metrics.Enabled)
            return;

        if (!hintPlan.HasRowAccessHints)
            return;

        string? hintedPrimaryEquivalent = null;
        foreach (var item in hintPlan.PrimaryEquivalentIndexNames)
        {
            if (!hintPlan.AllowedIndexNames.Contains(item))
                continue;

            hintedPrimaryEquivalent = item;
            break;
        }

        if (!string.IsNullOrWhiteSpace(hintedPrimaryEquivalent))
            Cnn.Metrics.IncrementIndexHint(hintedPrimaryEquivalent!);
    }

    private IEnumerable<EvalRow> ApplyJoin(
        IEnumerable<EvalRow> leftRows,
        SqlJoin join,
        IDictionary<string, Source> ctes,
        bool hasOrderBy,
        bool hasGroupBy)
        => JoinService.ApplyJoin(leftRows, join, ctes, hasOrderBy, hasGroupBy);

    private Source ResolveSource(
        SqlTableSource ts,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow = null)
    {
        var source = SourceResolver.ResolveBaseSource(ts, ctes, outerRow);
        return ApplyTableTransformsIfNeeded(source, ts.Pivot, ts.Unpivot, ctes);
    }

    private Source ApplyTableTransformsIfNeeded(
        Source source,
        SqlPivotSpec? pivot,
        SqlUnpivotSpec? unpivot,
        IDictionary<string, Source> ctes)
        => PivotHelper.ApplyTableTransformsIfNeeded(source, pivot, unpivot, ctes);
}
