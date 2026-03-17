namespace DbSqlLikeMem;

internal sealed class AstQueryJoinService(
    Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, AstQueryExecutorBase.Source> resolveSource,
    Func<IReadOnlyList<SqlMySqlIndexHint>?, ITableMock, bool, bool, AstQueryExecutorBase.MySqlIndexHintPlan?> buildMySqlIndexHintPlan,
    Func<SqlExpr, AstQueryExecutorBase.EvalRow, IDictionary<string, AstQueryExecutorBase.Source>, bool> evalJoinPredicate)
{
    private readonly Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, AstQueryExecutorBase.Source> _resolveSource = resolveSource;
    private readonly Func<IReadOnlyList<SqlMySqlIndexHint>?, ITableMock, bool, bool, AstQueryExecutorBase.MySqlIndexHintPlan?> _buildMySqlIndexHintPlan = buildMySqlIndexHintPlan;
    private readonly Func<SqlExpr, AstQueryExecutorBase.EvalRow, IDictionary<string, AstQueryExecutorBase.Source>, bool> _evalJoinPredicate = evalJoinPredicate;

    public IEnumerable<AstQueryExecutorBase.EvalRow> ApplyJoin(
        IEnumerable<AstQueryExecutorBase.EvalRow> leftRows,
        SqlJoin join,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        var joinType = join.Type;

        if (joinType is SqlJoinType.CrossApply or SqlJoinType.OuterApply)
        {
            var isOuterApply = joinType == SqlJoinType.OuterApply;
            foreach (var leftRow in leftRows)
            {
                foreach (var applied in ApplyApplyJoin(join, ctes, hasOrderBy, hasGroupBy, leftRow, isOuterApply))
                    yield return applied;
            }

            yield break;
        }

        if (join.Table.IsLateral)
        {
            foreach (var leftRow in leftRows)
            {
                foreach (var lateralRow in ApplyLateralJoin(join, ctes, leftRow, joinType == SqlJoinType.Left))
                    yield return lateralRow;
            }

            yield break;
        }

        var rightSource = _resolveSource(join.Table, ctes, null);
        EnsureForcedIndexesExist(join.Table.MySqlIndexHints, rightSource, hasOrderBy, hasGroupBy);

        if (joinType == SqlJoinType.Cross)
        {
            foreach (var leftRow in leftRows)
            {
                foreach (var rightFields in rightSource.Rows())
                    yield return MergeRows(leftRow, rightSource, rightFields);
            }

            yield break;
        }

        if (joinType == SqlJoinType.Right)
        {
            var swapped = new SqlJoin(SqlJoinType.Left, join.Table, join.On);
            foreach (var row in ApplyJoinRight(leftRows, swapped, rightSource, ctes))
                yield return row;
            yield break;
        }

        var isLeftJoin = joinType == SqlJoinType.Left;
        foreach (var leftRow in leftRows)
        {
            foreach (var row in ApplyLeftJoin(join, ctes, rightSource, isLeftJoin, leftRow))
                yield return row;
        }
    }

    private IEnumerable<AstQueryExecutorBase.EvalRow> ApplyApplyJoin(
        SqlJoin join,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        bool hasOrderBy,
        bool hasGroupBy,
        AstQueryExecutorBase.EvalRow leftRow,
        bool isOuterApply)
    {
        var rightSource = _resolveSource(join.Table, ctes, leftRow);
        EnsureForcedIndexesExist(join.Table.MySqlIndexHints, rightSource, hasOrderBy, hasGroupBy);

        var matched = false;
        foreach (var rightFields in rightSource.Rows())
        {
            matched = true;
            yield return MergeRows(leftRow, rightSource, rightFields);
        }

        if (isOuterApply && !matched)
            yield return CreateNullExtendedRow(leftRow, rightSource);
    }

    private IEnumerable<AstQueryExecutorBase.EvalRow> ApplyLeftJoin(
        SqlJoin join,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.Source rightSource,
        bool isLeftJoin,
        AstQueryExecutorBase.EvalRow leftRow)
    {
        var matched = false;
        foreach (var rightFields in rightSource.Rows())
        {
            var merged = MergeRows(leftRow, rightSource, rightFields);
            if (!_evalJoinPredicate(join.On, merged, ctes))
                continue;

            matched = true;
            yield return merged;
        }

        if (isLeftJoin && !matched)
            yield return CreateNullExtendedRow(leftRow, rightSource);
    }

    private IEnumerable<AstQueryExecutorBase.EvalRow> ApplyLateralJoin(
        SqlJoin join,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow leftRow,
        bool isLeftJoin)
    {
        var rightSource = _resolveSource(join.Table, ctes, leftRow);
        var matched = false;

        foreach (var rightFields in rightSource.Rows())
        {
            var merged = MergeRows(leftRow, rightSource, rightFields);
            if (!_evalJoinPredicate(join.On, merged, ctes))
                continue;

            matched = true;
            yield return merged;
        }

        if (isLeftJoin && !matched)
            yield return CreateNullExtendedRow(leftRow, rightSource);
    }

    private IEnumerable<AstQueryExecutorBase.EvalRow> ApplyJoinRight(
        IEnumerable<AstQueryExecutorBase.EvalRow> leftRows,
        SqlJoin leftJoin,
        AstQueryExecutorBase.Source rightSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes)
    {
        var leftList = leftRows as IList<AstQueryExecutorBase.EvalRow> ?? [.. leftRows];

        foreach (var rightFields in rightSource.Rows())
        {
            var matched = false;
            foreach (var leftRow in leftList)
            {
                var merged = MergeRows(leftRow, rightSource, rightFields);
                if (!_evalJoinPredicate(leftJoin.On, merged, ctes))
                    continue;

                matched = true;
                yield return merged;
            }

            if (!matched)
                yield return CreateRightOnlyRow(rightSource, rightFields);
        }
    }

    private void EnsureForcedIndexesExist(
        IReadOnlyList<SqlMySqlIndexHint>? hints,
        AstQueryExecutorBase.Source source,
        bool hasOrderBy,
        bool hasGroupBy)
    {
        if (source.Physical is null)
            return;

        var hintPlan = _buildMySqlIndexHintPlan(hints, source.Physical, hasOrderBy, hasGroupBy);
        if (hintPlan?.MissingForcedIndexes.Count > 0)
            throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");
    }

    private static AstQueryExecutorBase.EvalRow MergeRows(
        AstQueryExecutorBase.EvalRow leftRow,
        AstQueryExecutorBase.Source rightSource,
        Dictionary<string, object?> rightFields)
    {
        var merged = leftRow.CloneRow();
        merged.AddSource(rightSource);
        merged.AddFields(rightFields);
        return merged;
    }

    private static AstQueryExecutorBase.EvalRow CreateNullExtendedRow(
        AstQueryExecutorBase.EvalRow leftRow,
        AstQueryExecutorBase.Source rightSource)
    {
        var merged = leftRow.CloneRow();
        merged.AddSource(rightSource);
        foreach (var columnName in rightSource.ColumnNames)
            merged.Fields[$"{rightSource.Alias}.{columnName}"] = null;

        return merged;
    }

    private static AstQueryExecutorBase.EvalRow CreateRightOnlyRow(
        AstQueryExecutorBase.Source rightSource,
        Dictionary<string, object?> rightFields)
    {
        var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var sources = new Dictionary<string, AstQueryExecutorBase.Source>(StringComparer.OrdinalIgnoreCase)
        {
            [rightSource.Alias] = rightSource
        };

        var row = new AstQueryExecutorBase.EvalRow(fields, sources);
        row.AddFields(rightFields);
        return row;
    }
}
