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
            var rightRows = MaterializeFields(rightSource.Rows());
            if (rightRows is ICollection<Dictionary<string, object?>> rightCollection
                && rightCollection.Count == 0)
            {
                yield break;
            }

            foreach (var leftRow in leftRows)
            {
                foreach (var rightFields in rightRows)
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
        var leftJoinRightRows = MaterializeFields(rightSource.Rows());
        foreach (var leftRow in leftRows)
        {
            foreach (var row in ApplyLeftJoin(join, ctes, leftJoinRightRows, rightSource, isLeftJoin, leftRow))
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
        IReadOnlyList<Dictionary<string, object?>> rightRows,
        AstQueryExecutorBase.Source rightSource,
        bool isLeftJoin,
        AstQueryExecutorBase.EvalRow leftRow)
    {
        if (rightRows.Count == 0)
        {
            if (isLeftJoin)
                yield return CreateNullExtendedRow(leftRow, rightSource);

            yield break;
        }

        var matched = false;
        for (var i = 0; i < rightRows.Count; i++)
        {
            var rightFields = rightRows[i];
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
        var rightRows = rightSource.Rows();
        if (rightRows is ICollection<Dictionary<string, object?>> rightCollection
            && rightCollection.Count == 0)
        {
            if (isLeftJoin)
                yield return CreateNullExtendedRow(leftRow, rightSource);

            yield break;
        }

        var matched = false;

        foreach (var rightFields in rightRows)
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
        var rightRows = MaterializeFields(rightSource.Rows());
        if (rightRows.Count == 0)
        {
            yield break;
        }

        if (leftRows is ICollection<AstQueryExecutorBase.EvalRow> leftCollection
            && leftCollection.Count == 0)
        {
            for (var i = 0; i < rightRows.Count; i++)
                yield return CreateRightOnlyRow(null, rightSource, rightRows[i]);

            yield break;
        }

        var leftList = MaterializeRows(leftRows);
        var leftTemplate = leftList.Count > 0 ? leftList[0] : null;

        if (leftList.Count == 0)
        {
            for (var i = 0; i < rightRows.Count; i++)
                yield return CreateRightOnlyRow(null, rightSource, rightRows[i]);

            yield break;
        }

        for (var rightIndex = 0; rightIndex < rightRows.Count; rightIndex++)
        {
            var rightFields = rightRows[rightIndex];
            var matched = false;
            for (var leftIndex = 0; leftIndex < leftList.Count; leftIndex++)
            {
                var leftRow = leftList[leftIndex];
                var merged = MergeRows(leftRow, rightSource, rightFields);
                if (!_evalJoinPredicate(leftJoin.On, merged, ctes))
                    continue;

                matched = true;
                yield return merged;
            }

            if (!matched)
                yield return CreateRightOnlyRow(leftTemplate, rightSource, rightFields);
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
        => leftRow.MergeJoinRow(rightSource, rightFields);

    private static AstQueryExecutorBase.EvalRow CreateNullExtendedRow(
        AstQueryExecutorBase.EvalRow leftRow,
        AstQueryExecutorBase.Source rightSource)
        => leftRow.CreateNullExtendedJoinRow(rightSource);

    private static AstQueryExecutorBase.EvalRow CreateRightOnlyRow(
        AstQueryExecutorBase.EvalRow? leftTemplate,
        AstQueryExecutorBase.Source rightSource,
        Dictionary<string, object?> rightFields)
    {
        var rightOrdinalCount = rightSource.ColumnNames.Count;
        Dictionary<string, object?> fields;
        Dictionary<string, AstQueryExecutorBase.Source> sources;
        object?[]? ordinalValues = null;
        Dictionary<string, int>? ordinalIndexes = null;

        if (leftTemplate is not null)
        {
            fields = rightFields;
            fields.EnsureCapacity(fields.Count + leftTemplate.Fields.Count);
            foreach (var key in leftTemplate.Fields.Keys)
                fields.TryAdd(key, null);

            sources = new Dictionary<string, AstQueryExecutorBase.Source>(leftTemplate.Sources, StringComparer.OrdinalIgnoreCase);
            sources.EnsureCapacity(sources.Count + 1);

            var hasLeftOrdinalMetadata = leftTemplate.OrdinalValues is not null && leftTemplate.OrdinalIndexes is not null;
            var leftOrdinalCount = hasLeftOrdinalMetadata ? leftTemplate.OrdinalValues!.Length : 0;
            if (hasLeftOrdinalMetadata || rightOrdinalCount > 0)
            {
                ordinalValues = new object?[leftOrdinalCount + rightOrdinalCount];
                ordinalIndexes = hasLeftOrdinalMetadata
                    ? new Dictionary<string, int>(leftTemplate.OrdinalIndexes!.Count + rightOrdinalCount * 3, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, int>(Math.Max(1, rightOrdinalCount * 3), StringComparer.OrdinalIgnoreCase);

                if (hasLeftOrdinalMetadata)
                {
                    foreach (var ordinal in leftTemplate.OrdinalIndexes!)
                        ordinalIndexes[ordinal.Key] = ordinal.Value;
                }

                if (rightOrdinalCount > 0)
                {
                    ordinalIndexes.EnsureCapacity(ordinalIndexes.Count + rightOrdinalCount * 3);
                    PopulateSourceColumns(rightSource, rightFields, fields, ordinalValues, ordinalIndexes, leftOrdinalCount, nullValue: null);
                }
            }
        }
        else
        {
            fields = rightFields;
            fields.EnsureCapacity(fields.Count + rightOrdinalCount);
            sources = new Dictionary<string, AstQueryExecutorBase.Source>(1, StringComparer.OrdinalIgnoreCase);
            ordinalValues = new object?[rightSource.ColumnNames.Count];
            ordinalIndexes = new Dictionary<string, int>(Math.Max(1, rightSource.ColumnNames.Count * 3), StringComparer.OrdinalIgnoreCase);
            PopulateSourceColumns(rightSource, rightFields, fields, ordinalValues, ordinalIndexes, 0, nullValue: null);
        }

        sources[rightSource.Alias] = rightSource;

        return new AstQueryExecutorBase.EvalRow(fields, sources)
        {
            OrdinalValues = ordinalValues,
            OrdinalIndexes = ordinalIndexes
        };
    }

    private static List<AstQueryExecutorBase.EvalRow> MaterializeRows(IEnumerable<AstQueryExecutorBase.EvalRow> rows)
    {
        if (rows is List<AstQueryExecutorBase.EvalRow> list)
            return list;

        if (rows is ICollection<AstQueryExecutorBase.EvalRow> collection)
            return new List<AstQueryExecutorBase.EvalRow>(collection);

        return [.. rows];
    }

    private static List<Dictionary<string, object?>> MaterializeFields(IEnumerable<Dictionary<string, object?>> rows)
    {
        if (rows is List<Dictionary<string, object?>> list)
            return list;

        if (rows is ICollection<Dictionary<string, object?>> collection)
            return new List<Dictionary<string, object?>>(collection);

        return [.. rows];
    }

    private static void PopulateSourceColumns(
        AstQueryExecutorBase.Source source,
        Dictionary<string, object?> sourceFields,
        Dictionary<string, object?> targetFields,
        object?[] ordinalValues,
        Dictionary<string, int> ordinalIndexes,
        int ordinalOffset,
        object? nullValue)
    {
        for (var i = 0; i < source.ColumnNames.Count; i++)
        {
            var columnName = source.ColumnNames[i];
            var qualifiedName = source.GetQualifiedColumnName(i);
            var ordinalIndex = ordinalOffset + i;
            var value = sourceFields.TryGetValue(qualifiedName, out var current)
                ? current
                : nullValue;

            targetFields[qualifiedName] = value;
            targetFields.TryAdd(columnName, value);

            ordinalValues[ordinalIndex] = value;
            ordinalIndexes.TryAdd(qualifiedName, ordinalIndex);
            ordinalIndexes.TryAdd(columnName, ordinalIndex);
            if (source.TryGetSourceQualifiedColumnName(i, out var sourceQualifiedName))
                ordinalIndexes.TryAdd(sourceQualifiedName, ordinalIndex);
        }
    }
}
