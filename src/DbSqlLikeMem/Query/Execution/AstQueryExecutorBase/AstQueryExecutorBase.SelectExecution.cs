namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private TableResultMock ExecuteSelect(
        SqlSelectQuery selectQuery,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(selectQuery, nameof(selectQuery));

        var ctes = inheritedCtes is null
            ? new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, Source>(inheritedCtes, StringComparer.OrdinalIgnoreCase);

        foreach (var cte in selectQuery.Ctes)
        {
            var cteStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var res = cte.Query switch
            {
                SqlSelectQuery cteSelect => ExecuteSelect(cteSelect, ctes, outerRow),
                SqlUnionQuery cteUnion => ExecuteUnion(
                    cteUnion.Parts,
                    cteUnion.AllFlags,
                    cteUnion.OrderBy,
                    cteUnion.RowLimit,
                    cteUnion.RawSql),
                _ => throw new NotSupportedException($"CTE query type '{cte.Query.GetType().Name}' is not supported.")
            };
            ctes[cte.Name] = Source.FromResult(cte.Name, res);
            debugTrace?.AddStep(
                "CteMaterialize",
                0,
                res.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(cteStart)),
                cte.Name);
        }

        if (TryEvaluateSimpleUnionAllCount(selectQuery, ctes, outerRow, out var fastCountResult))
            return fastCountResult;

        var fromStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var rows = BuildFrom(
            selectQuery.Table,
            ctes,
            selectQuery.Where,
            hasOrderBy: selectQuery.OrderBy.Count > 0,
            hasGroupBy: selectQuery.GroupBy.Count > 0);
        if (debugTrace is not null)
        {
            var fromRows = rows as List<EvalRow> ?? [.. rows];
            debugTrace.AddStep(
                "TableScan",
                (int)Math.Min(int.MaxValue, AstQueryPlanMetricsHelper.GetKnownSourceRows(_context, selectQuery.Table)),
                fromRows.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(fromStart)),
                SqlSourceFormattingHelper.FormatSource(selectQuery.Table));
            rows = fromRows;
        }

        foreach (var j in selectQuery.Joins)
        {
            var joinStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = debugTrace is not null
                ? (rows as ICollection<EvalRow>)?.Count ?? rows.Count()
                : 0;
            rows = ApplyJoin(
                rows,
                j,
                ctes,
                hasOrderBy: selectQuery.OrderBy.Count > 0,
                hasGroupBy: selectQuery.GroupBy.Count > 0);
            if (debugTrace is not null)
            {
                var joinedRows = rows as List<EvalRow> ?? [.. rows];
                debugTrace.AddStep(
                    $"Join({FormatJoinTypeForDebug(j.Type)})",
                    inputRows,
                    joinedRows.Count,
                    TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(joinStart)),
                    SqlSourceFormattingHelper.FormatJoinDebugDetails(j));
                rows = joinedRows;
            }
        }

        if (outerRow is not null)
            rows = AttachOuterRows(rows, outerRow);

        if (selectQuery.Where is not null)
        {
            var filterStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = debugTrace is not null
                ? (rows as ICollection<EvalRow>)?.Count ?? rows.Count()
                : 0;
            rows = ApplyRowPredicate(rows, selectQuery.Where, ctes);
            if (debugTrace is not null)
            {
                var filteredRows = rows as List<EvalRow> ?? [.. rows];
                debugTrace.AddStep(
                    "Filter",
                    inputRows,
                    filteredRows.Count,
                    TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(filterStart)),
                    SqlExprPrinter.Print(selectQuery.Where));
                rows = filteredRows;
            }
        }

        var needsGrouping = selectQuery.GroupBy.Count > 0 || selectQuery.Having is not null || ContainsAggregate(selectQuery);
        if (needsGrouping)
        {
            var groupedRows = rows as List<EvalRow> ?? [.. rows];
            if (debugTrace is null && TryEvaluateSimpleStringAggregate(selectQuery, groupedRows, ctes, out var fastStringAggregateResult))
                return fastStringAggregateResult;

            return ExecuteGroup(selectQuery, ctes, groupedRows, debugTrace);
        }

        var projectedRows = rows as List<EvalRow> ?? [.. rows];
        var projectStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var projected = ProjectRows(selectQuery, projectedRows, ctes);
        debugTrace?.AddStep(
            "Project",
            projectedRows.Count,
            projected.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(selectQuery.SelectItems));

        if (selectQuery.Distinct)
        {
            var distinctStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = projected.Count;
            projected = ApplyDistinct(projected, _context);
            debugTrace?.AddStep(
                "Distinct",
                inputRows,
                projected.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(selectQuery.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(selectQuery))
            _cnn.SetLastFoundRows(projected.Count);

        projected = ApplyOrderAndLimit(projected, selectQuery, ctes, debugTrace);
        projected = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(projected, selectQuery, debugTrace);
        return projected;
    }

    private bool TryEvaluateSimpleUnionAllCount(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow,
        out TableResultMock result)
    {
        result = null!;

        if (query.Table?.DerivedUnion is null
            || query.Joins.Count > 0
            || query.Where is not null
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.ForJson is not null
            || query.SelectItems.Count != 1)
            return false;

        var (exprRaw, _) = SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!TryParseScalarCountAggregate(exprRaw, out var countArg) || countArg is not StarExpr)
            return false;

        var union = query.Table.DerivedUnion;
        if (union.RowLimit is not null
            || ContainsDistinctUnionFlag(union.AllFlags))
            return false;

        long count = 0;
        foreach (var part in union.Parts)
        {
            if (!TryCountSimpleRows(part, ctes, outerRow, out var partCount))
                return false;

            count += partCount;
        }

        var tableAlias = query.Table?.Alias ?? query.Table?.TableFunction?.Name ?? query.Table?.Name ?? string.Empty;
        var columnAlias = SelectPlanProjectionHelper.InferColumnAlias(exprRaw);
        result = new TableResultMock
        {
            Columns =
            [
                SelectPlanProjectionHelper.CreateSelectPlanColumn(
                    tableAlias,
                    columnAlias,
                    0,
                    DbType.Int64,
                    isNullable: false)
            ]
        };
        result.Add(new Dictionary<int, object?> { [0] = count });
        result.JoinFields.Add(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        if (query.OrderBy.Count > 0 || query.RowLimit is not null)
        {
            var orderCtes = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase);
            result = ApplyOrderAndLimit(result, query, orderCtes);
        }

        return true;
    }

    private static bool ContainsDistinctUnionFlag(IReadOnlyList<bool> allFlags)
    {
        for (var i = 0; i < allFlags.Count; i++)
        {
            if (!allFlags[i])
                return true;
        }

        return false;
    }

    private bool TryEvaluateSimpleStringAggregate(
        SqlSelectQuery query,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes,
        out TableResultMock result)
    {
        result = null!;

        if (query.GroupBy.Count > 0
            || query.Having is not null
            || query.SelectItems.Count != 1)
        {
            return false;
        }

        var (exprRaw, _) = SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!TryParseStringAggregateCall(exprRaw, out var aggregateCall))
            return false;

        var dialect2 = Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para agregação.");
        var aggregateDefinition = aggregateCall.ResolvedScalarFunction;
        if (aggregateDefinition is null
            && !dialect2.TryGetScalarFunctionDefinition(aggregateCall, out aggregateDefinition))
        {
            return false;
        }

        if (aggregateDefinition is not null
            && !aggregateDefinition.AllowsCall)
        {
            return false;
        }

        if (aggregateDefinition is null)
            return false;

        if (aggregateCall.Distinct)
            return false;

        var firstRow = rows.Count > 0 ? rows[0] : EvalRow.Empty();
        var aggregateGroup = new EvalGroup(rows);
        var resultValue = EvalStringAggregateForCallExpr(aggregateCall, aggregateGroup, ctes, aggregateCall.Name);

        result = new TableResultMock
        {
            Columns =
            [
                SelectPlanProjectionHelper.CreateSelectPlanColumn(
                    query.Table?.Alias ?? query.Table?.TableFunction?.Name ?? query.Table?.Name ?? string.Empty,
                    SelectPlanProjectionHelper.InferColumnAlias(exprRaw),
                    0,
                    DbType.String,
                    isNullable: true)
            ]
        };
        result.Add(new Dictionary<int, object?> { [0] = resultValue });
        result.JoinFields.Add(firstRow.Fields);

        if (HasSqlCalcFoundRows(query))
            _cnn.SetLastFoundRows(result.Count);

        if (query.Distinct)
            result = ApplyDistinct(result, _context);

        result = ApplyOrderAndLimit(result, query, ctes);
        result = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(result, query);
        return true;
    }

    private bool TryParseStringAggregateCall(string exprRaw, out CallExpr call)
    {
        call = null!;

        SqlExpr expr;
        try
        {
            expr = ParseScalarExpr(exprRaw);
        }
        catch
        {
            return false;
        }

        if (expr is not CallExpr parsedCall)
            return false;

        if (parsedCall.Name is not ("GROUP_CONCAT" or "STRING_AGG" or "LISTAGG"))
            return false;

        if (parsedCall.WithinGroupOrderBy is not null)
            return false;

        call = parsedCall;
        return true;
    }

    private bool TryCountSimpleRows(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        EvalRow? outerRow,
        out long count)
    {
        count = 0;

        if (query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.Distinct
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null)
        {
            return false;
        }

        if (outerRow is null && query.Where is null)
        {
            if (query.Table is null)
            {
                count = 1;
                return true;
            }

            if (AstQueryPlanMetricsHelper.HasKnownPhysicalTable(query.Table))
            {
                count = AstQueryPlanMetricsHelper.GetKnownSourceRows(_context, query.Table);
                return true;
            }
        }

        if (outerRow is null
            && query.Table is not null
            && query.Where is not null
            && TryCountRowsFromPrimaryKey(query, ctes, out count))
        {
            return true;
        }

        var rows = BuildFrom(
            query.Table,
            ctes,
            query.Where,
            hasOrderBy: query.OrderBy.Count > 0,
            hasGroupBy: false);

        if (query.Where is null)
        {
            foreach (var _ in rows)
                count++;
            return true;
        }

        if (outerRow is not null)
        {
            foreach (var candidate in rows)
            {
                if (Eval(query.Where, AttachOuterRow(candidate, outerRow), group: null, ctes).ToBool())
                    count++;
            }

            return true;
        }

        foreach (var candidate in rows)
        {
            if (Eval(query.Where, candidate, group: null, ctes).ToBool())
                count++;
        }

        return true;
    }

    private bool TryCountRowsFromPrimaryKey(
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        out long count)
    {
        count = 0;

        var src = ResolveSource(query.Table!, ctes);
        if (src.Physical is not TableMock tableMock)
            return false;

        var primaryKeyIndexes = tableMock.PrimaryKeyIndexes;
        if (primaryKeyIndexes.Count == 0)
            return false;

        var hintPlan = AstQueryIndexHelper.BuildMySqlIndexHintPlan(query.Table!.MySqlIndexHints, src.Physical, hasOrderBy: query.OrderBy.Count > 0, hasGroupBy: false);
        if (hintPlan?.MissingForcedIndexes.Count > 0)
            throw new InvalidOperationException($"MySQL FORCE INDEX referencia índice inexistente: {string.Join(", ", hintPlan.MissingForcedIndexes)}.");

        if (!PartitionHelper.TryCollectColumnEqualities(query.Where!, src, out var equalsByColumn))
            return false;

        var pkValues = new Dictionary<int, object?>(primaryKeyIndexes.Count);
        foreach (var pkIdx in primaryKeyIndexes)
        {
            if (!tableMock.ColumnsByIndex.TryGetValue(pkIdx, out var pkColumnName))
                return false;

            var normalizedColumn = pkColumnName.NormalizeName();
            if (!equalsByColumn.TryGetValue(normalizedColumn, out var value))
                return false;

            pkValues[pkIdx] = value;
        }

        IndexHelper.RecordPrimaryKeyHintMetric(tableMock, hintPlan);
        if (_cnn.Metrics.Enabled)
            _cnn.Metrics.IndexLookups++;
        count = tableMock.TryFindRowByPk(pkValues, out _)
            ? 1
            : 0;
        return true;
    }

    private TableResultMock ExecuteGroup(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var sourceRows = rows as List<EvalRow> ?? [.. rows];
        var keyExprs = BuildGroupByKeyExpressions(q);

        GroupKey BuildGroupKey(EvalRow row)
        {
            var values = new object?[keyExprs.Length];
            for (var i = 0; i < keyExprs.Length; i++)
                values[i] = Eval(keyExprs[i], row, group: null, ctes);

            return new GroupKey(values);
        }

        var groupStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var grouped = MaterializeGroups(sourceRows.GroupBy(
            BuildGroupKey,
            new GroupKey.GroupKeyComparer(context)));
        debugTrace?.AddStep(
            "Group",
            sourceRows.Count,
            grouped.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(groupStart)),
            QueryDebugTraceFormattingHelper.FormatGroupDebugDetails(q));

        if (q.Having is null)
            return ProjectGrouped(q, grouped, ctes, debugTrace);

        var aliasExprs = new List<(string Alias, SqlExpr Ast)>(q.SelectItems.Count);
        for (var i = 0; i < q.SelectItems.Count; i++)
        {
            var selectItem = q.SelectItems[i];
            var (exprRaw, alias) = SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
            if (string.IsNullOrWhiteSpace(alias))
                continue;

            SqlExpr ast;
#pragma warning disable CA1031 // Do not catch general exception types
            try { ast = ParseExpr(exprRaw); }
            catch (Exception e)
            {
#pragma warning disable CA1303
                Console.WriteLine($"{GetType().Name}.{nameof(ExecuteSelect)}");
#pragma warning restore CA1303
                Console.WriteLine(e);
                ast = new RawSqlExpr(exprRaw);
            }
#pragma warning restore CA1031

            aliasExprs.Add((alias!, ast));
        }

        var havingExpr = HavingHelper.NormalizeHavingExpression(q.Having, q);

        var havingStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var inputGroups = grouped.Count;
        grouped = ApplyHavingPredicate(grouped, havingExpr, aliasExprs, ctes);
        debugTrace?.AddStep(
            "Having",
            inputGroups,
            grouped.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(havingStart)),
            SqlExprPrinter.Print(q.Having));

        return ProjectGrouped(q, grouped, ctes, debugTrace);
    }

    private IEnumerable<EvalRow> ApplyRowPredicate(
        IEnumerable<EvalRow> rows,
        SqlExpr predicate,
        IDictionary<string, Source> ctes)
        => rows.Where(r => Eval(predicate, r, group: null, ctes).ToBool());

    private List<MaterializedGroup> ApplyHavingPredicate(
        IReadOnlyList<MaterializedGroup> grouped,
        SqlExpr havingExpr,
        IReadOnlyList<(string Alias, SqlExpr Ast)> aliasExprs,
        IDictionary<string, Source> ctes)
    {
        if (grouped.Count == 0)
            return [];

        var filtered = new List<MaterializedGroup>(grouped.Count);

        var firstGroup = grouped[0];
        var firstEvalCtx = BuildHavingEvaluationContext(firstGroup, aliasExprs, ctes, out var firstEvalGroup);
        HavingHelper.EnsureHavingIdentifiersAreBound(havingExpr, firstEvalCtx, Dialect!);
        if (Eval(havingExpr, firstEvalCtx, firstEvalGroup, ctes).ToBool())
            filtered.Add(firstGroup);

        for (var i = 1; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var evalCtx = BuildHavingEvaluationContext(group, aliasExprs, ctes, out var evalGroup);
            if (Eval(havingExpr, evalCtx, evalGroup, ctes).ToBool())
                filtered.Add(group);
        }

        return filtered;
    }

    private EvalRow BuildHavingEvaluationContext(
        MaterializedGroup grouped,
        IReadOnlyList<(string Alias, SqlExpr Ast)> aliasExprs,
        IDictionary<string, Source> ctes,
        out EvalGroup evalGroup)
    {
        var rows = grouped.Rows;
        evalGroup = new EvalGroup(rows);
        var first = rows[0];

        var fields = new Dictionary<string, object?>(first.Fields, StringComparer.OrdinalIgnoreCase);
        fields.EnsureCapacity(first.Fields.Count + aliasExprs.Count);

        var sources = new Dictionary<string, Source>(first.Sources, StringComparer.OrdinalIgnoreCase);
        sources.EnsureCapacity(first.Sources.Count);

        var baseOrdinalValues = first.OrdinalValues is null ? [] : first.OrdinalValues;
        var ordinalValues = new object?[baseOrdinalValues.Length + aliasExprs.Count];
        if (baseOrdinalValues.Length > 0)
            Array.Copy(baseOrdinalValues, ordinalValues, baseOrdinalValues.Length);

        var ordinalIndexes = first.OrdinalIndexes is null
            ? new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, int>(first.OrdinalIndexes, StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < aliasExprs.Count; i++)
        {
            var (alias, ast) = aliasExprs[i];
            var value = Eval(ast, first, evalGroup, ctes);
            fields[alias] = value;

            var ordinalIndex = baseOrdinalValues.Length + i;
            ordinalValues[ordinalIndex] = value;
            ordinalIndexes[alias] = ordinalIndex;
        }

        return new EvalRow(fields, sources)
        {
            OrdinalValues = ordinalValues,
            OrdinalIndexes = ordinalIndexes
        };
    }

    private SqlExpr[] BuildGroupByKeyExpressions(SqlSelectQuery q)
    {
        var keyExprs = new List<SqlExpr>(q.GroupBy.Count);

        foreach (var groupByRaw in q.GroupBy)
        {
            var raw = groupByRaw.Trim();
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ord))
            {
                if (ord < 1)
                    throw new InvalidOperationException("invalid: GROUP BY ordinal must be >= 1");

                var idx = ord - 1;
                if (idx >= q.SelectItems.Count)
                    throw new InvalidOperationException($"invalid: GROUP BY ordinal {ord} out of range");

                var selectItem = q.SelectItems[idx];
                var (exprRaw, _) = SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
                keyExprs.Add(ParseExpr(exprRaw));
                continue;
            }

            keyExprs.Add(ParseExpr(groupByRaw));
        }

        return [.. keyExprs];
    }

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
            yield return CreateSourceEvalRow(src, r);
    }

    private void TryRecordPrimaryKeyHintMetric(
        ITableMock table,
        MySqlIndexHintPlan? hintPlan)
    {
        if (hintPlan is null || !_cnn.Metrics.Enabled)
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
            _cnn.Metrics.IncrementIndexHint(hintedPrimaryEquivalent!);
    }
}
