namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
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
            return false;

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!AstQueryAggregateEvaluator.TryParseStringAggregateCall(exprRaw, ParseScalarExpr, out var aggregateCall))
            return false;

        var aggregateDefinition = aggregateCall.ResolvedScalarFunction;
        if (aggregateDefinition is null
            && !context.Dialect.TryGetScalarFunctionDefinition(aggregateCall, out aggregateDefinition))
            return false;

        if (aggregateDefinition is not null
            && !aggregateDefinition.AllowsCall)
            return false;

        if (aggregateDefinition is null)
            return false;

        if (aggregateCall.Distinct)
            return false;

        var firstRow = rows.Count > 0 ? rows[0] : EvalRow.Empty();
        var aggregateGroup = new EvalGroup(rows);
        object? resultValue = context.EvalAggregate(aggregateCall, aggregateGroup, ctes, Eval);

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
            Cnn.SetLastSelectRows(result.Count);

        if (query.DistinctOn.Count > 0)
            return false;

        if (query.Distinct)
            result = _context.ApplyDistinct(result);

        result = context.ApplyQueryOrderLimit(
            result,
            query,
            ctes,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture));
        result = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(result, query);
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
            || query.HasDistinctClause()
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.RowLimit is not null
            || query.ForJson is not null)
            return false;

        if (outerRow is null && query.Where is null)
        {
            if (query.Table is null)
            {
                count = 1;
                return true;
            }

            if (AstQueryPlanMetricsHelper.HasKnownPhysicalTable(query.Table))
            {
                count = _context.GetKnownSourceRows(query.Table);
                return true;
            }
        }

        if (outerRow is null
            && query.Table is not null
            && query.Where is not null
            && TryCountRowsFromPrimaryKey(query, ctes, out count))
            return true;

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
        if (!IndexHelper.TryCountRowsFromIndex(src, query.Table, query.Where, hasOrderBy: query.OrderBy.Count > 0, hasGroupBy: false, out count))
            return false;

        return true;
    }

    private TableResultMock ExecuteGroup(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows,
        QueryDebugTraceBuilder? debugTrace)
    {
        var sourceRows = rows as List<EvalRow> ?? [.. rows];
        var keyExprs = AstQuerySelectGroupKeyHelper.BuildGroupByKeyExpressions(q, ParseExpr);

        GroupKey BuildGroupKey(EvalRow row)
        {
            var values = keyExprs.Length > 0 ? OrdinalPool.Rent(keyExprs.Length) : [];
            for (var i = 0; i < keyExprs.Length; i++)
                values[i] = Eval(keyExprs[i], row, group: null, ctes);

            var key = new GroupKey(values);
            OrdinalPool.Return(values);
            return key;
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
            var (exprRaw, alias) = SelectAliasParserHelper.SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
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

    private TableResultMock ExecuteGroupCore(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        IEnumerable<EvalRow> rows)
    {
        var keyExprs = AstQuerySelectGroupKeyHelper.BuildGroupByKeyExpressions(q, ParseExpr);

        GroupKey BuildGroupKey(EvalRow row)
        {
            var values = keyExprs.Length > 0 ? OrdinalPool.Rent(keyExprs.Length) : [];
            for (var i = 0; i < keyExprs.Length; i++)
                values[i] = Eval(keyExprs[i], row, group: null, ctes);

            var key = new GroupKey(values);
            OrdinalPool.Return(values);
            return key;
        }

        var grouped = MaterializeGroups(rows.GroupBy(
            BuildGroupKey,
            new GroupKey.GroupKeyComparer(context)));

        if (q.Having is null)
            return ProjectGrouped(q, grouped, ctes, debugTrace: null);

        var aliasExprs = new List<(string Alias, SqlExpr Ast)>(q.SelectItems.Count);
        for (var i = 0; i < q.SelectItems.Count; i++)
        {
            var selectItem = q.SelectItems[i];
            var (exprRaw, alias) = SelectAliasParserHelper.SplitTrailingAsAlias(selectItem.Raw, selectItem.Alias);
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
        grouped = ApplyHavingPredicate(grouped, havingExpr, aliasExprs, ctes);
        return ProjectGrouped(q, grouped, ctes, debugTrace: null);
    }

    private IEnumerable<EvalRow> ApplyRowPredicate(
        IEnumerable<EvalRow> rows,
        SqlExpr predicate,
        IDictionary<string, Source> ctes)
    {
        var compiled = CompilePredicate(predicate);
        if (compiled is not null)
        {
            foreach (var row in rows)
            {
                if (compiled(row))
                    yield return row;
            }
        }
        else
        {
            foreach (var row in rows)
            {
                if (Eval(predicate, row, group: null, ctes).ToBool())
                    yield return row;
            }
        }
    }

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
        {
            var firstEvalCtx = BuildHavingEvaluationContext(firstGroup, aliasExprs, ctes, out var firstEvalGroup);
            HavingHelper.EnsureHavingIdentifiersAreBound(havingExpr, firstEvalCtx, context.Dialect!);
            if (Eval(havingExpr, firstEvalCtx, firstEvalGroup, ctes).ToBool())
                filtered.Add(firstGroup);
        }

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

        // Copy fields from the first row of the group. Do NOT return first.Fields to the pool
        // because 'first' is still owned by 'grouped.Rows' and is accessed later
        // (e.g., via res.JoinFields.Add(first.Fields) in ProjectGrouped).
        var fields = SqlRowPool.Get(first.Fields.Count + aliasExprs.Count);
        foreach (var kvp in first.Fields)
            fields[kvp.Key] = kvp.Value;

        var sources = new Dictionary<string, Source>(first.Sources, StringComparer.OrdinalIgnoreCase);
        sources.EnsureCapacity(first.Sources.Count);

        var baseOrdinalValues = first.OrdinalValues is null ? [] : first.OrdinalValues;
        // Array flows into EvalRow → TableResultMock; not returned to pool.
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
}

