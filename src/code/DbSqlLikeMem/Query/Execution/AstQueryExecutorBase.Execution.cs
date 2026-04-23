namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    public TableResultMock ExecuteUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy = null,
        SqlRowLimit? rowLimit = null,
        string? sqlContextForErrors = null)
    {
        ClearSubqueryEvaluationCaches();
        return _context.ExecuteUnion(
            parts,
            allFlags,
            orderBy,
            rowLimit,
            sqlContextForErrors,
            parts1 => ExecuteSelect(parts1, null, null),
            (result, query, ctes, trace) => context.ApplyQueryOrderLimit(
                result,
                query,
                ctes,
                ParseExpr,
                (expr, row) => Eval(expr, row, group: null, ctes),
                (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
                trace),
            AstQueryPlanMetricsHelper.CountKnownInputTables
            );
    }

    /// <summary>
    /// EN: Implements ExecuteSelect.
    /// PT: Implementa ExecuteSelect.
    /// </summary>
    public TableResultMock ExecuteSelect(SqlSelectQuery q)
    {
        var sw = Stopwatch.StartNew();
        ClearSubqueryEvaluationCaches();
        QueryDebugTraceBuilder? debugTrace = Cnn.IsDebugTraceCaptureEnabled
            ? new QueryDebugTraceBuilder(SqlConst.SELECT)
            : null;
        var hasSqlCalcFoundRows = HasSqlCalcFoundRows(q);
        var result = ExecuteSelect(q, null, null, debugTrace);
        sw.Stop();

        if (!hasSqlCalcFoundRows)
            Cnn.SetLastSelectRows(result.Count);

        var metrics = _context.BuildPlanRuntimeMetrics(q, result.Count, sw.ElapsedMilliseconds);
        var indexRecommendations = BuildIndexRecommendations(_context, q, metrics);
        var planWarnings = QueryPlanWarningHelper.BuildPlanWarnings(q, metrics);
        var runtimeContext = _context.BuildPlanRuntimeContext();
        if (Cnn.Db.CaptureExecutionPlans)
        {
            var plan = SqlExecutionPlanFormatter.FormatSelect(
                q,
                metrics,
                indexRecommendations,
                planWarnings,
                runtimeContext: runtimeContext);
            result.ExecutionPlan = plan;
            Cnn.RegisterExecutionPlan(plan);
        }
        if (debugTrace is not null)
            Cnn.RegisterDebugTrace(debugTrace.Build());
        return result;
    }

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

        if (TryEvaluateSimpleUnionCount(selectQuery, ctes, outerRow, out var fastCountResult))
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
                (int)Math.Min(int.MaxValue, _context.GetKnownSourceRows(selectQuery.Table)),
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
                    $"Join({AstQuerySelectExecutionHelper.FormatJoinTypeForDebug(j.Type)})",
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

        var needsGrouping = selectQuery.GroupBy.Count > 0
            || selectQuery.Having is not null
            || AstQueryAggregateAnalysisHelper.ContainsAggregate(
                selectQuery,
                ParseScalarExpr,
                AggregateExpressionInspector.WalkHasAggregate);
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
            projected = _context.ApplyDistinct(projected);
            debugTrace?.AddStep(
                "Distinct",
                inputRows,
                projected.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(selectQuery.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(selectQuery))
            Cnn.SetLastSelectRows(projected.Count);

        projected = context.ApplyQueryOrderLimit(
            projected,
            selectQuery,
            ctes,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
            debugTrace);
        projected = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(projected, selectQuery, debugTrace);
        return projected;
    }

    private bool TryEvaluateSimpleUnionCount(
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

        var (exprRaw, _) = SelectAliasParserHelper.SplitTrailingAsAlias(query.SelectItems[0].Raw, query.SelectItems[0].Alias);
        if (!AstQueryAggregateEvaluator.TryParseScalarCountAggregate(exprRaw, ParseExpr, out var countArg, out var isCountBig) || countArg is not StarExpr)
            return false;

        var union = query.Table.DerivedUnion;
        if (union.RowLimit is not null
            || union.Parts.Count != 2
            || union.AllFlags.Count != 1)
            return false;

        if (union.AllFlags[0])
        {
            long allCount = 0;
            foreach (var part in union.Parts)
            {
                if (!TryCountSimpleRows(part, ctes, outerRow, out var partCount))
                    return false;

                allCount += partCount;
            }

            return CreateSimpleUnionCountResult(query, exprRaw, isCountBig, allCount, out result);
        }

        if (!TryCountSimpleRows(union.Parts[0], ctes, outerRow, out _)
            || !TryCountSimpleRows(union.Parts[1], ctes, outerRow, out _))
            return false;

        var leftRows = ExecuteSelect(union.Parts[0], ctes, outerRow);
        var rightRows = ExecuteSelect(union.Parts[1], ctes, outerRow);
        if (leftRows.Columns.Count != rightRows.Columns.Count)
            return false;

        var seenRows = new HashSet<Dictionary<int, object?>>(new SqlRowDictionaryComparer(context));
        long distinctCount = 0;
        for (var i = 0; i < leftRows.Count; i++)
        {
            if (seenRows.Add(leftRows[i]))
                distinctCount++;
        }

        for (var i = 0; i < rightRows.Count; i++)
        {
            if (seenRows.Add(rightRows[i]))
                distinctCount++;
        }

        return CreateSimpleUnionCountResult(query, exprRaw, isCountBig, distinctCount, out result);
    }

    private bool CreateSimpleUnionCountResult(
        SqlSelectQuery query,
        string exprRaw,
        bool isCountBig,
        long count,
        out TableResultMock result)
    {
        var tableAlias = query.Table?.Alias ?? query.Table?.TableFunction?.Name ?? query.Table?.Name ?? string.Empty;
        var columnAlias = SelectPlanProjectionHelper.InferColumnAlias(exprRaw);
        var countValue = AstQueryAggregateEvaluator.CreateCountAggregateResult(context, isCountBig, count);
        result = new TableResultMock
        {
            Columns =
            [
                SelectPlanProjectionHelper.CreateSelectPlanColumn(
                    tableAlias,
                    columnAlias,
                    0,
                    countValue is int ? DbType.Int32 : DbType.Int64,
                    isNullable: false)
            ]
        };
        result.Add(new Dictionary<int, object?> { [0] = countValue });
        result.JoinFields.Add(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        if (query.OrderBy.Count > 0 || query.RowLimit is not null)
        {
            var orderCtes = new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase);
            result = context.ApplyQueryOrderLimit(
                result,
                query,
                orderCtes,
                ParseExpr,
                (expr, row) => Eval(expr, row, group: null, orderCtes),
                (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture));
        }

        return true;
    }

}
