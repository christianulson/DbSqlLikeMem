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
        return ExecuteUnion(
            new SqlUnionQuery(parts, allFlags, orderBy ?? [], rowLimit)
            {
                RawSql = sqlContextForErrors ?? string.Empty
            },
            inheritedCtes: null,
            outerRow: null);
    }

    private TableResultMock ExecuteUnion(
        SqlUnionQuery query,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow)
        => _context.ExecuteUnion(
            query.Parts,
            query.AllFlags,
            query.OrderBy,
            query.RowLimit,
            query.RawSql,
            parts1 => ExecuteSelect(parts1, inheritedCtes, outerRow),
            (result, selectQuery, ctes, trace) => context.ApplyQueryOrderLimit(
                result,
                selectQuery,
                ctes,
                ParseExpr,
                (expr, row) => Eval(expr, row, group: null, ctes),
                (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
                trace),
            AstQueryPlanMetricsHelper.CountKnownInputTables
            );

    private TableResultMock ExecuteQuery(
        SqlQueryBase query,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow)
        => query switch
        {
            SqlSelectQuery select => ExecuteSelect(select, inheritedCtes, outerRow),
            SqlUnionQuery union => ExecuteUnion(union, inheritedCtes, outerRow),
            _ => throw new NotSupportedException($"Subquery query type '{query.GetType().Name}' is not supported.")
        };

    /// <summary>
    /// EN: Implements ExecuteSelect.
    /// PT-br: Implementa ExecuteSelect.
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

        return debugTrace is not null
            ? ExecuteSelectWithDebugTrace(selectQuery, inheritedCtes, outerRow, debugTrace)
            : ExecuteSelectCore(selectQuery, inheritedCtes, outerRow);
    }

    private TableResultMock ExecuteSelectCore(
        SqlSelectQuery selectQuery,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow)
    {
        var ctes = inheritedCtes is null
            ? new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, Source>(inheritedCtes, StringComparer.OrdinalIgnoreCase);

        foreach (var cte in selectQuery.Ctes)
        {
            var res = ExecuteCte(cte, ctes, outerRow);
            ctes[cte.Name] = Source.FromResult(cte.Name, res);
        }

        if (TryEvaluateSimpleUnionCount(selectQuery, ctes, outerRow, out var fastCountResult))
            return fastCountResult;

        //if (TryExecuteSelectDirect(selectQuery, ctes, out var directResult))
        //    return directResult;

        if (TryExecuteFastSelectPath(selectQuery, ctes, outerRow, out var fastResult))
            return fastResult;

        var rows = BuildFrom(
            selectQuery.Table,
            ctes,
            selectQuery.Where,
            hasOrderBy: selectQuery.OrderBy.Count > 0,
            hasGroupBy: selectQuery.GroupBy.Count > 0);

        foreach (var j in selectQuery.Joins)
            rows = ApplyJoin(rows, j, ctes, hasOrderBy: selectQuery.OrderBy.Count > 0, hasGroupBy: selectQuery.GroupBy.Count > 0);

        if (outerRow is not null)
            rows = AttachOuterRows(rows, outerRow);

        if (selectQuery.Where is not null)
            rows = ApplyRowPredicate(rows, selectQuery.Where, ctes);

        var needsGrouping = selectQuery.GroupBy.Count > 0
            || selectQuery.Having is not null
            || AstQueryAggregateAnalysisHelper.ContainsAggregate(
                selectQuery,
                ParseScalarExpr,
                AggregateExpressionInspector.WalkHasAggregate);
        if (needsGrouping)
        {
            // Fast path: avoid materialization when rows is already a List.
            if (rows is List<EvalRow> groupedRows)
            {
                if (TryEvaluateSimpleStringAggregate(selectQuery, groupedRows, ctes, out var fastStringAggregateResult))
                    return fastStringAggregateResult;
            }

            return ExecuteGroupCore(selectQuery, ctes, rows);
        }

        var projectedRows = rows as List<EvalRow> ?? [.. rows];
        var projected = ProjectRows(selectQuery, projectedRows, ctes);

        if (selectQuery.DistinctOn.Count > 0)
        {
            if (selectQuery.OrderBy.Count > 0)
                _context.TryApplyOrder(projected, selectQuery.OrderBy, ParseExpr, (expr, row) => Eval(expr, row, group: null, ctes));

            projected = _context.ApplyDistinctOn(projected, selectQuery.DistinctOn, ParseExpr, (expr, row) => Eval(expr, row, group: null, ctes));
        }
        else if (selectQuery.Distinct)
        {
            projected = _context.ApplyDistinct(projected);
        }

        if (HasSqlCalcFoundRows(selectQuery))
            Cnn.SetLastSelectRows(projected.Count);

        projected = context.ApplyQueryOrderLimit(projected, selectQuery, ctes, ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture));
        return AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(projected, selectQuery);
    }

    private TableResultMock ExecuteSelectWithDebugTrace(
        SqlSelectQuery selectQuery,
        IDictionary<string, Source>? inheritedCtes,
        EvalRow? outerRow,
        QueryDebugTraceBuilder debugTrace)
    {
        var ctes = inheritedCtes is null
            ? new Dictionary<string, Source>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, Source>(inheritedCtes, StringComparer.OrdinalIgnoreCase);

        foreach (var cte in selectQuery.Ctes)
        {
            var cteStart = Stopwatch.GetTimestamp();
            var res = ExecuteCte(cte, ctes, outerRow);
            ctes[cte.Name] = Source.FromResult(cte.Name, res);
            debugTrace.AddStep("CteMaterialize", 0, res.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(cteStart)), cte.Name);
        }

        if (TryEvaluateSimpleUnionCount(selectQuery, ctes, outerRow, out var fastCountResult))
            return fastCountResult;

        var fromStart = Stopwatch.GetTimestamp();
        var rows = BuildFrom(selectQuery.Table, ctes, selectQuery.Where,
            hasOrderBy: selectQuery.OrderBy.Count > 0, hasGroupBy: selectQuery.GroupBy.Count > 0);
        var fromRows = rows as List<EvalRow> ?? [.. rows];
        debugTrace.AddStep("TableScan",
            (int)Math.Min(int.MaxValue, _context.GetKnownSourceRows(selectQuery.Table)),
            fromRows.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(fromStart)),
            SqlSourceFormattingHelper.FormatSource(selectQuery.Table));
        rows = fromRows;

        foreach (var j in selectQuery.Joins)
        {
            var joinStart = Stopwatch.GetTimestamp();
            var inputRows = (rows as ICollection<EvalRow>)?.Count ?? rows.Count();
            rows = ApplyJoin(rows, j, ctes, hasOrderBy: selectQuery.OrderBy.Count > 0, hasGroupBy: selectQuery.GroupBy.Count > 0);
            var joinedRows = rows as List<EvalRow> ?? [.. rows];
            debugTrace.AddStep($"Join({AstQuerySelectExecutionHelper.FormatJoinTypeForDebug(j.Type)})",
                inputRows, joinedRows.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(joinStart)),
                SqlSourceFormattingHelper.FormatJoinDebugDetails(j));
            rows = joinedRows;
        }

        if (outerRow is not null)
            rows = AttachOuterRows(rows, outerRow);

        if (selectQuery.Where is not null)
        {
            var filterStart = Stopwatch.GetTimestamp();
            var inputRows = (rows as ICollection<EvalRow>)?.Count ?? rows.Count();
            rows = ApplyRowPredicate(rows, selectQuery.Where, ctes);
            var filteredRows = rows as List<EvalRow> ?? [.. rows];
            debugTrace.AddStep("Filter", inputRows, filteredRows.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(filterStart)),
                SqlExprPrinter.Print(selectQuery.Where));
            rows = filteredRows;
        }

        var needsGrouping = selectQuery.GroupBy.Count > 0
            || selectQuery.Having is not null
            || AstQueryAggregateAnalysisHelper.ContainsAggregate(selectQuery, ParseScalarExpr, AggregateExpressionInspector.WalkHasAggregate);
        if (needsGrouping)
        {
            var groupedRows = rows as List<EvalRow> ?? [.. rows];
            return ExecuteGroup(selectQuery, ctes, groupedRows, debugTrace);
        }

        var projectedRows = rows as List<EvalRow> ?? [.. rows];
        var projectStart = Stopwatch.GetTimestamp();
        var projected = ProjectRows(selectQuery, projectedRows, ctes);
        debugTrace.AddStep("Project", projectedRows.Count, projected.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(selectQuery.SelectItems));

        if (selectQuery.DistinctOn.Count > 0)
        {
            var distinctStart = Stopwatch.GetTimestamp();
            var inputRows = projected.Count;
            if (selectQuery.OrderBy.Count > 0)
                _context.TryApplyOrder(projected, selectQuery.OrderBy, ParseExpr, (expr, row) => Eval(expr, row, group: null, ctes));
            projected = _context.ApplyDistinctOn(projected, selectQuery.DistinctOn, ParseExpr, (expr, row) => Eval(expr, row, group: null, ctes));
            debugTrace.AddStep("Distinct On", inputRows, projected.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(selectQuery.DistinctOn.Count));
        }
        else if (selectQuery.Distinct)
        {
            var distinctStart = Stopwatch.GetTimestamp();
            var inputRows = projected.Count;
            projected = _context.ApplyDistinct(projected);
            debugTrace.AddStep("Distinct", inputRows, projected.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(selectQuery.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(selectQuery))
            Cnn.SetLastSelectRows(projected.Count);

        projected = context.ApplyQueryOrderLimit(projected, selectQuery, ctes, ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
            debugTrace);
        return AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(projected, selectQuery, debugTrace);
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

    private bool TryExecuteSelectDirect(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        out TableResultMock result)
    {
        result = null!;

        if (q.Joins.Count > 0
            || q.GroupBy.Count > 0
            || q.Having is not null
            || q.DistinctOn.Count > 0
            || q.Distinct
            || q.OrderBy.Count > 0
            || q.RowLimit is not null
            || q.ForJson is not null
            || q.Where is null
            || q.Table is null
            || q.Table.DerivedUnion is not null)
            return false;

        if (AstQueryAggregateAnalysisHelper.ContainsAggregate(q, ParseScalarExpr, AggregateExpressionInspector.WalkHasAggregate))
            return false;

        var src = ResolveSource(q.Table, ctes);
        if (src.Physical is not TableMock tableMock)
            return false;

        var rawItems = tableMock.Count;
        if (rawItems == 0)
        {
            var emptyPlan = _context.BuildSelectPlan(q, [], ctes, ParseScalarExpr, Eval, QueryRowValueHelper.ResolveColumn);
            result = new TableResultMock();
            for (int i = 0; i < emptyPlan.Columns.Count; i++)
                result.Columns.Add(emptyPlan.Columns[i]);
            return true;
        }

        var columnMapping = src.PhysicalColumnIndexes;
        if (columnMapping is null || columnMapping.Length == 0)
            return false;

        var fields = SqlRowPool.Get(src.ColumnNames.Count, StringComparer.OrdinalIgnoreCase);
        var ordinalValues = OrdinalPool.Rent(src.ColumnNames.Count);
        var evalRow = new EvalRow(fields, src.SourceDict)
        {
            OrdinalIndexes = src.SourceOrdinalIndexes,
            SingleSource = src.SourceDict.Count == 1 ? src : null
        };

        var sampleRow = tableMock.GetRawRow(0);
        for (var i = 0; i < src.ColumnNames.Count; i++)
        {
            var idx = columnMapping[i];
            var val = sampleRow[idx];
            var qualifiedName = src.GetQualifiedColumnName(i);
            fields[qualifiedName] = val;
            ordinalValues[i] = val;
        }
        evalRow.OrdinalValues = ordinalValues;

        var sampleRows = new List<EvalRow>(1) { evalRow };
        var parameterState = _context.SnapshotPositionalParameterState();
        SelectPlan selectPlan;
        try
        {
            selectPlan = _context.BuildSelectPlan(q, sampleRows, ctes, ParseScalarExpr, Eval, QueryRowValueHelper.ResolveColumn);
        }
        finally
        {
            _context.RestorePositionalParameterState(parameterState);
        }

        var hasWindowFunctions = selectPlan.HasWindowFunctions;
        if (hasWindowFunctions)
        {
            OrdinalPool.Return(ordinalValues);
            SqlRowPool.Return(fields);
            return false;
        }

        Func<EvalRow, bool>? compiledPredicate = null;
        if (q.Where is not null)
        {
            var predicateParamState = _context.SnapshotPositionalParameterState();
            try
            {
                compiledPredicate = CompilePredicate(q.Where);
            }
            finally
            {
                _context.RestorePositionalParameterState(predicateParamState);
            }
        }

        result = new TableResultMock();
        for (int i = 0; i < selectPlan.Columns.Count; i++)
            result.Columns.Add(selectPlan.Columns[i]);

        var projectedColumnCount = selectPlan.Evaluators.Count;

        for (var rowIdx = 0; rowIdx < rawItems; rowIdx++)
        {
            var rawRow = tableMock.GetRawRow(rowIdx);

            for (var i = 0; i < src.ColumnNames.Count; i++)
                ordinalValues[i] = rawRow[columnMapping[i]];

            evalRow.OrdinalValues = ordinalValues;

            if (compiledPredicate is not null && !compiledPredicate(evalRow))
                continue;

            var outRow = IntDictPool.Get(projectedColumnCount);
            for (int i = 0; i < projectedColumnCount; i++)
                outRow[i] = selectPlan.Evaluators[i](evalRow, null);

            result.Add(outRow);
            result.JoinFields.Add(fields);
        }

        OrdinalPool.Return(ordinalValues);
        return true;
    }

    private bool TryExecuteFastSelectPath(
        SqlSelectQuery q,
        Dictionary<string, Source> ctes,
        EvalRow? outerRow,
        out TableResultMock result)
    {
        result = null!;

        if (q.Joins.Count > 0
            || q.GroupBy.Count > 0
            || q.Having is not null
            || q.DistinctOn.Count > 0
            || q.Distinct
            || q.OrderBy.Count > 0
            || q.RowLimit is not null
            || q.ForJson is not null
            || q.Where is null
            || q.Table is null
            || q.Table.DerivedUnion is not null
            || outerRow is not null)
            return false;

        if (AstQueryAggregateAnalysisHelper.ContainsAggregate(q, ParseScalarExpr, AggregateExpressionInspector.WalkHasAggregate))
            return false;

        // Check PK early to avoid resolving source twice (caller also calls BuildFrom)
        if (q.Table?.Name is null)
            return false;

        if (!_context.Connection.TryGetTable(q.Table.Name, out var physicalTable, q.Table.DbName)
            || physicalTable is not TableMock tableMock)
            return false;

        var pkIndexes = tableMock.PkIndexArray;
        if (pkIndexes.Length == 0)
            return false;

        var src = ResolveSource(q.Table, ctes);
        if (src.Physical is null)
            return false;

        var parameterState = _context.SnapshotPositionalParameterState();
        try
        {
            if (!PartitionHelper.TryCollectColumnEqualities(q.Where, src, out var equalities))
                return false;

            for (var i = 0; i < pkIndexes.Length; i++)
            {
                var pkColName = tableMock.GetColumnByIndex(pkIndexes[i]).Name.NormalizeName();
                if (!equalities.ContainsKey(pkColName))
                    return false;
            }

            object?[] pkValues;
            switch (pkIndexes.Length)
            {
                case 1:
                {
                    var colName = tableMock.GetColumnByIndex(pkIndexes[0]).Name.NormalizeName();
                    if (!tableMock.TryFindRowByPkValues(equalities[colName], out var rowIdx))
                    {
                        result = new TableResultMock();
                        return true;
                    }
                    var fieldRows = src.RowsByIndexes(rowIdx);
                    var evalRows = new List<EvalRow>(1);
                    foreach (var fields in fieldRows)
                        evalRows.Add(AstQueryRowSourceHelper.CreateSourceEvalRow(src, fields));
                    result = ProjectRows(q, evalRows, ctes);
                    RecordFastPathMetrics(tableMock, q.Table);
                    return true;
                }
                case 2:
                {
                    var colName0 = tableMock.GetColumnByIndex(pkIndexes[0]).Name.NormalizeName();
                    var colName1 = tableMock.GetColumnByIndex(pkIndexes[1]).Name.NormalizeName();
                    if (!tableMock.TryFindRowByPkValues(equalities[colName0], equalities[colName1], out var rowIdx))
                    {
                        result = new TableResultMock();
                        return true;
                    }
                    var fieldRows2 = src.RowsByIndexes(rowIdx);
                    var evalRows2 = new List<EvalRow>(1);
                    foreach (var fields in fieldRows2)
                        evalRows2.Add(AstQueryRowSourceHelper.CreateSourceEvalRow(src, fields));
                    result = ProjectRows(q, evalRows2, ctes);
                    RecordFastPathMetrics(tableMock, q.Table);
                    return true;
                }
                case 3:
                {
                    var colName0 = tableMock.GetColumnByIndex(pkIndexes[0]).Name.NormalizeName();
                    var colName1 = tableMock.GetColumnByIndex(pkIndexes[1]).Name.NormalizeName();
                    var colName2 = tableMock.GetColumnByIndex(pkIndexes[2]).Name.NormalizeName();
                    if (!tableMock.TryFindRowByPkValues(equalities[colName0], equalities[colName1], equalities[colName2], out var rowIdx))
                    {
                        result = new TableResultMock();
                        return true;
                    }
                    var fieldRows3 = src.RowsByIndexes(rowIdx);
                    var evalRows3 = new List<EvalRow>(1);
                    foreach (var fields in fieldRows3)
                        evalRows3.Add(AstQueryRowSourceHelper.CreateSourceEvalRow(src, fields));
                    result = ProjectRows(q, evalRows3, ctes);
                    RecordFastPathMetrics(tableMock, q.Table);
                    return true;
                }
                default:
                {
                    pkValues = new object?[pkIndexes.Length];
                    for (var i = 0; i < pkIndexes.Length; i++)
                    {
                        var colName = tableMock.GetColumnByIndex(pkIndexes[i]).Name.NormalizeName();
                        pkValues[i] = equalities[colName];
                    }
                    if (!tableMock.TryFindRowByPkValues(pkValues, out var rowIdx))
                    {
                        result = new TableResultMock();
                        return true;
                    }
                    var fieldRowsN = src.RowsByIndexes(rowIdx);
                    var evalRowsN = new List<EvalRow>(1);
                    foreach (var fields in fieldRowsN)
                        evalRowsN.Add(AstQueryRowSourceHelper.CreateSourceEvalRow(src, fields));
                    result = ProjectRows(q, evalRowsN, ctes);
                    RecordFastPathMetrics(tableMock, q.Table);
                    return true;
                }
            }
        }
        finally
        {
            _context.RestorePositionalParameterState(parameterState);
        }
    }

    private void RecordFastPathMetrics(TableMock tableMock, SqlTableSource tableSource)
    {
        if (!Cnn.Metrics.Enabled)
            return;

        Cnn.Metrics.IndexLookups++;

        if (tableSource.MySqlIndexHints is { Count: > 0 } hints)
        {
            var hintPlan = AstQueryIndexHelper.BuildMySqlIndexHintPlan(
                hints,
                tableMock,
                hasOrderBy: false,
                hasGroupBy: false);

            TryRecordPrimaryKeyHintMetric(tableMock, hintPlan);
        }
    }
}
