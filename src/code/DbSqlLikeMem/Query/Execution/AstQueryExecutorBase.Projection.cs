namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    // ---------------- PROJECTION ----------------

    private TableResultMock ProjectRows(
        SqlSelectQuery q,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        var res = new TableResultMock();
        var selectPlan = _context.BuildSelectPlan(
            q,
            rows,
            ctes,
            ParseScalarExpr,
            Eval,
            QueryRowValueHelper.ResolveColumn);

        context.ComputeWindowSlots(
            Eval,
            selectPlan.WindowSlots,
            rows,
            ctes);

        var columnCount = selectPlan.Columns.Count;
        var projectedColumnCount = selectPlan.Evaluators.Count;

        for (int i = 0; i < columnCount; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        foreach (var r in rows)
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
            var outRow = new Dictionary<int, object?>(projectedColumnCount);
            for (int i = 0; i < projectedColumnCount; i++)
                outRow[i] = selectPlan.Evaluators[i](r, null);

            res.Add(outRow);
            res.JoinFields.Add(r.Fields);
        }

        return res;
    }

    private TableResultMock ProjectGrouped(
        SqlSelectQuery q,
        IReadOnlyList<MaterializedGroup> groups,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var projectStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var res = new TableResultMock();
        var groupsList = groups as List<MaterializedGroup> ?? new List<MaterializedGroup>(groups);
        var hasGroups = groupsList.Count > 0;

        // SQL aggregate semantics: when no GROUP BY is present and the filtered input is empty,
        // aggregate projections (e.g. COUNT(*)) still return a single row.
        if (!hasGroups && q.GroupBy.Count == 0)
            groupsList.Add(new MaterializedGroup(default, new List<EvalRow>()));

        var representativeRows = hasGroups
            ? new List<EvalRow>(groupsList.Count)
            : [];
        if (hasGroups)
        {
            for (var i = 0; i < groupsList.Count; i++)
                representativeRows.Add(groupsList[i].Rows[0]);
        }

        var selectPlan = _context.BuildSelectPlan(
            q,
            representativeRows,
            ctes,
            ParseScalarExpr,
            Eval,
            QueryRowValueHelper.ResolveColumn);

        var columnCount = selectPlan.Columns.Count;
        var groupedColumnCount = selectPlan.Evaluators.Count;

        for (int i = 0; i < columnCount; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        foreach (var g in groupsList)
        {
            using var positionalScope = _context.BeginPositionalParameterScope();
            var eg = new EvalGroup(g.Rows);
            var outRow = new Dictionary<int, object?>(groupedColumnCount);

            var first = g.Rows.Count > 0 ? g.Rows[0] : EvalRow.Empty();
            for (int i = 0; i < groupedColumnCount; i++)
            {
                var value = selectPlan.Evaluators[i](first, eg);
                outRow[i] = value;
            }

            res.Add(outRow);
            res.JoinFields.Add(first.Fields);
        }

        if (q.DistinctOn.Count > 0)
        {
            var distinctStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = res.Count;

            if (q.OrderBy.Count > 0)
                _context.TryApplyOrder(
                    res,
                    q.OrderBy,
                    ParseExpr,
                    (expr, row) => Eval(expr, row, group: null, ctes));

            res = _context.ApplyDistinctOn(res, q.DistinctOn, ParseExpr, (expr, row) =>
            {
                using var positionalScope = _context.BeginPositionalParameterScope();
                return Eval(expr, row, group: null, ctes);
            });

            debugTrace?.AddStep(
                "Distinct On",
                inputRows,
                res.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(q.DistinctOn.Count));
        }
        else if (q.Distinct)
        {
            var distinctStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
            var inputRows = res.Count;
            res = _context.ApplyDistinct(res);
            debugTrace?.AddStep(
                "Distinct",
                inputRows,
                res.Count,
                TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(distinctStart)),
                QueryDebugTraceFormattingHelper.FormatDistinctDebugDetails(q.SelectItems.Count));
        }

        if (HasSqlCalcFoundRows(q))
            Cnn.SetLastSelectRows(res.Count);

        // ORDER / LIMIT
        debugTrace?.AddStep(
            "Project",
            groupsList.Count,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(q.SelectItems));
        res = _context.ApplyQueryOrderLimit(
            res,
            q,
            ctes,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
            debugTrace);
        res = AstQueryExecutorForJsonHelper.ApplyForJsonIfNeeded(res, q, debugTrace);
        return res;
    }

    private bool HasSqlCalcFoundRows(SqlSelectQuery query)
        => _context.Dialect?.SupportsSqlCalcFoundRowsModifier == true
           && !string.IsNullOrWhiteSpace(query.RawSql)
           && _sqlCalcFoundRowsRegex.IsMatch(query.RawSql);
}
