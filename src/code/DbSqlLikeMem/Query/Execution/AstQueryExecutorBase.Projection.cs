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
            var outRow = IntDictPool.Get(projectedColumnCount);
            using (var positionalScope = _context.BeginPositionalParameterScope())
            {
                for (int i = 0; i < projectedColumnCount; i++)
                    outRow[i] = selectPlan.Evaluators[i](r, null);
            }

            res.Add(outRow);
            res.JoinFields.Add(r.Fields);
        }

        // Return per-row OrdinalValues arrays to pool; no longer needed after projection.
        foreach (var r in rows)
            OrdinalPool.Return(r.OrdinalValues);

        return res;
    }

    private TableResultMock ProjectGrouped(
        SqlSelectQuery q,
        IEnumerable<MaterializedGroup> groups,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        var projectStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var res = new TableResultMock();

        using var enumerator = groups.GetEnumerator();
        if (!enumerator.MoveNext())
        {
            // SQL aggregate semantics: when no GROUP BY is present and the filtered input is empty,
            // aggregate projections (e.g. COUNT(*)) still return a single row.
            if (q.GroupBy.Count == 0)
            {
                var emptyGroup = new MaterializedGroup(default, []);
                ProjectGroupedCore(q, res, emptyGroup, ctes, debugTrace, projectStart);
            }

            return res;
        }

        // Use first group's first row to build the select plan.
        // A single sample row is sufficient for type inference in GROUP BY queries
        // (aggregate types are determined by function, key column types are uniform).
        var firstGroup = enumerator.Current;
        var sampleFirst = firstGroup.Rows.Count > 0 ? firstGroup.Rows[0] : EvalRow.Empty();
        var sampleRows = new List<EvalRow>(1) { sampleFirst };

        var selectPlan = _context.BuildSelectPlan(
            q,
            sampleRows,
            ctes,
            ParseScalarExpr,
            Eval,
            QueryRowValueHelper.ResolveColumn);

        var columnCount = selectPlan.Columns.Count;
        var groupedColumnCount = selectPlan.Evaluators.Count;

        for (int i = 0; i < columnCount; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        // Process first group, then release its rows
        ProjectGroupedGroup(firstGroup, selectPlan, groupedColumnCount, res);
        firstGroup.ReleaseRows();

        // Process remaining groups (each group's rows are released after evaluation)
        while (enumerator.MoveNext())
        {
            var g = enumerator.Current;
            ProjectGroupedGroup(g, selectPlan, groupedColumnCount, res);
            g.ReleaseRows();
        }

        debugTrace?.AddStep(
            "Project",
            res.Count,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(q.SelectItems));

        return ApplyGroupedPostProcessing(res, q, ctes, debugTrace);
    }

    private static void ProjectGroupedGroup(
        MaterializedGroup g,
        SelectPlan selectPlan,
        int groupedColumnCount,
        TableResultMock res)
    {
        var eg = new EvalGroup(g.Rows);
        var outRow = IntDictPool.Get(groupedColumnCount);

        var first = g.Rows.Count > 0 ? g.Rows[0] : EvalRow.Empty();
        for (int i = 0; i < groupedColumnCount; i++)
            outRow[i] = selectPlan.Evaluators[i](first, eg);

        res.Add(outRow);
        res.JoinFields.Add(first.Fields);
    }

    private void ProjectGroupedCore(
        SqlSelectQuery q,
        TableResultMock res,
        MaterializedGroup group,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace,
        long projectStart)
    {
        var selectPlan = _context.BuildSelectPlan(
            q,
            [],
            ctes,
            ParseScalarExpr,
            Eval,
            QueryRowValueHelper.ResolveColumn);

        var columnCount = selectPlan.Columns.Count;
        var groupedColumnCount = selectPlan.Evaluators.Count;

        for (int i = 0; i < columnCount; i++)
            res.Columns.Add(selectPlan.Columns[i]);

        var outRow = IntDictPool.Get(groupedColumnCount);

        var first = EvalRow.Empty();
        var eg = new EvalGroup(group.Rows);
        for (int i = 0; i < groupedColumnCount; i++)
            outRow[i] = selectPlan.Evaluators[i](first, eg);

        res.Add(outRow);
        res.JoinFields.Add(first.Fields);

        debugTrace?.AddStep(
            "Project",
            1,
            res.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(projectStart)),
            QueryDebugTraceFormattingHelper.FormatProjectDebugDetails(q.SelectItems));
    }

    private TableResultMock ApplyGroupedPostProcessing(
        TableResultMock res,
        SqlSelectQuery q,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace)
    {
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

            res = _context.ApplyDistinctOn(res, q.DistinctOn, ParseExpr, (expr, row) => Eval(expr, row, group: null, ctes));

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
