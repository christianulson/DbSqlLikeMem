namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private SelectPlan BuildSelectPlan(
        SqlSelectQuery q,
        List<EvalRow> sampleRows,
        IDictionary<string, Source> ctes)
    {
        var cacheKey = ctes.Count == 0 ? BuildSelectPlanCacheKey(q, sampleRows) : null;
        if (cacheKey is not null
            && _cnn.TryGetCachedSelectPlan(cacheKey, out var cachedPlan)
            && cachedPlan is not null)
        {
            return cachedPlan.CanBeCachedWithoutClone
                ? cachedPlan
                : cachedPlan.CloneForExecution();
        }

        var plan = SelectPlanBuilderHelper.Build(
            q,
            sampleRows,
            ctes,
            _context,
            ParseScalarExpr,
            Eval,
            ResolveColumn);

        if (cacheKey is not null)
            _cnn.TryCacheSelectPlan(cacheKey, plan.CanBeCachedWithoutClone ? plan : plan.CloneForCache());

        return plan;
    }

    private string? BuildSelectPlanCacheKey(SqlSelectQuery query, List<EvalRow> sampleRows)
    {
        if (string.IsNullOrWhiteSpace(query.RawSql))
            return null;

        var cacheDialect = Dialect ?? _cnn.ExecutionDialect;
        var sb = new StringBuilder(query.RawSql.Length + 160);
        sb.Append(query.RawSql);
        sb.Append("|dialect:");
        sb.Append(cacheDialect.Name);
        sb.Append(':');
        sb.Append(cacheDialect.Version);
        sb.Append("|schema:");
        sb.Append(_cnn.GetSelectPlanCacheGeneration());
        sb.Append("|sources:");
        sb.Append(sampleRows.Count);

        if (sampleRows.Count == 0)
        {
            sb.Append("|<empty>");
            return sb.ToString();
        }

        var firstRow = sampleRows[0];
        if (firstRow.Sources.Count <= 1)
        {
            foreach (var sourceEntry in firstRow.Sources)
            {
                sb.Append('|');
                sb.Append(sourceEntry.Key);
                sb.Append('=');
                sb.Append(sourceEntry.Value.Name);
                sb.Append('/');
                sb.Append(sourceEntry.Value.Alias);
                sb.Append(':');
                for (var i = 0; i < sourceEntry.Value.ColumnNames.Count; i++)
                {
                    if (i > 0)
                        sb.Append(',');

                    sb.Append(sourceEntry.Value.ColumnNames[i]);
                }
            }

            return sb.ToString();
        }

        var sources = new List<KeyValuePair<string, Source>>(firstRow.Sources.Count);
        foreach (var sourceEntry in firstRow.Sources)
            sources.Add(sourceEntry);

        sources.Sort(static (left, right) => StringComparer.OrdinalIgnoreCase.Compare(left.Key, right.Key));

        foreach (var sourceEntry in sources)
        {
            sb.Append('|');
            sb.Append(sourceEntry.Key);
            sb.Append('=');
            sb.Append(sourceEntry.Value.Name);
            sb.Append('/');
            sb.Append(sourceEntry.Value.Alias);
            sb.Append(':');
            for (var i = 0; i < sourceEntry.Value.ColumnNames.Count; i++)
            {
                if (i > 0)
                    sb.Append(',');

                sb.Append(sourceEntry.Value.ColumnNames[i]);
            }
        }

        return sb.ToString();
    }

    // Remove "AS alias" somente quando:
    // - está no FINAL do select item
    // - e esse SqlConst.AS está fora de parênteses (pra não quebrar CAST(x AS CHAR))
    private static (string expr, string? alias) SplitTrailingAsAlias(
        string raw,
        string? alreadyAlias)
        => SelectAliasParserHelper.SplitTrailingAsAlias(raw, alreadyAlias);

    private TableResultMock ApplyOrderAndLimit(
        TableResultMock res,
        SqlSelectQuery q,
        IDictionary<string, Source> ctes,
        QueryDebugTraceBuilder? debugTrace = null)
        => AstQueryOrderLimitHelper.Apply(
            res,
            q,
            ctes,
            ParseExpr,
            (expr, row) => Eval(expr, row, group: null, ctes),
            (expr, scope) => Convert.ToInt32(Eval(expr, EvalRow.Empty(), null, scope), CultureInfo.InvariantCulture),
            CompareSql,
            debugTrace);

    private static string FormatJoinTypeForDebug(SqlJoinType joinType)
        => joinType switch
        {
            SqlJoinType.CrossApply => "CROSS APPLY",
            SqlJoinType.OuterApply => "OUTER APPLY",
            _ => joinType.ToString().ToUpperInvariant()
        };
}
