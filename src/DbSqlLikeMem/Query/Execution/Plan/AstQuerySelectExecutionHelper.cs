using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQuerySelectExecutionHelper
{
    internal static SelectPlan BuildSelectPlan(
        this QueryExecutionContext context,
        SqlSelectQuery query,
        List<EvalRow> sampleRows,
        IDictionary<string, Source> ctes,
        Func<string, SqlExpr> parseScalarExpr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalExpression,
        Func<string?, string, EvalRow, object?> resolveColumn)
    {
        var cacheKey = ctes.Count == 0 ? BuildSelectPlanCacheKey(context, query, sampleRows) : null;
        if (cacheKey is not null
            && context.Connection.TryGetCachedSelectPlan(cacheKey, out var cachedPlan)
            && cachedPlan is not null)
        {
            return cachedPlan.CanBeCachedWithoutClone
                ? cachedPlan
                : cachedPlan.CloneForExecution();
        }

        var plan = SelectPlanBuilderHelper.Build(
            query,
            sampleRows,
            ctes,
            context,
            parseScalarExpr,
            evalExpression,
            resolveColumn);

        if (cacheKey is not null)
            context.Connection.TryCacheSelectPlan(cacheKey, plan.CanBeCachedWithoutClone ? plan : plan.CloneForCache());

        return plan;
    }

    internal static bool ContainsDistinctUnionFlag(IReadOnlyList<bool> allFlags)
    {
        for (var i = 0; i < allFlags.Count; i++)
        {
            if (!allFlags[i])
                return true;
        }

        return false;
    }

    internal static string FormatJoinTypeForDebug(SqlJoinType joinType)
        => joinType switch
        {
            SqlJoinType.CrossApply => "CROSS APPLY",
            SqlJoinType.OuterApply => "OUTER APPLY",
            _ => joinType.ToString().ToUpperInvariant()
        };

    private static string? BuildSelectPlanCacheKey(
        this QueryExecutionContext context,
        SqlSelectQuery query,
        List<EvalRow> sampleRows)
    {
        if (string.IsNullOrWhiteSpace(query.RawSql))
            return null;

        var cacheDialect = context.Dialect ?? context.Connection.ExecutionDialect;
        var sb = new StringBuilder(query.RawSql.Length + 160);
        sb.Append(query.RawSql);
        sb.Append("|dialect:");
        sb.Append(cacheDialect.Name);
        sb.Append(':');
        sb.Append(cacheDialect.Version);
        sb.Append("|schema:");
        sb.Append(context.Connection.GetSelectPlanCacheGeneration());
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
}
