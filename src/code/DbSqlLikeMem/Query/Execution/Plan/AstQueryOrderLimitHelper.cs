using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryOrderLimitHelper
{
    internal static TableResultMock ApplyQueryOrderLimit(
        this QueryExecutionContext context,
        TableResultMock result,
        SqlSelectQuery query,
        IDictionary<string, Source> ctes,
        Func<string, SqlExpr> parseExpr,
        Func<SqlExpr, EvalRow, object?> evalExpression,
        Func<SqlExpr, IDictionary<string, Source>, int> evalLimitExpr,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        if (query.OrderBy.Count == 0)
        {
            var limitInput = result.Count;
            QueryRowLimitHelper.ApplyLimit(result, query.RowLimit, expr => evalLimitExpr(expr, ctes));
            if (debugTrace is not null && query.RowLimit is not null)
            {
                debugTrace.AddStep(
                    "Limit",
                    limitInput,
                    result.Count,
                    TimeSpan.Zero,
                    QueryDebugTraceFormattingHelper.FormatLimitDebugDetails(query.RowLimit));
            }

            return result;
        }

        var sortStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var sortInput = result.Count;
        var sorted = context.TryApplyOrder(
            result,
            query.OrderBy,
            parseExpr,
            (expr, row) => evalExpression(expr, row));

        if (!sorted)
        {
            var limitInput = result.Count;
            QueryRowLimitHelper.ApplyLimit(result, query.RowLimit, expr => evalLimitExpr(expr, ctes));
            if (debugTrace is not null && query.RowLimit is not null)
            {
                debugTrace.AddStep(
                    "Limit",
                    limitInput,
                    result.Count,
                    TimeSpan.Zero,
                    QueryDebugTraceFormattingHelper.FormatLimitDebugDetails(query.RowLimit));
            }

            return result;
        }

        debugTrace?.AddStep(
            "Sort",
            sortInput,
            result.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(sortStart)),
            QueryDebugTraceFormattingHelper.FormatOrderByDebugDetails(query.OrderBy));

        var limitInputRows = result.Count;
        QueryRowLimitHelper.ApplyLimit(result, query.RowLimit, expr => evalLimitExpr(expr, ctes));
        if (debugTrace is not null && query.RowLimit is not null)
        {
            debugTrace.AddStep(
                "Limit",
                limitInputRows,
                result.Count,
                TimeSpan.Zero,
                QueryDebugTraceFormattingHelper.FormatLimitDebugDetails(query.RowLimit));
        }

        return result;
    }
}
