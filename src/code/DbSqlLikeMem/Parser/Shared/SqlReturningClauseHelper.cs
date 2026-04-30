namespace DbSqlLikeMem;

internal static class SqlReturningClauseHelper
{
    private static readonly HashSet<string> AggregateFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        SqlConst.COUNT,
        SqlConst.SUM,
        SqlConst.AVG,
        SqlConst.MIN,
        SqlConst.MAX,
        SqlConst.GROUP_CONCAT,
        SqlConst.STRING_AGG,
        SqlConst.LISTAGG,
        SqlConst.LIST,
        SqlConst.JSON_ARRAYAGG,
        SqlConst.JSON_OBJECTAGG,
        "STDDEV",
        "STDDEV_POP",
        "STDDEV_SAMP",
        SqlConst.VARIANCE,
        SqlConst.VAR_POP,
        SqlConst.VAR_SAMP,
        SqlConst.VAR,
        SqlConst.BIT_AND,
        SqlConst.BIT_OR,
        SqlConst.BIT_XOR
    };

    internal static IReadOnlyList<SqlSelectItem> ParseOptionalReturningItems(
        this SqlQueryParserContext ctx,
        bool supportsReturning)
    {
        if (!ctx.IsWord(SqlConst.RETURNING))
            return [];

        if (!supportsReturning)
            throw ctx.NotSupported(SqlConst.RETURNING);

        ctx.Consume(); // RETURNING

        List<string> raws;
        try
        {
            raws = ctx.ParseReturningItemsRaw(ctx.ReadRawExpressionUntilCommaOrTerminator);
        }
        catch (NotSupportedException ex) when (
            ex.Message.Contains("RETURNING has unbalanced parentheses in expression.", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("RETURNING has unbalanced parentheses in expression.", ex);
        }
        catch (InvalidOperationException ex) when (
            ex.Message.Contains("RETURNING has unbalanced parentheses in expression.", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("RETURNING has unbalanced parentheses in expression.", ex);
        }

        return raws.ConvertAll(raw =>
        {
            var (expr, alias) = SqlAliasParserHelper.SplitTrailingAsAliasTopLevel(raw.AsSpan(), ctx.Dialect);
            if (string.IsNullOrWhiteSpace(expr))
                throw new InvalidOperationException(
                    $"RETURNING requires at least one expression (found '{SqlQueryParserContext.DescribeFoundTokenFromRaw(raw)}').");

            try
            {
                var parsedExpr = ctx.ParseScalar(expr);
                ctx.ValidateReturningExpression(parsedExpr);
            }
            catch (InvalidOperationException ex)
            {
                if (ex.Message.Contains("aggregate functions in this dialect", StringComparison.OrdinalIgnoreCase))
                    throw;

                throw new InvalidOperationException("RETURNING expression is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("RETURNING expression is invalid.", ex);
            }

            return new SqlSelectItem(expr, alias);
        });
    }

    private static void ValidateReturningExpression(
        this SqlQueryParserContext ctx,
        SqlExpr expr)
    {
        if (ctx.Dialect.SupportsAggregateFunctionsInReturningClause)
            return;

        if (!ContainsAggregateFunction(expr))
            return;

        throw new InvalidOperationException("RETURNING clause does not allow aggregate functions in this dialect.");
    }

    private static bool ContainsAggregateFunction(SqlExpr expr)
    {
        switch (expr)
        {
            case CallExpr call:
                return ContainsAggregateFunctionInCall(call.Name, call.Args);
            case FunctionCallExpr fn:
                return ContainsAggregateFunctionInCall(fn.Name, fn.Args);
            case UnaryExpr unary:
                return ContainsAggregateFunction(unary.Expr);
            case BinaryExpr binary:
                return ContainsAggregateFunctionInBinary(binary);
            case InExpr inExpr:
                return ContainsAggregateFunction(inExpr.Left) || inExpr.Items.Any(ContainsAggregateFunction);
            case LikeExpr likeExpr:
                return ContainsAggregateFunctionInLike(likeExpr);
            case IsNullExpr isNullExpr:
                return ContainsAggregateFunction(isNullExpr.Expr);
            case RowExpr rowExpr:
                return ContainsAggregateFunctionInRow(rowExpr);
            case CaseExpr caseExpr:
                return ContainsAggregateFunctionInCase(caseExpr);
            case WindowFunctionExpr windowExpr:
                return ContainsAggregateFunctionInWindow(windowExpr);
            case JsonAccessExpr jsonAccessExpr:
                return ContainsAggregateFunctionInJsonAccess(jsonAccessExpr);
            case QuantifiedComparisonExpr quantifiedComparisonExpr:
                return ContainsAggregateFunction(quantifiedComparisonExpr.Left);
            case ExistsExpr:
                return false;
            default:
                return false;
        }
    }

    private static bool ContainsAggregateFunctionInCall(string name, IReadOnlyList<SqlExpr> args)
        => IsAggregateFunctionName(name) || args.Any(ContainsAggregateFunction);

    private static bool ContainsAggregateFunctionInBinary(BinaryExpr binary)
        => ContainsAggregateFunction(binary.Left) || ContainsAggregateFunction(binary.Right);

    private static bool ContainsAggregateFunctionInLike(LikeExpr likeExpr)
        => ContainsAggregateFunction(likeExpr.Left)
           || ContainsAggregateFunction(likeExpr.Pattern)
           || (likeExpr.Escape is not null && ContainsAggregateFunction(likeExpr.Escape));

    private static bool ContainsAggregateFunctionInRow(RowExpr rowExpr)
        => rowExpr.Items.Any(ContainsAggregateFunction);

    private static bool ContainsAggregateFunctionInCase(CaseExpr caseExpr)
        => (caseExpr.BaseExpr is not null && ContainsAggregateFunction(caseExpr.BaseExpr))
           || caseExpr.Whens.Any(when => ContainsAggregateFunction(when.When) || ContainsAggregateFunction(when.Then))
           || (caseExpr.ElseExpr is not null && ContainsAggregateFunction(caseExpr.ElseExpr));

    private static bool ContainsAggregateFunctionInWindow(WindowFunctionExpr windowExpr)
        => windowExpr.Args.Any(ContainsAggregateFunction)
           || windowExpr.Spec.PartitionBy.Any(ContainsAggregateFunction)
           || windowExpr.Spec.OrderBy.Any(item => ContainsAggregateFunction(item.Expr));

    private static bool ContainsAggregateFunctionInJsonAccess(JsonAccessExpr jsonAccessExpr)
        => ContainsAggregateFunction(jsonAccessExpr.Target)
           || ContainsAggregateFunction(jsonAccessExpr.Path);

    private static bool IsAggregateFunctionName(string name)
    {
        return !string.IsNullOrWhiteSpace(name) && AggregateFunctionNames.Contains(name);
    }
}
