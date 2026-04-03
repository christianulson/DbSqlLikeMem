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
            var (expr, alias) = SqlAliasParserHelper.SplitTrailingAsAliasTopLevel(raw, ctx.Dialect);
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
                if (IsAggregateFunctionName(call.Name))
                    return true;
                return call.Args.Any(ContainsAggregateFunction);
            case FunctionCallExpr fn:
                if (IsAggregateFunctionName(fn.Name))
                    return true;
                return fn.Args.Any(ContainsAggregateFunction);
            case UnaryExpr unary:
                return ContainsAggregateFunction(unary.Expr);
            case BinaryExpr binary:
                return ContainsAggregateFunction(binary.Left) || ContainsAggregateFunction(binary.Right);
            case InExpr inExpr:
                return ContainsAggregateFunction(inExpr.Left) || inExpr.Items.Any(ContainsAggregateFunction);
            case LikeExpr likeExpr:
                return ContainsAggregateFunction(likeExpr.Left)
                    || ContainsAggregateFunction(likeExpr.Pattern)
                    || (likeExpr.Escape is not null && ContainsAggregateFunction(likeExpr.Escape));
            case IsNullExpr isNullExpr:
                return ContainsAggregateFunction(isNullExpr.Expr);
            case RowExpr rowExpr:
                return rowExpr.Items.Any(ContainsAggregateFunction);
            case CaseExpr caseExpr:
                return (caseExpr.BaseExpr is not null && ContainsAggregateFunction(caseExpr.BaseExpr))
                    || caseExpr.Whens.Any(when => ContainsAggregateFunction(when.When) || ContainsAggregateFunction(when.Then))
                    || (caseExpr.ElseExpr is not null && ContainsAggregateFunction(caseExpr.ElseExpr));
            case WindowFunctionExpr windowExpr:
                return windowExpr.Args.Any(ContainsAggregateFunction)
                    || windowExpr.Spec.PartitionBy.Any(ContainsAggregateFunction)
                    || windowExpr.Spec.OrderBy.Any(item => ContainsAggregateFunction(item.Expr));
            case JsonAccessExpr jsonAccessExpr:
                return ContainsAggregateFunction(jsonAccessExpr.Target)
                    || ContainsAggregateFunction(jsonAccessExpr.Path);
            case QuantifiedComparisonExpr quantifiedComparisonExpr:
                return ContainsAggregateFunction(quantifiedComparisonExpr.Left);
            case ExistsExpr:
                return false;
            default:
                return false;
        }
    }

    private static bool IsAggregateFunctionName(string name)
    {
        return !string.IsNullOrWhiteSpace(name) && AggregateFunctionNames.Contains(name);
    }
}
