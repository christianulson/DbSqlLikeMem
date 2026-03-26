using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvaluateCorrelatedCountComparisonFast(
    BinaryExpr expression,
    EvalRow row,
    IDictionary<string, Source> ctes,
    out object? result);

internal static class AstQueryExpressionEvaluationHelper
{
    internal static object? EvalLike(
        LikeExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var left = eval(expression.Left, row, group, ctes)?.ToString() ?? string.Empty;
        var pattern = eval(expression.Pattern, row, group, ctes)?.ToString() ?? string.Empty;
        var escape = expression.Escape is null
            ? null
            : eval(expression.Escape, row, group, ctes)?.ToString();
        return left.Like(pattern, context, escape, expression.CaseInsensitive ? true : null);
    }

    internal static object? EvalNot(
        UnaryExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<InExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalNotIn)
    {
        if (expression.Expr is InExpr notInExpression)
            return evalNotIn(notInExpression, row, group, ctes);

        var value = eval(expression.Expr, row, group, ctes);
        return AstQueryExecutorBase.IsNullish(value) ? null : !value.ToBool();
    }

    internal static object? EvalIn(
        InExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getScalarLookup,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getRowLookup)
        => AstQueryInMembershipHelper.EvaluateIn(
            expression,
            row,
            group,
            ctes,
            context,
            eval,
            getScalarLookup,
            getRowLookup);

    internal static object? EvalNotIn(
        InExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getScalarLookup,
        Func<SubqueryExpr, EvalRow, IDictionary<string, Source>, InSubqueryLookupState> getRowLookup)
        => AstQueryInMembershipHelper.EvaluateNotIn(
            expression,
            row,
            group,
            ctes,
            context,
            eval,
            getScalarLookup,
            getRowLookup);

    internal static object? EvalJsonAccess(
        JsonAccessExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<JsonAccessExpr, SqlExpr> mapJsonAccess,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var mapped = mapJsonAccess(expression);
        return eval(mapped, row, group, ctes);
    }

    internal static object?[] EvalRowExpression(
        RowExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
        => [.. expression.Items.Select(item => eval(item, row, group, ctes))];

    internal static object? EvalBetween(
        BetweenExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var ge = new BinaryExpr(SqlBinaryOp.GreaterOrEqual, expression.Expr, expression.Low);
        var le = new BinaryExpr(SqlBinaryOp.LessOrEqual, expression.Expr, expression.High);
        var and = new BinaryExpr(SqlBinaryOp.And, ge, le);

        var res = eval(and, row, group, ctes);
        if (expression.Negated)
            return res is null ? null : !(bool)res;

        return res;
    }

    internal static object? EvalBinary(
        BinaryExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        ISqlDialect? dialect,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        AstQueryTryEvaluateCorrelatedCountComparisonFast tryEvaluateCorrelatedCountComparisonFast)
    {
        if (TryEvalLogicalBinary(expression, row, group, ctes, eval, out var logicalResult))
            return logicalResult;

        if (tryEvaluateCorrelatedCountComparisonFast(expression, row, ctes, out var countComparisonResult))
            return countComparisonResult;

        var left = eval(expression.Left, row, group, ctes);
        var right = eval(expression.Right, row, group, ctes);

        if (AstQueryBinaryExpressionHelper.TryEvalConcatBinary(expression.Op, left, right, dialect, out var concatResult))
            return concatResult;

        if (AstQueryBinaryArithmeticHelper.TryEvalArithmeticBinary(expression.Op, left, right, out var arithmeticResult))
            return arithmeticResult;

        if (AstQueryBinaryExpressionHelper.TryEvalNullSafeEqualityBinary(expression.Op, left, right, context, out var nullSafeEqualityResult))
            return nullSafeEqualityResult;

        if (left is null || left is DBNull || right is null || right is DBNull)
            return false;

        return AstQueryBinaryExpressionHelper.EvalComparisonBinary(expression.Op, left, right, context);
    }

    internal static bool TryEvalLogicalBinary(
        BinaryExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        out object? result)
    {
        result = expression.Op switch
        {
            SqlBinaryOp.And => eval(expression.Left, row, group, ctes).ToBool()
                && eval(expression.Right, row, group, ctes).ToBool(),
            SqlBinaryOp.Or => eval(expression.Left, row, group, ctes).ToBool()
                || eval(expression.Right, row, group, ctes).ToBool(),
            _ => null
        };

        return expression.Op is SqlBinaryOp.And or SqlBinaryOp.Or;
    }
}
