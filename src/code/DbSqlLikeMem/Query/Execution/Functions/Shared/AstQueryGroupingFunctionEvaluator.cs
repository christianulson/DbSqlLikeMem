namespace DbSqlLikeMem;

internal static class AstQueryGroupingFunctionEvaluator
{
    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!TryEvalGroupingFunction(context, fn, evalArg, out result))
            return TryEvalGroupingIdFunction(context, fn, evalArg, out result);

        return true;
    }

    internal static bool TryEvalGroupingFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!string.Equals(fn.Name, "GROUPING", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        result = 0;
        return true;
    }

    internal static bool TryEvalGroupingIdFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!string.Equals(fn.Name, "GROUPING_ID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        result = 0;
        return true;
    }
}
