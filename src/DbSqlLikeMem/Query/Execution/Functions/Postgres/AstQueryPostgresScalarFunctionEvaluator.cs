namespace DbSqlLikeMem;

internal static class AstQueryPostgresScalarFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        if (AstQueryPostgresSystemFunctionEvaluator.TryEvaluate(fn, context, evalArg, getCurrentQueryText, out result)
            || AstQueryPostgresDateFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresScalarUtilityFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresTextFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresNetworkFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresUnicodeFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresRegexFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresArrayFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryPostgresUuidFunctionEvaluator.TryEvaluate(fn, context, out result))
        {
            return true;
        }

        result = null;
        return false;
    }
}
