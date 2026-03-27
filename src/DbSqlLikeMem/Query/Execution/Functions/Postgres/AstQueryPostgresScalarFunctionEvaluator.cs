namespace DbSqlLikeMem;

internal static class AstQueryPostgresScalarFunctionEvaluator
{
    internal static bool TryEvaluateyPostgresScalarFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (context.TryEvaluatePostgresSystemFunction(fn, evalArg, out result)
            || context.TryEvaluatePostgresDateFunction(fn, evalArg, out result)
            || AstQueryPostgresScalarUtilityFunctionEvaluator.TryEvaluatePostgresScalarUtilityFunction(context, fn, evalArg, out result)
            || AstQueryPostgresTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryPostgresNetworkFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryPostgresUnicodeFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryPostgresRegexFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryPostgresArrayFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryPostgresJsonFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryPostgresUuidFunctionEvaluator.TryEvaluate(context, fn, out result))
        {
            return true;
        }

        result = null;
        return false;
    }
}
