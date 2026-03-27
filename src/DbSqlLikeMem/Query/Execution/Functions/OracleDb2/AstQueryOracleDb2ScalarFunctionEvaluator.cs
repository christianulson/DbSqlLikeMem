namespace DbSqlLikeMem;

internal static class AstQueryOracleDb2ScalarFunctionEvaluator
{
    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (context.TryEvalCoreFunctions(fn, evalArg, tryCoerceDateTime, out result)
            || context.TryEvalUtilityFunctions(fn, evalArg, out result)
            || AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }
}
