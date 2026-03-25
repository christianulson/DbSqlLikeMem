namespace DbSqlLikeMem;

internal static class AstQueryOracleDb2ScalarFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions(fn, context, evalArg, tryCoerceDateTime, out result)
            || QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions(fn, context, evalArg, out result)
            || AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }
}
