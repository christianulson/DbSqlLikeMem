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
        if (IsFoundRowsEquivalentFunction(fn.Name, context.Dialect))
        {
            if (fn.Args.Count != 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() não aceita argumentos.");

            result = context.Connection.GetLastFoundRows();
            return true;
        }

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

    private static bool IsFoundRowsEquivalentFunction(string functionName, ISqlDialect dialect)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return dialect.TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition is not null
            && definition.AllowsCall;
    }
}
