using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryMySqlMariaDbFunctionEvaluator
{
    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        TryParseExactCachedDateTimeDelegate tryParseExactCachedDateTime,
        out object? result)
    {
        if (QueryTextSearchFunctionHelper.TryEvalFindInSetFunction(fn, evalArg, out result)
            || context.TryEvalMatchAgainstFunction(fn, evalArg, out result)
            || context.TryEvalConditionalAndNullFunctions(fn, evalArg, out result))
        {
            return true;
        }

        if (IsFoundRowsEquivalentFunction(fn.Name, context.Dialect))
        {
            if (fn.Args.Count != 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() não aceita argumentos.");

            result = context.Connection.GetLastFoundRows();
            return true;
        }

        if (context.TryEvalUtilityFunctions(fn, evalArg, tryConvertNumericToInt64, out result)
            || context.TryEvalFunctions(fn, evalArg, tryConvertNumericToDouble, tryConvertNumericToInt64, tryCoerceDateTime, tryParseExactCachedDateTime, out result))
        {
            return true;
        }

        if (context.Dialect.SupportsMariaDbFunctions)
        {
            if (context.TryEvalFunctions(fn, evalArg, out result))
                return true;

            if (context.TryEvalSpecialFunctions(fn, evalArg, out result))
                return true;
        }

        if (context.TryEvaluate(fn, row, group, ctes, evalArg, eval, getTemporalUnit, out result)
            || AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || context.TryEvaluate(fn, row, evalArg, tryConvertNumericToInt64, tryConvertNumericToDouble, out result))
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
