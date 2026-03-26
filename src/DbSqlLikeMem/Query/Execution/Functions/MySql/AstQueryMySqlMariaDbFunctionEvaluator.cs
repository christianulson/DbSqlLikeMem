using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryMySqlMariaDbFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
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
            || QueryTextSearchFunctionHelper.TryEvalMatchAgainstFunction(fn, context, evalArg, out result)
            || QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions(fn, context, evalArg, out result))
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

        if (QueryMySqlUtilityFunctionHelper.TryEvalUtilityFunctions(fn, context, evalArg, tryConvertNumericToInt64, out result)
            || QueryMySqlDateTimeFunctionHelper.TryEvalFunctions(fn, context, evalArg, tryConvertNumericToDouble, tryConvertNumericToInt64, tryCoerceDateTime, tryParseExactCachedDateTime, out result))
        {
            return true;
        }

        if (context.Dialect.SupportsMariaDbFunctions)
        {
            if (QueryMariaDbFunctionHelper.TryEvalFunctions(fn, context, evalArg, out result))
                return true;

            if (QueryMariaDbSpecialFunctionHelper.TryEvalFunctions(fn, context, evalArg, out result))
                return true;
        }

        if (AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate(fn, context, row, group, ctes, evalArg, eval, getTemporalUnit, out result)
            || AstQueryMySqlConversionAndMetadataFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryMySqlUtilityFunctionEvaluator.TryEvaluate(fn, context, row, evalArg, tryConvertNumericToInt64, tryConvertNumericToDouble, out result))
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
