using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQueryCastStringAndDateTailEvaluator(
    AstQueryTryEvalFunctionFamily tryEvalCastConversionFamily,
    AstQueryTryEvalFunctionFamily tryEvalCastConcatAndStringTail,
    AstQueryTryEvalFunctionFamily tryEvalCastDateTail)
{
    private readonly AstQueryTryEvalFunctionFamily _tryEvalCastConversionFamily =
        tryEvalCastConversionFamily ?? throw new ArgumentNullException(nameof(tryEvalCastConversionFamily));

    private readonly AstQueryTryEvalFunctionFamily _tryEvalCastConcatAndStringTail =
        tryEvalCastConcatAndStringTail ?? throw new ArgumentNullException(nameof(tryEvalCastConcatAndStringTail));

    private readonly AstQueryTryEvalFunctionFamily _tryEvalCastDateTail =
        tryEvalCastDateTail ?? throw new ArgumentNullException(nameof(tryEvalCastDateTail));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_tryEvalCastConversionFamily(fn, row, group, ctes, context, evalArg, out result)
            || _tryEvalCastConcatAndStringTail(fn, row, group, ctes, context, evalArg, out result)
            || _tryEvalCastDateTail(fn, row, group, ctes, context, evalArg, out result))
        {
            return true;
        }

        if (fn.Args.Count == 0
            && SqlTemporalFunctionEvaluator.IsKnownTemporalFunctionName(context, fn.Name))
        {
            throw new InvalidOperationException($"Temporal function '{fn.Name}' is not supported for context.Dialect '{context.Dialect.Name}'.");
        }

        result = null;
        return false;
    }

    internal static bool TryEvalCastConcatAndStringTail(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = row;
        _ = group;
        _ = ctes;

        result = QueryConcatFunctionHelper.TryEvalConcatFunctions(
            fn,
            evalArg,
            context.Dialect.ConcatReturnsNullOnNullInput,
            out var handledConcat);
        if (handledConcat)
            return true;

        if (AstQueryGeneralScalarFunctionEvaluator.TryEvalCharFunction(fn, context, evalArg, out result)
            || AstQueryOracleDb2LegacyFunctionEvaluator.TryEvalDialectSpecificCastFunction(fn, context, evalArg, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalBasicStringFunction(fn, evalArg, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalSubstringFunction(fn, evalArg, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalReplaceFunction(fn, context, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    internal static bool TryEvalCastDateTail(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        if (AstQueryTemporalArithmeticFunctionEvaluator.TryEvaluate(fn, context, row, group, ctes, evalArg, eval, getTemporalUnit, out result))
        {
            return true;
        }

        if (AstQueryGeneralDateFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(fn, row, group, ctes, evalArg, getTemporalUnit, AstQueryExecutorBase.ResolveTemporalUnit, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalFieldFunction(fn, context, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }
}
