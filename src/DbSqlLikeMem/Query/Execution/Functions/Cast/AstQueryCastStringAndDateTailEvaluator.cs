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
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_tryEvalCastConversionFamily(context, fn, row, group, ctes, evalArg, out result)
            || _tryEvalCastConcatAndStringTail(context,fn, row, group, ctes,  evalArg, out result)
            || _tryEvalCastDateTail(context, fn, row, group, ctes, evalArg, out result))
        {
            return true;
        }

        if (fn.Args.Count == 0
            && context.IsKnownTemporalFunctionName(fn.Name))
        {
            throw new InvalidOperationException($"Temporal function '{fn.Name}' is not supported for context.Dialect '{context.Dialect.Name}'.");
        }

        result = null;
        return false;
    }

    internal static bool TryEvalCastConcatAndStringTail(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = row;
        _ = group;
        _ = ctes;

        var handledConcat = context.TryEvalConcatFunctions(
            fn,
            evalArg,
            out result);
        if (handledConcat)
            return true;

        if (AstQuerySharedTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || context.TryEvalDialectSpecificCastFunction(fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    internal static bool TryEvalCastDateTail(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        out object? result)
    {
        if (context.TryEvaluate(fn, row, group, ctes, evalArg, eval, getTemporalUnit, out result))
        {
            return true;
        }

        if (AstQueryGeneralDateFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQuerySqlServerTemporalAccessorFunctionEvaluator.TryEvaluate(
                fn,
                row,
                group,
                ctes,
                evalArg,
                getTemporalUnit,
                AstQueryExecutionRuntimeHelper.ResolveTemporalUnit,
                out result)
            || AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(
                fn,
                row,
                group,
                ctes,
                evalArg,
                getTemporalUnit,
                AstQueryExecutionRuntimeHelper.ResolveTemporalUnit,
                out result)
            || QueryMariaDbFunctionHelper.TryEvalFunctions(context, fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }
}
