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
}
