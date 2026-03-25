namespace DbSqlLikeMem;

internal delegate object? AstQueryEvalTryCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate object? AstQueryEvalParseFunction(FunctionCallExpr fn, Func<int, object?> evalArg, bool swallowErrors);

internal delegate object? AstQueryEvalCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate object? AstQueryTryEvalJsonAndNumberFunctions(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out bool handled);

internal sealed class AstQueryCastConversionFamilyEvaluator(
    AstQueryTryEvalJsonAndNumberFunctions tryEvalJsonAndNumberFunctions,
    AstQueryEvalTryCast evalTryCast,
    AstQueryEvalParseFunction evalParseFunction,
    AstQueryEvalCast evalCast)
{
    private readonly AstQueryTryEvalJsonAndNumberFunctions _tryEvalJsonAndNumberFunctions =
        tryEvalJsonAndNumberFunctions ?? throw new ArgumentNullException(nameof(tryEvalJsonAndNumberFunctions));

    private readonly AstQueryEvalTryCast _evalTryCast =
        evalTryCast ?? throw new ArgumentNullException(nameof(evalTryCast));

    private readonly AstQueryEvalParseFunction _evalParseFunction =
        evalParseFunction ?? throw new ArgumentNullException(nameof(evalParseFunction));

    private readonly AstQueryEvalCast _evalCast =
        evalCast ?? throw new ArgumentNullException(nameof(evalCast));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = _tryEvalJsonAndNumberFunctions(fn, context, evalArg, out var handledJsonNumber);
        if (handledJsonNumber)
            return true;

        if (fn.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.SupportsTryCastFunction)
                throw SqlUnsupported.ForDialect(context.Dialect, "TRY_CAST");

            result = _evalTryCast(fn, evalArg);
            return true;
        }

        if (fn.Name.Equals("TRY_CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.SupportsTryConvertFunction)
                throw SqlUnsupported.ForDialect(context.Dialect, "TRY_CONVERT");

            result = _evalTryCast(fn, evalArg);
            return true;
        }

        if (fn.Name.Equals("PARSE", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.SupportsParseFunction)
                throw SqlUnsupported.ForDialect(context.Dialect, "PARSE");

            result = _evalParseFunction(fn, evalArg, false);
            return true;
        }

        if (fn.Name.Equals("TRY_PARSE", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.SupportsTryParseFunction)
                throw SqlUnsupported.ForDialect(context.Dialect, "TRY_PARSE");

            result = _evalParseFunction(fn, evalArg, true);
            return true;
        }

        if (fn.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase))
        {
            result = _evalCast(fn, evalArg);
            return true;
        }

        result = null;
        return false;
    }
}
