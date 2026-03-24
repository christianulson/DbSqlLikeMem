namespace DbSqlLikeMem;

internal delegate object? AstQueryEvalTryCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate object? AstQueryEvalParseFunction(FunctionCallExpr fn, Func<int, object?> evalArg, bool swallowErrors);

internal delegate object? AstQueryEvalCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate object? AstQueryTryEvalJsonAndNumberFunctions(
    FunctionCallExpr fn,
    ISqlDialect dialect,
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
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = _tryEvalJsonAndNumberFunctions(fn, dialect, evalArg, out var handledJsonNumber);
        if (handledJsonNumber)
            return true;

        if (fn.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsTryCastFunction)
                throw SqlUnsupported.ForDialect(dialect, "TRY_CAST");

            result = _evalTryCast(fn, evalArg);
            return true;
        }

        if (fn.Name.Equals("TRY_CONVERT", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsTryConvertFunction)
                throw SqlUnsupported.ForDialect(dialect, "TRY_CONVERT");

            result = _evalTryCast(fn, evalArg);
            return true;
        }

        if (fn.Name.Equals("PARSE", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsParseFunction)
                throw SqlUnsupported.ForDialect(dialect, "PARSE");

            result = _evalParseFunction(fn, evalArg, false);
            return true;
        }

        if (fn.Name.Equals("TRY_PARSE", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.SupportsTryParseFunction)
                throw SqlUnsupported.ForDialect(dialect, "TRY_PARSE");

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
