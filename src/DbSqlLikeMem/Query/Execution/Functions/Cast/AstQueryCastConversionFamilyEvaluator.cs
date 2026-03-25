namespace DbSqlLikeMem;

internal delegate object? AstQueryEvalTryCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate object? AstQueryEvalParseFunction(FunctionCallExpr fn, Func<int, object?> evalArg, bool swallowErrors);

internal delegate object? AstQueryEvalCast(FunctionCallExpr fn, Func<int, object?> evalArg);

internal delegate bool AstQueryTryEvalJsonAccessShimFunction(
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalJsonExtractionFunction(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalSqlServerJsonModifyFunction(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalOpenJsonFunction(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalJsonUnquoteFunction(
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalToNumberFunction(
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal sealed class AstQueryCastConversionFamilyEvaluator(
    AstQueryTryEvalJsonAccessShimFunction tryEvalJsonAccessShimFunction,
    AstQueryTryEvalJsonExtractionFunction tryEvalJsonExtractionFunction,
    AstQueryTryEvalSqlServerJsonModifyFunction tryEvalSqlServerJsonModifyFunction,
    AstQueryTryEvalOpenJsonFunction tryEvalOpenJsonFunction,
    AstQueryTryEvalJsonUnquoteFunction tryEvalJsonUnquoteFunction,
    AstQueryTryEvalToNumberFunction tryEvalToNumberFunction,
    AstQueryEvalTryCast evalTryCast,
    AstQueryEvalParseFunction evalParseFunction,
    AstQueryEvalCast evalCast)
{
    private readonly AstQueryTryEvalJsonAccessShimFunction _tryEvalJsonAccessShimFunction =
        tryEvalJsonAccessShimFunction ?? throw new ArgumentNullException(nameof(tryEvalJsonAccessShimFunction));

    private readonly AstQueryTryEvalJsonExtractionFunction _tryEvalJsonExtractionFunction =
        tryEvalJsonExtractionFunction ?? throw new ArgumentNullException(nameof(tryEvalJsonExtractionFunction));

    private readonly AstQueryTryEvalSqlServerJsonModifyFunction _tryEvalSqlServerJsonModifyFunction =
        tryEvalSqlServerJsonModifyFunction ?? throw new ArgumentNullException(nameof(tryEvalSqlServerJsonModifyFunction));

    private readonly AstQueryTryEvalOpenJsonFunction _tryEvalOpenJsonFunction =
        tryEvalOpenJsonFunction ?? throw new ArgumentNullException(nameof(tryEvalOpenJsonFunction));

    private readonly AstQueryTryEvalJsonUnquoteFunction _tryEvalJsonUnquoteFunction =
        tryEvalJsonUnquoteFunction ?? throw new ArgumentNullException(nameof(tryEvalJsonUnquoteFunction));

    private readonly AstQueryTryEvalToNumberFunction _tryEvalToNumberFunction =
        tryEvalToNumberFunction ?? throw new ArgumentNullException(nameof(tryEvalToNumberFunction));

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
        if (_tryEvalJsonAccessShimFunction(fn, evalArg, out result))
            return true;

        if (_tryEvalJsonExtractionFunction(fn, context, evalArg, out result))
            return true;

        if (_tryEvalSqlServerJsonModifyFunction(fn, context, evalArg, out result))
            return true;

        if (_tryEvalOpenJsonFunction(fn, context, evalArg, out result))
            return true;

        if (_tryEvalJsonUnquoteFunction(fn, evalArg, out result))
            return true;

        if (_tryEvalToNumberFunction(fn, evalArg, out result))
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
