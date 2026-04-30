namespace DbSqlLikeMem.MySql;

internal static class AstQueryMySqlCastFunctionEvaluator
{
    private static readonly AstQueryCastConversionFamilyEvaluator _evaluator = new(
        tryEvalJsonAccessShimFunction: AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonAccessShimFunction,
        tryEvalJsonExtractionFunction: AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction,
        tryEvalSqlServerJsonModifyFunction: TryEvalMySqlJsonModifyFunction,
        tryEvalOpenJsonFunction: TryEvalMySqlOpenJsonFunction,
        tryEvalJsonUnquoteFunction: AstQueryJsonUnquoteFunctionEvaluator.TryEvalJsonUnquoteFunction,
        tryEvalToNumberFunction: AstQueryToNumberFunctionEvaluator.TryEvalToNumberFunction);

    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => _evaluator.TryEvaluate(fn, context, evalArg, out result);

    private static bool TryEvalMySqlJsonModifyFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;
        _ = evalArg;
        result = null;
        return false;
    }

    private static bool TryEvalMySqlOpenJsonFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;
        _ = evalArg;
        result = null;
        return false;
    }
}
