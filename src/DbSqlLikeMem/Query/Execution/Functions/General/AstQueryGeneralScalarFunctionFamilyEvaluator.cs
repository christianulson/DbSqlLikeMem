using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryGeneralScalarFunctionFamilyEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        AstQueryGeneralScalarFunctionHandler tryEvalJsonObjectFunction,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<CallExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, IntervalValue?> parseIntervalValue,
        out object? result)
        => context.TryEvaluate(
            fn,
            row,
            group,
            ctes,
            tryEvalJsonObjectFunction,
            evalArg,
            eval,
            parseIntervalValue,
            out result);

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        AstQueryGeneralScalarFunctionHandler tryEvalJsonObjectFunction,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<CallExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, IntervalValue?> parseIntervalValue,
        out object? result)
    {
        if (AstQuerySharedTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || tryEvalJsonObjectFunction(context, fn, evalArg, out result)
            || TryEvalGeneralDateArithmeticFunctions(context, fn, evalArg, out result))
        {
            return true;
        }

        if (AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result))
            return true;

        SequenceFunctionSupportHelper.EnsureSupported(context.Dialect, fn.Name);
        if (SqlSequenceEvaluator.TryEvaluateCall(context.Connection, fn.Name, fn.Args, expr => eval(expr, row, group, ctes), out var sequenceValue))
        {
            result = sequenceValue;
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalGeneralDateArithmeticFunctions(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);
}
