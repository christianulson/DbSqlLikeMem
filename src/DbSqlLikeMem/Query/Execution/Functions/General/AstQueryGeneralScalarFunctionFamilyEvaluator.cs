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
        AstQueryTryEvalGeneralSystemAndJsonFunction tryEvalGeneralSystemAndJsonFunction,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<CallExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, IntervalValue?> parseIntervalValue,
        out object? result)
        => context.TryEvaluate(
            fn,
            row,
            group,
            ctes,
            tryEvalGeneralSystemAndJsonFunction,
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
        AstQueryTryEvalGeneralSystemAndJsonFunction tryEvalGeneralSystemAndJsonFunction,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Func<CallExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, IntervalValue?> parseIntervalValue,
        out object? result)
    {
        if (tryEvalGeneralSystemAndJsonFunction(context, fn, evalArg, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction(context, fn, evalArg, out result)
            || TryEvalGeneralDateAndTimeFunctions(fn, row, group, ctes, context, evalArg, parseIntervalValue, out result))
        {
            return true;
        }

        SequenceFunctionSupportHelper.EnsureSupported(context.Dialect, fn.Name);
        if (SqlSequenceEvaluator.TryEvaluateCall(context.Connection, fn.Name, fn.Args, expr => eval(expr, row, group, ctes), out var sequenceValue))
        {
            result = sequenceValue;
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalGeneralDateAndTimeFunctions(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<CallExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, IntervalValue?> parseIntervalValue,
        out object? result)
    {
        if (AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result))
        {
            return true;
        }

        if (AstQueryGeneralScalarFunctionEvaluator.TryEvalSubDateFunction(fn, row, group, ctes, parseIntervalValue, evalArg, out result))
        {
            return true;
        }

        return AstQueryGeneralDateTimeFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);
    }
}
