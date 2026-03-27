namespace DbSqlLikeMem;

internal static class AstQueryPostgresScalarUtilityFunctionEvaluator
{
    internal static bool TryEvaluatePostgresScalarUtilityFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;
        if (context.Dialect.Functions.TryGetValue(fn.Name, out var handler)
            && handler.AstExecutor != null)
            return handler.AstExecutor(context, fn, evalArg, out result);

        return false;
    }

    internal static void CreateHandlers(
        this QueryExecutionContext context)
    {
        var f = context.Dialect.Functions;
        f.Add("INT", TryEvalNumNullsFunction, "NUM_NULLS");
        f.Add("INT", TryEvalNumNonNullsFunction, "NUM_NONNULLS");
        f.Add("BIGINT", TryEvalLcmFunction, "LCM");
        f.Add("INT", TryEvalMinScaleFunction, "MIN_SCALE");
        f.Add("STRING_ARRAY", TryEvalParseIdentFunction, "PARSE_IDENT");
    }

    private static bool TryEvalNumNullsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        result = Enumerable.Range(0, fn.Args.Count).Count(i => AstQueryExecutorBase.IsNullish(evalArg(i)));
        return true;
    }

    private static bool TryEvalNumNonNullsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        result = Enumerable.Range(0, fn.Args.Count).Count(i => !AstQueryExecutorBase.IsNullish(evalArg(i)));
        return true;
    }

    private static bool TryEvalLcmFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var leftValue = evalArg(0);
        var rightValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(leftValue) || AstQueryExecutorBase.IsNullish(rightValue))
        {
            result = null;
            return true;
        }

        var left = Math.Abs(Convert.ToInt64(leftValue.ToDec(), CultureInfo.InvariantCulture));
        var right = Math.Abs(Convert.ToInt64(rightValue.ToDec(), CultureInfo.InvariantCulture));
        if (left == 0 || right == 0)
        {
            result = 0L;
            return true;
        }

        result = checked((left / AstQueryGeneralScalarFunctionEvaluator.ComputeGreatestCommonDivisor(left, right)) * right);
        return true;
    }

    private static bool TryEvalMinScaleFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = AstQueryGeneralScalarFunctionEvaluator.GetMinimumNumericScale(value!);
        return true;
    }

    private static bool TryEvalParseIdentFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (!AstQueryGeneralScalarFunctionEvaluator.TryParsePostgresIdentifierParts(text, out var parts))
        {
            result = null;
            return true;
        }

        result = parts.ToArray();
        return true;
    }
}
