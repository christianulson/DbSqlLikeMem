namespace DbSqlLikeMem;

internal static class QueryConcatFunctionHelper
{
    internal static object? TryEvalConcatFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool nullInputReturnsNull,
        out bool handled)
    {
        handled = true;

        if (TryEvalConcatFunction(fn, evalArg, nullInputReturnsNull, out var concatResult))
            return concatResult;

        if (TryEvalConcatWithSeparatorFunction(fn, evalArg, out var concatWithSeparatorResult))
            return concatWithSeparatorResult;

        handled = false;
        return null;
    }

    private static bool TryEvalConcatFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool nullInputReturnsNull,
        out object? result)
    {
        if (!fn.Name.Equals(SqlConst.CONCAT, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var parts = new string[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
        {
            if (!TryConvertConcatArgument(evalArg(i), nullInputReturnsNull, out var part))
            {
                result = null;
                return true;
            }

            parts[i] = part;
        }

        result = string.Concat(parts);
        return true;
    }

    private static bool TryEvalConcatWithSeparatorFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals(SqlConst.CONCAT_WS, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var separatorValue = evalArg(0);
        if (IsNullish(separatorValue))
        {
            result = null;
            return true;
        }

        var parts = new List<string>();
        for (var i = 1; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            if (!IsNullish(value))
                parts.Add(value?.ToString() ?? string.Empty);
        }

        result = string.Join(separatorValue?.ToString() ?? string.Empty, parts);
        return true;
    }

    private static bool TryConvertConcatArgument(
        object? value,
        bool nullInputReturnsNull,
        out string part)
    {
        if (IsNullish(value))
        {
            part = string.Empty;
            return !nullInputReturnsNull;
        }

        part = value?.ToString() ?? string.Empty;
        return true;
    }

    private static bool IsNullish(object? value)
        => value is null or DBNull;
}
