using System.Globalization;

namespace DbSqlLikeMem;

internal static class QueryConcatFunctionHelper
{
    internal static bool TryEvalConcatFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Name.Equals("CONCAT_WS", StringComparison.OrdinalIgnoreCase))
            return TryEvalConcatWithSeparatorFunction(fn, evalArg, out result);

        if (fn.Name.Equals("CONCAT", StringComparison.OrdinalIgnoreCase)
            && context.TryEvalConcatFunction(fn, evalArg, out result))
            return true;

        result = null;
        return false;
    }

    private static bool TryEvalConcatFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var parts = new string[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
        {
            if (!context.TryConvertConcatArgument(evalArg(i), out var part))
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
                parts.Add(ToConcatText(value));
        }

        result = string.Join(ToConcatText(separatorValue), parts);
        return true;
    }

    private static bool TryConvertConcatArgument(
        this QueryExecutionContext context,
        object? value,
        out string part)
    {
        if (IsNullish(value))
        {
            part = string.Empty;
            return !context.Dialect.ConcatFunctionReturnsNullOnNullInput;
        }

        part = ToConcatText(value);
        return true;
    }

    private static bool IsNullish(object? value)
        => value is null or DBNull;

    private static string ToConcatText(object? value)
    {
        if (value is null || value is DBNull)
            return string.Empty;

        if (value is IFormattable formattable)
            return formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty;

        return value.ToString() ?? string.Empty;
    }
}

