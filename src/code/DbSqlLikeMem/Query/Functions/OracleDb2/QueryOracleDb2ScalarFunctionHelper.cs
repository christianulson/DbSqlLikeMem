using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal delegate bool TryCoerceDateTimeDelegate(object? value, out DateTime result);

internal static class QueryOracleDb2ScalarFunctionHelper
{
    private delegate bool OracleDb2CoreFunctionHandler(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result);

    private static readonly IReadOnlyDictionary<string, OracleDb2CoreFunctionHandler> _handlers =
        CreateHandlers();

    public static bool TryEvalCoreFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalCoreFunctions(
            context,
            fn,
            evalArg,
            TryCoerceDateTime,
            out result);

    public static bool TryEvalCoreFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, tryCoerceDateTime, out result);

        result = null;
        return false;
    }

    private static Dictionary<string, OracleDb2CoreFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, OracleDb2CoreFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalAddMonthsFunction, "ADD_MONTHS");
        Register(handlers, TryEvalAsciiStrFunction, "ASCIISTR");
        Register(handlers, TryEvalBinToNumFunction, "BIN_TO_NUM");
        Register(handlers, TryEvalBitAndFunction, "BITAND");
        Register(handlers, TryEvalBitOrFunction, "BITOR");
        Register(handlers, TryEvalBitXorFunction, "BITXOR");
        Register(handlers, TryEvalBitNotFunction, "BITNOT");
        Register(handlers, TryEvalBitAndNotFunction, "BITANDNOT");
        Register(handlers, TryEvalTruncFunction, "TRUNC", "TRUNCATE");
        return handlers;
    }

    private static void Register(
        IDictionary<string, OracleDb2CoreFunctionHandler> handlers,
        OracleDb2CoreFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalAddMonthsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ADD_MONTHS() espera data e quantidade de meses.");

        var baseValue = evalArg(0);
        var monthsValue = evalArg(1);
        if (IsNullish(baseValue) || IsNullish(monthsValue))
        {
            result = null;
            return true;
        }

        if (!tryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        try
        {
            var months = Convert.ToInt32(monthsValue.ToDec());
            result = dateTime.AddMonths(months);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAsciiStrFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var builder = new StringBuilder(text.Length);
        foreach (var ch in text)
        {
            if (ch <= 0x7F)
            {
                builder.Append(ch);
                continue;
            }

            builder.Append('\\');
            builder.Append(((int)ch).ToString("X4", CultureInfo.InvariantCulture));
        }

        result = builder.ToString();
        return true;
    }

    private static bool TryEvalBinToNumFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        try
        {
            long acc = 0;
            for (var i = 0; i < fn.Args.Count; i++)
            {
                var bitValue = evalArg(i);
                if (IsNullish(bitValue))
                {
                    result = null;
                    return true;
                }

                var bit = Convert.ToInt32(bitValue.ToDec(), CultureInfo.InvariantCulture);
                acc = (acc << 1) | (bit != 0 ? 1L : 0L);
            }

            result = acc;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitAndFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BITAND() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            var r = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            result = l & r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitOrFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BITOR() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            var r = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            result = l | r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitXorFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BITXOR() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            var r = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            result = l ^ r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitNotFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (fn.Args.Count < 1)
            throw new InvalidOperationException("BITNOT() espera 1 argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var v = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            result = ~v;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitAndNotFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BITANDNOT() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            var r = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            result = l & ~r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalTruncFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = context;
        _ = tryCoerceDateTime;

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (fn.Args.Count < 2)
            {
                result = decimal.Truncate(number);
                return true;
            }

            var decimalsValue = evalArg(1);
            if (IsNullish(decimalsValue))
            {
                result = null;
                return true;
            }

            var decimals = Convert.ToInt32(decimalsValue.ToDec());
            var factor = (decimal)Math.Pow(10d, decimals);
            result = decimal.Truncate(number * factor) / factor;
            return true;
        }
        catch
        {
            var coerceDateTime = tryCoerceDateTime ?? TryCoerceDateTime;
            if (coerceDateTime(value, out var dateTime))
            {
                if (fn.Args.Count < 2)
                {
                    result = TruncateDateTime(dateTime, TemporalUnit.Day);
                    return true;
                }

                var unitText = evalArg(1)?.ToString();
                if (string.IsNullOrWhiteSpace(unitText))
                {
                    result = null;
                    return true;
                }

                result = TruncateDateTime(dateTime, AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText!));
                return true;
            }

            result = null;
            return true;
        }
    }

    private static bool IsNullish(object? value) => value is null or DBNull;
}
