namespace DbSqlLikeMem;

internal delegate bool TryCoerceDateTimeDelegate(object? value, out DateTime result);

internal static class QueryOracleDb2ScalarFunctionHelper
{
    private delegate bool OracleDb2CoreFunctionHandler(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result);

    private static readonly IReadOnlyDictionary<string, OracleDb2CoreFunctionHandler> _handlers =
        CreateHandlers();

    public static bool TryEvalCoreFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalCoreFunctions(
            fn,
            dialect,
            evalArg,
            AstQueryExecutorBase.TryCoerceDateTime,
            out result);

    public static bool TryEvalCoreFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, dialect, evalArg, tryCoerceDateTime, out result);

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (!fn.Name.Equals("ADD_MONTHS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (!fn.Name.Equals("ASCIISTR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (!fn.Name.Equals("BIN_TO_NUM", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (!fn.Name.Equals("BITAND", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (!fn.Name.Equals("BITOR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (!fn.Name.Equals("BITXOR", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (!fn.Name.Equals("BITNOT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate? tryCoerceDateTime,
        out object? result)
    {
        _ = tryCoerceDateTime;
        if (!fn.Name.Equals("BITANDNOT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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

    private static bool IsNullish(object? value) => value is null or DBNull;
}
