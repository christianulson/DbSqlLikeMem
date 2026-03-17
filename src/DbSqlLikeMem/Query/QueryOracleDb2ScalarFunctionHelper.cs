namespace DbSqlLikeMem;

internal delegate bool TryCoerceDateTimeDelegate(object? value, out DateTime result);

internal static class QueryOracleDb2ScalarFunctionHelper
{
    public static bool TryEvalCoreFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        return TryEvalAddMonthsFunction(fn, dialect, evalArg, tryCoerceDateTime, out result)
            || TryEvalAsciiStrFunction(fn, dialect, evalArg, out result)
            || TryEvalBinToNumFunction(fn, dialect, evalArg, out result)
            || TryEvalBitAndFunction(fn, dialect, evalArg, out result)
            || TryEvalBitOrFunction(fn, dialect, evalArg, out result)
            || TryEvalBitXorFunction(fn, dialect, evalArg, out result)
            || TryEvalBitNotFunction(fn, dialect, evalArg, out result)
            || TryEvalBitAndNotFunction(fn, dialect, evalArg, out result);
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
        out object? result)
    {
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
        out object? result)
    {
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
        out object? result)
    {
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
        out object? result)
    {
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
        out object? result)
    {
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
        out object? result)
    {
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
        out object? result)
    {
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
