namespace DbSqlLikeMem;

internal static class AstQueryMySqlConversionAndMetadataFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(fn, dialect, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalMySqlConvFunction, "CONV");
        Register(handlers, TryEvalMySqlDayFunctions, "DAYNAME", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR");
        Register(handlers, TryEvalMySqlVersionFunction, "VERSION");
        Register(handlers, TryEvalMySqlCurrentDateFunction, "CURDATE", "CURRENT_DATE");
        Register(handlers, TryEvalMySqlUtcDateFunction, "UTC_DATE");
        Register(handlers, TryEvalMySqlCurrentTimeFunction, "CURTIME", "CURRENT_TIME", "LOCALTIME");
        Register(handlers, TryEvalMySqlUtcTimeFunction, "UTC_TIME");
        Register(handlers, TryEvalMySqlLocalTimestampFunction, "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "NOW", "SYSDATE", "SYSTEMDATE");
        Register(handlers, TryEvalMySqlUtcTimestampFunction, "UTC_TIMESTAMP");
        Register(handlers, TryEvalMySqlDatabaseFunctions, "DATABASE", "SCHEMA", "SESSION_USER", "CURRENT_USER", "USER", "SYSTEM_USER", "CONNECTION_ID");
        Register(handlers, TryEvalMySqlStringMetadataFunctions, "CHARSET", "COLLATION", "COERCIBILITY");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryGeneralScalarFunctionHandler> handlers,
        AstQueryGeneralScalarFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalMySqlConvFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CONV", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("CONV() espera valor, base origem e base destino.");

        var value = evalArg(0);
        var fromBaseValue = evalArg(1);
        var toBaseValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(value) || AstQueryExecutorBase.IsNullish(fromBaseValue) || AstQueryExecutorBase.IsNullish(toBaseValue))
        {
            result = null;
            return true;
        }

        if (!AstQueryExecutorBase.TryConvertNumericToInt64(fromBaseValue!, out var fromBase)
            || !AstQueryExecutorBase.TryConvertNumericToInt64(toBaseValue!, out var toBase))
        {
            result = null;
            return true;
        }

        var sourceBase = (int)Math.Abs(fromBase);
        if (sourceBase < 2)
            sourceBase = 2;
        else if (sourceBase > 36)
            sourceBase = 36;

        var targetBase = (int)Math.Abs(toBase);
        if (targetBase < 2)
            targetBase = 2;
        else if (targetBase > 36)
            targetBase = 36;

        var textValue = Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.IsNullOrWhiteSpace(textValue))
        {
            result = null;
            return true;
        }

        if (!TryParseBaseN(textValue!.Trim(), sourceBase, out var parsed))
        {
            result = null;
            return true;
        }

        result = ConvertToBaseN(parsed, targetBase);
        return true;
    }

    private static bool TryEvalMySqlDayFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("DAYNAME" or "DAYOFMONTH" or "DAYOFWEEK" or "DAYOFYEAR"))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value!, out var date))
        {
            result = null;
            return true;
        }

        result = name switch
        {
            "DAYNAME" => CultureInfo.InvariantCulture.DateTimeFormat.GetDayName(date.DayOfWeek),
            "DAYOFMONTH" => date.Day,
            "DAYOFWEEK" => ((int)date.DayOfWeek + 1),
            "DAYOFYEAR" => date.DayOfYear,
            _ => null
        };
        return true;
    }

    private static bool TryEvalMySqlVersionFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        result = $"MySQL {FormatMySqlServerVersion(dialect.Version)}";
        return true;
    }

    private static bool TryEvalMySqlCurrentDateFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        result = DateTime.Now.Date;
        return true;
    }

    private static bool TryEvalMySqlUtcDateFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow.Date;
        return true;
    }

    private static bool TryEvalMySqlCurrentTimeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        result = DateTime.Now.TimeOfDay;
        return true;
    }

    private static bool TryEvalMySqlUtcTimeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow.TimeOfDay;
        return true;
    }

    private static bool TryEvalMySqlLocalTimestampFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        result = DateTime.Now;
        return true;
    }

    private static bool TryEvalMySqlUtcTimestampFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow;
        return true;
    }

    private static bool TryEvalMySqlDatabaseFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("DATABASE" or "SCHEMA" or "SESSION_USER" or "CURRENT_USER" or "USER" or "SYSTEM_USER" or "CONNECTION_ID"))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count != 0)
            throw new InvalidOperationException($"{name}() nao aceita argumentos.");

        result = name switch
        {
            "DATABASE" or "SCHEMA" => "DefaultSchema",
            "SESSION_USER" => "root@localhost",
            "CURRENT_USER" => "root@localhost",
            "USER" => "root@localhost",
            "SYSTEM_USER" => "root@localhost",
            "CONNECTION_ID" => 1L,
            _ => null
        };
        return true;
    }

    private static bool TryEvalMySqlStringMetadataFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("CHARSET" or "COLLATION" or "COERCIBILITY"))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

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

        result = name switch
        {
            "CHARSET" => "utf8mb4",
            "COLLATION" => "utf8mb4_general_ci",
            "COERCIBILITY" => 0,
            _ => null
        };
        return true;
    }

    private static string FormatMySqlServerVersion(int version)
        => version switch
        {
            30 => "3.0",
            40 => "4.0",
            55 => "5.5",
            56 => "5.6",
            57 => "5.7",
            80 => "8.0",
            84 => "8.4",
            _ => version.ToString(CultureInfo.InvariantCulture),
        };

    private static bool TryParseBaseN(string text, int radix, out long value)
    {
        value = 0;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var negative = text[0] == '-';
        var start = negative ? 1 : 0;
        long resultValue = 0;

        for (var i = start; i < text.Length; i++)
        {
            var ch = char.ToUpperInvariant(text[i]);
            int digit;
            if (ch >= '0' && ch <= '9')
                digit = ch - '0';
            else if (ch >= 'A' && ch <= 'Z')
                digit = ch - 'A' + 10;
            else
                return false;

            if (digit >= radix)
                return false;

            resultValue = checked(resultValue * radix + digit);
        }

        value = negative ? -resultValue : resultValue;
        return true;
    }

    private static string ConvertToBaseN(long value, int radix)
    {
        if (value == 0)
            return "0";

        var negative = value < 0;
        var working = Math.Abs(value);
        var chars = new List<char>();
        const string digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        while (working > 0)
        {
            var rem = (int)(working % radix);
            chars.Add(digits[rem]);
            working /= radix;
        }

        if (negative)
            chars.Add('-');

        chars.Reverse();
        return new string(chars.ToArray());
    }
}
