namespace DbSqlLikeMem;

internal static class AstQueryMySqlConversionAndMetadataFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(context, fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalMySqlConvertFunction, "CONVERT");
        Register(handlers, TryEvalMySqlConvFunction, "CONV");
        Register(handlers, TryEvalMySqlDayNameFunction, "DAYNAME");
        Register(handlers, TryEvalMySqlDayOfMonthFunction, "DAYOFMONTH");
        Register(handlers, TryEvalMySqlDayOfWeekFunction, "DAYOFWEEK");
        Register(handlers, TryEvalMySqlDayOfYearFunction, "DAYOFYEAR");
        Register(handlers, TryEvalMySqlVersionFunction, "VERSION");
        Register(handlers, TryEvalMySqlCurrentDateFunction, "CURDATE", "CURRENT_DATE");
        Register(handlers, TryEvalMySqlUtcDateFunction, "UTC_DATE");
        Register(handlers, TryEvalMySqlCurrentTimeFunction, "CURTIME", "CURRENT_TIME", "LOCALTIME");
        Register(handlers, TryEvalMySqlUtcTimeFunction, "UTC_TIME");
        Register(handlers, TryEvalMySqlLocalTimestampFunction, "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "NOW", "SYSDATE", "SYSTEMDATE");
        Register(handlers, TryEvalMySqlUtcTimestampFunction, "UTC_TIMESTAMP");
        Register(handlers, TryEvalMySqlDatabaseFunction, "DATABASE");
        Register(handlers, TryEvalMySqlSchemaFunction, "SCHEMA");
        Register(handlers, TryEvalMySqlSessionUserFunction, "SESSION_USER");
        Register(handlers, TryEvalMySqlCurrentUserFunction, "CURRENT_USER");
        Register(handlers, TryEvalMySqlUserFunction, "USER");
        Register(handlers, TryEvalMySqlSystemUserFunction, "SYSTEM_USER");
        Register(handlers, TryEvalMySqlConnectionIdFunction, "CONNECTION_ID");
        Register(handlers, TryEvalMySqlCharsetFunction, "CHARSET");
        Register(handlers, TryEvalMySqlCollationFunction, "COLLATION");
        Register(handlers, TryEvalMySqlCoercibilityFunction, "COERCIBILITY");
        return handlers;
    }

    private static bool TryEvalMySqlConvertFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value)
            ? null
            : value is string textValue
                ? textValue
                : value!.ToString();
        return true;
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalMySqlDayNameFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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

        result = CultureInfo.InvariantCulture.DateTimeFormat.GetDayName(date.DayOfWeek);
        return true;
    }

    private static bool TryEvalMySqlDayOfMonthFunction(
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
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value!, out var date))
        {
            result = null;
            return true;
        }

        result = date.Day;
        return true;
    }

    private static bool TryEvalMySqlDayOfWeekFunction(
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
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value!, out var date))
        {
            result = null;
            return true;
        }

        result = (int)date.DayOfWeek + 1;
        return true;
    }

    private static bool TryEvalMySqlDayOfYearFunction(
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
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value!, out var date))
        {
            result = null;
            return true;
        }

        result = date.DayOfYear;
        return true;
    }

    private static bool TryEvalMySqlVersionFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;

        result = $"MySQL {FormatMySqlServerVersion(context.Dialect.Version)}";
        return true;
    }

    private static bool TryEvalMySqlCurrentDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = DateTime.UtcNow.Date;
        return true;
    }

    private static bool TryEvalMySqlUtcDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = DateTime.UtcNow.Date;
        return true;
    }

    private static bool TryEvalMySqlCurrentTimeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = DateTime.UtcNow.TimeOfDay;
        return true;
    }

    private static bool TryEvalMySqlUtcTimeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = DateTime.UtcNow.TimeOfDay;
        return true;
    }

    private static bool TryEvalMySqlLocalTimestampFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = DateTime.UtcNow;
        return true;
    }

    private static bool TryEvalMySqlUtcTimestampFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = DateTime.UtcNow;
        return true;
    }

    private static bool TryEvalMySqlDatabaseFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count != 0)
            throw new InvalidOperationException("DATABASE() nao aceita argumentos.");

        _ = context;
        _ = evalArg;

        result = "DefaultSchema";
        return true;
    }

    private static bool TryEvalMySqlSchemaFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "DefaultSchema";
        return true;
    }

    private static bool TryEvalMySqlSessionUserFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "root@localhost";
        return true;
    }

    private static bool TryEvalMySqlCurrentUserFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "root@localhost";
        return true;
    }

    private static bool TryEvalMySqlUserFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "root@localhost";
        return true;
    }

    private static bool TryEvalMySqlSystemUserFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "root@localhost";
        return true;
    }

    private static bool TryEvalMySqlConnectionIdFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = 1L;
        return true;
    }

    private static bool TryEvalMySqlCharsetFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
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

        result = "utf8mb4";
        return true;
    }

    private static bool TryEvalMySqlCollationFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
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

        result = "utf8mb4_general_ci";
        return true;
    }

    private static bool TryEvalMySqlCoercibilityFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
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

        result = 0;
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
        return new string([.. chars]);
    }
}
