namespace DbSqlLikeMem;

internal static class AstQuerySqliteDateFunctionEvaluator
{
    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalStrftimeFunction, "STRFTIME");
        Register(handlers, TryEvalJulianDayFunction, "JULIANDAY");
        Register(handlers, TryEvalUnixEpochFunction, "UNIXEPOCH");

        return handlers;
    }

    private static void Register(
        Dictionary<string, AstQueryGeneralScalarFunctionHandler> handlers,
        AstQueryGeneralScalarFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers.Add(name, handler);
    }

    private static bool TryEvalStrftimeFunction(
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

        var format = evalArg(0)?.ToString() ?? string.Empty;
        if (!TryResolveSqliteStrftimeBaseDateTime(fn, evalArg, out var dateTime))
        {
            result = null;
            return true;
        }

        ApplySqliteStrftimeModifiers(fn, evalArg, ref dateTime);

        result = FormatSqliteStrftime(format, dateTime);
        return true;
    }

    private static bool TryResolveSqliteStrftimeBaseDateTime(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out DateTime dateTime)
    {
        if (fn.Args.Count <= 1)
        {
            dateTime = DateTime.Now;
            return true;
        }

        var baseValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(baseValue))
        {
            dateTime = default;
            return false;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var parsed))
        {
            dateTime = parsed;
            return true;
        }

        if (AstQueryExecutorBase.TryConvertNumericToDouble(baseValue, out var epoch))
        {
            dateTime = DateTimeOffset.FromUnixTimeSeconds((long)epoch).DateTime;
            return true;
        }

        dateTime = default;
        return false;
    }

    private static void ApplySqliteStrftimeModifiers(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        ref DateTime dateTime)
    {
        for (var i = 2; i < fn.Args.Count; i++)
        {
            var modifier = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(modifier))
                continue;

            if (modifier!.Equals("utc", StringComparison.OrdinalIgnoreCase))
            {
                dateTime = dateTime.ToUniversalTime();
                continue;
            }

            if (modifier.Equals("localtime", StringComparison.OrdinalIgnoreCase))
            {
                dateTime = dateTime.ToLocalTime();
                continue;
            }

            if (modifier.Equals("unixepoch", StringComparison.OrdinalIgnoreCase))
            {
                dateTime = DateTimeOffset.FromUnixTimeSeconds((long)dateTime.ToUniversalTime().Subtract(AstQueryExecutorBase._unixEpoch).TotalSeconds).DateTime;
                continue;
            }

            if (AstQueryExecutorBase.TryParseDateModifier(modifier!, out var unit, out var amount))
                dateTime = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, amount);
        }
    }

    private static string FormatSqliteStrftime(string format, DateTime dateTime)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            builder.Append(token switch
            {
                'Y' => dateTime.ToString("yyyy", CultureInfo.InvariantCulture),
                'm' => dateTime.ToString("MM", CultureInfo.InvariantCulture),
                'd' => dateTime.ToString("dd", CultureInfo.InvariantCulture),
                'H' => dateTime.ToString("HH", CultureInfo.InvariantCulture),
                'M' => dateTime.ToString("mm", CultureInfo.InvariantCulture),
                'S' => dateTime.ToString("ss", CultureInfo.InvariantCulture),
                'f' => dateTime.ToString("ss.fff", CultureInfo.InvariantCulture),
                's' => new DateTimeOffset(dateTime.ToUniversalTime()).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture),
                'J' => (dateTime.ToOADate() + 2415018.5d).ToString("0.000000", CultureInfo.InvariantCulture),
                '%' => "%",
                _ => $"%{token}"
            });
        }

        return builder.ToString();
    }

    private static bool TryEvalJulianDayFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 1)
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = dateTime.ToOADate() + 2415018.5d;
        return true;
    }

    private static bool TryEvalUnixEpochFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = fn.Args.Count > 0 ? evalArg(0) : null;
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = (long)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            return true;
        }

        DateTime dateTime;
        if (value is string textValue)
        {
            if (!DateTime.TryParse(
                    textValue,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out dateTime))
            {
                result = null;
                return true;
            }
        }
        else if (!AstQueryExecutorBase.TryCoerceDateTime(value, out dateTime))
        {
            result = null;
            return true;
        }

        if (dateTime.Kind == DateTimeKind.Unspecified)
            dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);

        result = new DateTimeOffset(dateTime.ToUniversalTime()).ToUnixTimeSeconds();
        return true;
    }
}
