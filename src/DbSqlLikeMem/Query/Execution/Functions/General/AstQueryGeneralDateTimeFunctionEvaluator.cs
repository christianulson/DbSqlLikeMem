using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryGeneralDateTimeFunctionEvaluator
{
    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, context, evalArg, out result);

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalTimeFormatFunction, "TIME_FORMAT");
        Register(handlers, TryEvalTimeToSecFunction, "TIME_TO_SEC");
        Register(handlers, TryEvalTimeDiffFunction, "TIMEDIFF");
        Register(handlers, TryEvalToDaysFunction, "TO_DAYS");
        Register(handlers, TryEvalToSecondsFunction, "TO_SECONDS");
        Register(handlers, TryEvalTruncateFunction, "TRUNCATE");
        Register(handlers, TryEvalUnixTimestampFunction, "UNIX_TIMESTAMP", "UNIXEPOCH");
        Register(handlers, TryEvalWeekFunctions, "WEEK", "WEEKDAY", "WEEKOFYEAR", "YEARWEEK");

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

    private static bool TryEvalTimeFormatFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIME_FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var format = evalArg(1)?.ToString() ?? string.Empty;
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var isDateTime = TryCoerceDateTime(value, out var dateTime);
        var isTimeSpan = TryCoerceTimeSpan(value, out var timeSpan);
        if (!isDateTime && !isTimeSpan)
        {
            result = null;
            return true;
        }

        var formatNet = ConvertMySqlTimeFormat(format);
        var formatted = isDateTime
            ? dateTime.ToString(formatNet, CultureInfo.InvariantCulture)
            : DateTime.Today.Add(timeSpan).ToString(formatNet, CultureInfo.InvariantCulture);

        result = formatted;
        return true;
    }

    internal static string ConvertMySqlTimeFormat(string format)
    {
        if (string.IsNullOrEmpty(format))
            return format;

        var sb = new StringBuilder();
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                sb.Append(ch);
                continue;
            }

            var token = format[i + 1];
            i++;
            sb.Append(token switch
            {
                'H' => "HH",
                'k' => "H",
                'h' => "hh",
                'I' => "hh",
                'l' => "h",
                'i' => "mm",
                's' => "ss",
                'S' => "ss",
                'f' => "ffffff",
                'p' => "tt",
                'r' => "hh:mm:ss tt",
                'T' => "HH:mm:ss",
                _ => $"%{token}"
            });
        }

        return sb.ToString();
    }

    private static bool TryEvalTimeToSecFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIME_TO_SEC", StringComparison.OrdinalIgnoreCase))
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

        if (TryCoerceTimeSpan(value, out var span))
        {
            result = (long)span.TotalSeconds;
            return true;
        }

        if (TryCoerceDateTime(value, out var dateTime))
        {
            result = (long)dateTime.TimeOfDay.TotalSeconds;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalTimeDiffFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TIMEDIFF", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        if (TryCoerceDateTime(left, out var leftDate) && TryCoerceDateTime(right, out var rightDate))
        {
            result = leftDate - rightDate;
            return true;
        }

        if (TryCoerceTimeSpan(left, out var leftSpan) && TryCoerceTimeSpan(right, out var rightSpan))
        {
            result = leftSpan - rightSpan;
            return true;
        }

        result = null;
        return true;
    }



    private static bool TryEvalToDaysFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TO_DAYS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var days = (int)(dateTime.Date - DateTime.MinValue.Date).TotalDays + 1;
        result = days;
        return true;
    }

    private static bool TryEvalToSecondsFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TO_SECONDS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var seconds = (long)(dateTime.ToUniversalTime() - DateTime.MinValue.ToUniversalTime()).TotalSeconds + 1;
        result = seconds;
        return true;
    }

    private static bool TryEvalTruncateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TRUNCATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var decimalsValue = evalArg(1);
        if (IsNullish(value) || IsNullish(decimalsValue))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            var decimals = Convert.ToInt32(decimalsValue.ToDec());
            var factor = (decimal)Math.Pow(10d, decimals);
            result = Math.Truncate(number * factor) / factor;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalUnixTimestampFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("UNIX_TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("UNIXEPOCH", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = fn.Args.Count > 0 ? evalArg(0) : null;
        if (IsNullish(value))
        {
            result = (long)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            return true;
        }

        DateTime dateTime;
        if (value is string textValue)
        {
            if (!TryParseCachedDateTime(
                    textValue,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out dateTime))
            {
                result = null;
                return true;
            }
        }
        else if (!TryCoerceDateTime(value, out dateTime))
        {
            result = null;
            return true;
        }

        if (dateTime.Kind == DateTimeKind.Unspecified)
            dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);

        result = new DateTimeOffset(dateTime.ToUniversalTime()).ToUnixTimeSeconds();
        return true;
    }

    private static bool TryEvalWeekFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        if (!(name.Equals("WEEK", StringComparison.OrdinalIgnoreCase)
            || name.Equals("WEEKDAY", StringComparison.OrdinalIgnoreCase)
            || name.Equals("WEEKOFYEAR", StringComparison.OrdinalIgnoreCase)
            || name.Equals("YEARWEEK", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        if (name.Equals("WEEKDAY", StringComparison.OrdinalIgnoreCase))
        {
            var weekday = ((int)dateTime.DayOfWeek + 6) % 7;
            result = weekday;
            return true;
        }

        if (name.Equals("WEEKOFYEAR", StringComparison.OrdinalIgnoreCase))
        {
            var week = GetIsoWeekOfYear(dateTime);
            result = week;
            return true;
        }

        if (name.Equals("YEARWEEK", StringComparison.OrdinalIgnoreCase))
        {
            var week = GetIsoWeekOfYear(dateTime);
            var year = GetIsoWeekYear(dateTime);
            result = year * 100 + week;
            return true;
        }

        // MySQL WEEK(date) default mode is 0: Sunday-first, range 0-53.
        var firstDayOfYear = new DateTime(dateTime.Year, 1, 1);
        var dayOffset = (int)firstDayOfYear.DayOfWeek;
        var dayOfYearZeroBased = dateTime.DayOfYear - 1;
        result = (dayOfYearZeroBased + dayOffset) / 7;
        return true;
    }

}
