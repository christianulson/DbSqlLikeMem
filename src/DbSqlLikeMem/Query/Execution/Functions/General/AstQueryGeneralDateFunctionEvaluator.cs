namespace DbSqlLikeMem;

internal static class AstQueryGeneralDateFunctionEvaluator
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

        Register(handlers, TryEvalDateConstructionFunction, "DATE", "TIMESTAMP", "DATETIME", "TIME");
        Register(handlers, TryEvalStrftimeFunction, "STRFTIME");
        Register(handlers, TryEvalMakeDateFunction, "MAKEDATE");
        Register(handlers, TryEvalMakeTimeFunction, "MAKETIME");
        Register(handlers, TryEvalMicrosecondFunction, "MICROSECOND");
        Register(handlers, TryEvalMonthNameFunction, "MONTHNAME");
        Register(handlers, TryEvalPeriodFunctions, "PERIOD_ADD", "PERIOD_DIFF");
        Register(handlers, TryEvalQuarterFunction, "QUARTER");
        Register(handlers, TryEvalSecToTimeFunction, "SEC_TO_TIME");
        Register(handlers, TryEvalJulianDayFunction, "JULIANDAY");
        Register(handlers, TryEvalTruncFunction, "TRUNC");

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

    private static bool TryEvalDateConstructionFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("DATETIME", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TIME", StringComparison.OrdinalIgnoreCase))
            || fn.Args.Count < 1)
        {
            result = null;
            return false;
        }

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        for (var i = 1; i < fn.Args.Count; i++)
        {
            var modifier = evalArg(i)?.ToString();
            if (string.IsNullOrWhiteSpace(modifier)
                || !AstQueryExecutorBase.TryParseDateModifier(modifier!, out var unit, out var amount))
            {
                continue;
            }

            dateTime = AstQueryExecutorBase.ApplyDateDelta(dateTime, unit, amount);
        }

        if (fn.Name.Equals("TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = dateTime.TimeOfDay;
            return true;
        }

        result = fn.Name.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            ? dateTime.Date
            : dateTime;
        return true;
    }

    private static bool TryEvalStrftimeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STRFTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var format = evalArg(0)?.ToString() ?? string.Empty;
        DateTime dateTime;
        if (fn.Args.Count > 1)
        {
            var baseValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(baseValue))
            {
                result = null;
                return true;
            }

            if (AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var parsed))
            {
                dateTime = parsed;
            }
            else if (AstQueryExecutorBase.TryConvertNumericToDouble(baseValue, out var epoch))
            {
                dateTime = DateTimeOffset.FromUnixTimeSeconds((long)epoch).DateTime;
            }
            else
            {
                result = null;
                return true;
            }
        }
        else
        {
            dateTime = DateTime.Now;
        }

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

        result = FormatSqliteStrftime(format, dateTime);
        return true;
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

    private static bool TryEvalMakeDateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MAKEDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MAKEDATE() espera ano e dia do ano.");

        var yearValue = evalArg(0);
        var dayValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(yearValue) || AstQueryExecutorBase.IsNullish(dayValue))
        {
            result = null;
            return true;
        }

        var year = Convert.ToInt32(yearValue.ToDec());
        var dayOfYear = Convert.ToInt32(dayValue.ToDec());
        if (dayOfYear <= 0)
        {
            result = null;
            return true;
        }

        try
        {
            result = new DateTime(year, 1, 1).AddDays(dayOfYear - 1);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMakeTimeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MAKETIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("MAKETIME() espera hora, minuto e segundo.");

        var hourValue = evalArg(0);
        var minuteValue = evalArg(1);
        var secondValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(hourValue) || AstQueryExecutorBase.IsNullish(minuteValue) || AstQueryExecutorBase.IsNullish(secondValue))
        {
            result = null;
            return true;
        }

        try
        {
            var hours = Convert.ToInt32(hourValue.ToDec());
            var minutes = Convert.ToInt32(minuteValue.ToDec());
            var seconds = Convert.ToDouble(secondValue, CultureInfo.InvariantCulture);
            var secondsInt = (int)Math.Truncate(seconds);
            var microseconds = (int)Math.Round((seconds - secondsInt) * 1_000_000d);
            var time = new TimeSpan(0, hours, minutes, secondsInt, 0)
                .Add(TimeSpan.FromTicks(microseconds * 10L));
            result = time;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMicrosecondFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MICROSECOND", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            var micro = (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10);
            result = micro;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceTimeSpan(value, out var span))
        {
            var micro = (int)((span.Ticks % TimeSpan.TicksPerSecond) / 10);
            result = micro;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalMonthNameFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MONTHNAME", StringComparison.OrdinalIgnoreCase))
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

        result = dateTime.ToString("MMMM", CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalPeriodFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isAdd = fn.Name.Equals("PERIOD_ADD", StringComparison.OrdinalIgnoreCase);
        var isDiff = fn.Name.Equals("PERIOD_DIFF", StringComparison.OrdinalIgnoreCase);
        if (!isAdd && !isDiff)
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera dois argumentos.");

        var periodValue = evalArg(0);
        var secondValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(periodValue) || AstQueryExecutorBase.IsNullish(secondValue))
        {
            result = null;
            return true;
        }

        if (!TryParsePeriodValue(periodValue!, out var year, out var month))
        {
            result = null;
            return true;
        }

        if (isAdd)
        {
            var delta = Convert.ToInt32(secondValue.ToDec());
            var totalMonths = year * 12 + (month - 1) + delta;
            var newYear = totalMonths / 12;
            var newMonth = (totalMonths % 12) + 1;
            result = newYear * 100 + newMonth;
            return true;
        }

        if (!TryParsePeriodValue(secondValue!, out var otherYear, out var otherMonth))
        {
            result = null;
            return true;
        }

        var diff = (year * 12 + (month - 1)) - (otherYear * 12 + (otherMonth - 1));
        result = diff;
        return true;
    }

    private static bool TryParsePeriodValue(object value, out int year, out int month)
    {
        year = 0;
        month = 0;

        try
        {
            var num = Convert.ToInt32(value, CultureInfo.InvariantCulture);
            var abs = Math.Abs(num);
            year = abs / 100;
            month = abs % 100;
            if (month is < 1 or > 12)
                return false;

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryEvalQuarterFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("QUARTER", StringComparison.OrdinalIgnoreCase))
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

        result = ((dateTime.Month - 1) / 3) + 1;
        return true;
    }

    private static bool TryEvalSecToTimeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("SEC_TO_TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var seconds = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = TimeSpan.FromSeconds(seconds);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalJulianDayFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JULIANDAY", StringComparison.OrdinalIgnoreCase)
            || fn.Args.Count < 1)
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

    private static bool TryEvalTruncFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("TRUNC", StringComparison.OrdinalIgnoreCase)
            || fn.Args.Count < 1)
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is DateTime or DateTimeOffset)
        {
            AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime);
            result = dateTime.Date;
            return true;
        }

        try
        {
            var dec = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            result = Math.Truncate(dec);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }
}
