namespace DbSqlLikeMem;

internal static class AstQueryMySqlDateFunctionEvaluator
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

        Register(handlers, TryEvalMakeDateFunction, "MAKEDATE");
        Register(handlers, TryEvalMakeTimeFunction, "MAKETIME");
        Register(handlers, TryEvalMicrosecondFunction, "MICROSECOND");
        Register(handlers, TryEvalMonthNameFunction, "MONTHNAME");
        Register(handlers, TryEvalPeriodFunctions, "PERIOD_ADD", "PERIOD_DIFF");
        Register(handlers, TryEvalQuarterFunction, "QUARTER");
        Register(handlers, TryEvalLastDayFunction, "LAST_DAY");
        Register(handlers, TryEvalSubDateFunction, "SUBDATE");
        Register(handlers, TryEvalSecToTimeFunction, "SEC_TO_TIME");

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

    private static bool TryEvalMakeDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isAdd = string.Equals(fn.Name, "PERIOD_ADD", StringComparison.OrdinalIgnoreCase);
        var isDiff = string.Equals(fn.Name, "PERIOD_DIFF", StringComparison.OrdinalIgnoreCase);

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
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = ((dateTime.Month - 1) / 3) + 1;
        return true;
    }

    private static bool TryEvalLastDayFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        var lastDay = DateTime.DaysInMonth(dateTime.Year, dateTime.Month);
        result = new DateTime(dateTime.Year, dateTime.Month, lastDay, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Kind);
        return true;
    }

    private static bool TryEvalSecToTimeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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

    private static bool TryEvalSubDateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("SUBDATE() espera data e intervalo.");

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var intervalValue = evalArg(1);
        if (intervalValue is IntervalValue interval)
        {
            result = dateTime.Subtract(interval.Span);
            return true;
        }

        if (AstQueryExecutorBase.TryConvertNumericToDouble(intervalValue, out var dayOffset))
        {
            result = dateTime.AddDays(-dayOffset);
            return true;
        }

        result = null;
        return true;
    }
}
