using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQuerySqlServerTemporalAccessorFunctionEvaluator
{
    private delegate bool SqlServerTemporalAccessorFunctionHandler(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result);

    private static readonly IReadOnlyDictionary<string, SqlServerTemporalAccessorFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        result = null;

        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, row, group, ctes, evalArg, getTemporalUnit, resolveTemporalUnit, out result);

        return false;
    }

    private static Dictionary<string, SqlServerTemporalAccessorFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, SqlServerTemporalAccessorFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalDateNameFunction, "DATENAME");
        Register(handlers, TryEvalDatePartFunction, "DATEPART");
        return handlers;
    }

    private static void Register(
        IDictionary<string, SqlServerTemporalAccessorFunctionHandler> handlers,
        SqlServerTemporalAccessorFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalDateNameFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        _ = resolveTemporalUnit;
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DATENAME() espera 2 argumentos.");

        var unitText = GetTemporalUnitText(fn.Args[0], evalArg);
        var unit = SqlServerTemporalUnitHelper.Resolve(unitText);
        if (SqlServerTemporalUnitHelper.IsTimeZoneOffset(unitText))
        {
            var offsetValue = evalArg(1);
            if (!TryResolveTimeZoneOffsetMinutes(offsetValue, out var offsetMinutes))
            {
                result = null;
                return true;
            }

            result = offsetMinutes.ToString(CultureInfo.InvariantCulture);
            return true;
        }

        var value = evalArg(1);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            TemporalUnit.Year => dateTime.Year.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Month => dateTime.ToString("MMMM", CultureInfo.InvariantCulture),
            TemporalUnit.Day => dateTime.Day.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Yearday => dateTime.DayOfYear.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Week => SqlServerTemporalUnitHelper.GetWeekOfYear(dateTime).ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Weekday => SqlServerTemporalUnitHelper.GetWeekdayName(dateTime),
            TemporalUnit.Hour => dateTime.Hour.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Minute => dateTime.Minute.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Second => dateTime.Second.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Millisecond => dateTime.Millisecond.ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Microsecond => ((int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10L)).ToString(CultureInfo.InvariantCulture),
            TemporalUnit.Nanosecond => ((int)((dateTime.Ticks % TimeSpan.TicksPerSecond) * 100L)).ToString(CultureInfo.InvariantCulture),
            _ => null
        };
        return true;
    }

    private static bool TryEvalDatePartFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
        Func<string, TemporalUnit> resolveTemporalUnit,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name == "DATEPART" && fn.Args.Count < 2)
            throw new InvalidOperationException("DATEPART() espera 2 argumentos.");

        var unitText = GetTemporalUnitText(fn.Args[0], evalArg);
        var unit = SqlServerTemporalUnitHelper.Resolve(unitText);
        if (SqlServerTemporalUnitHelper.IsTimeZoneOffset(unitText))
        {
            var offsetValue = evalArg(1);
            if (!TryResolveTimeZoneOffsetMinutes(offsetValue, out var offsetMinutes))
            {
                result = null;
                return true;
            }

            result = offsetMinutes;
            return true;
        }

        var value = evalArg(1);
        if (IsNullish(value) || !TryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = unit switch
        {
            TemporalUnit.Year => dateTime.Year,
            TemporalUnit.Month => dateTime.Month,
            TemporalUnit.Day => dateTime.Day,
            TemporalUnit.Yearday => dateTime.DayOfYear,
            TemporalUnit.Week => SqlServerTemporalUnitHelper.GetWeekOfYear(dateTime),
            TemporalUnit.Weekday => SqlServerTemporalUnitHelper.GetWeekdayIndex(dateTime),
            TemporalUnit.Hour => dateTime.Hour,
            TemporalUnit.Minute => dateTime.Minute,
            TemporalUnit.Second => dateTime.Second,
            TemporalUnit.Millisecond => dateTime.Millisecond,
            TemporalUnit.Microsecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10L),
            TemporalUnit.Nanosecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) * 100L),
            _ => null
        };
        return true;
    }

    private static string GetTemporalUnitText(SqlExpr expr, Func<int, object?> evalArg)
        => expr switch
        {
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => col.Name,
            LiteralExpr lit => lit.Value?.ToString() ?? string.Empty,
            _ => evalArg(0)?.ToString() ?? string.Empty
        };

    private static bool TryResolveTimeZoneOffsetMinutes(object? value, out int offsetMinutes)
    {
        offsetMinutes = 0;

        if (value is null || value is DBNull)
            return false;

        if (value is DateTimeOffset dto)
        {
            offsetMinutes = (int)dto.Offset.TotalMinutes;
            return true;
        }

        if (value is DateTime)
            return true;

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
            return false;

        if (TryParseExplicitTimeZoneOffsetMinutes(text, out offsetMinutes))
            return true;

        return TryCoerceDateTime(value, out _);
    }

    private static bool TryParseExplicitTimeZoneOffsetMinutes(string text, out int offsetMinutes)
    {
        offsetMinutes = 0;

        var trimmed = text.Trim();
        if (trimmed.EndsWith("Z", StringComparison.OrdinalIgnoreCase))
        {
            offsetMinutes = 0;
            return true;
        }

        var separatorIndex = Math.Max(trimmed.LastIndexOf('T'), trimmed.LastIndexOf(' '));
        if (separatorIndex < 0 || separatorIndex + 1 >= trimmed.Length)
            return false;

        var trailing = trimmed[(separatorIndex + 1)..];
        if (string.IsNullOrWhiteSpace(trailing))
            return false;

        trailing = trailing.Trim();
        if (trailing.Length < 3)
            return false;

        var sign = trailing[0] switch
        {
            '+' => 1,
            '-' => -1,
            _ => 0
        };

        if (sign == 0)
            return false;

        var body = trailing[1..];
        if (body.Contains(':'))
        {
            var separatorIndex2 = body.IndexOf(':');
            if (separatorIndex2 <= 0 || separatorIndex2 >= body.Length - 1)
                return false;

            var hoursPart = body[..separatorIndex2];
            var minutesPart = body[(separatorIndex2 + 1)..];
            if (!int.TryParse(hoursPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours)
                || !int.TryParse(minutesPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes))
            {
                return false;
            }

            offsetMinutes = sign * ((hours * 60) + minutes);
            return true;
        }

        if (body.Length == 4
            && int.TryParse(body[..2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var compactHours)
            && int.TryParse(body[2..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var compactMinutes))
        {
            offsetMinutes = sign * ((compactHours * 60) + compactMinutes);
            return true;
        }

        if (int.TryParse(body, NumberStyles.Integer, CultureInfo.InvariantCulture, out var hoursOnly))
        {
            offsetMinutes = sign * (hoursOnly * 60);
            return true;
        }

        return false;
    }
}
