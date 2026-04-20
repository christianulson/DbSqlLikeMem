using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem.SqlServer;

internal static class SqlServerTemporalUnitHelper
{
    internal static TemporalUnit Resolve(string unitText)
    {
        var sharedUnit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText);
        if (sharedUnit != TemporalUnit.Unknown)
            return sharedUnit;

        if (string.IsNullOrWhiteSpace(unitText))
            return TemporalUnit.Unknown;

        return unitText.Trim().ToUpperInvariant() switch
        {
            "DAYOFYEAR" or "DY" or "Y" => TemporalUnit.Yearday,
            "WEEK" or "WEEKS" or "WK" or "WW" => TemporalUnit.Week,
            "WEEKDAY" or "DW" or "W" => TemporalUnit.Weekday,
            "NANOSECOND" or "NANOSECONDS" or "NS" => TemporalUnit.Nanosecond,
            _ => TemporalUnit.Unknown
        };
    }

    internal static bool IsIsoWeek(string unitText)
    {
        if (string.IsNullOrWhiteSpace(unitText))
            return false;

        var normalized = unitText.Trim();
        return normalized.Equals("ISO_WEEK", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("ISOWK", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("ISOWW", StringComparison.OrdinalIgnoreCase);
    }

    internal static bool IsTimeZoneOffset(string unitText)
    {
        if (string.IsNullOrWhiteSpace(unitText))
            return false;

        var normalized = unitText.Trim();
        return normalized.Equals("TZOFFSET", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("TZ", StringComparison.OrdinalIgnoreCase);
    }

    internal static bool TryResolveTimeZoneOffsetMinutes(object? value, out int offsetMinutes)
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

        return AstQueryExecutorBase.TryCoerceDateTime(value, out _);
    }

    internal static TemporalUnit Resolve(SqlExpr expr, Func<int, object?> evalArg)
    {
        return Resolve(GetUnitText(expr, evalArg));
    }

    internal static string GetUnitText(SqlExpr expr, Func<int, object?> evalArg)
    {
        var unitText = expr switch
        {
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => col.Name,
            LiteralExpr lit => lit.Value?.ToString() ?? string.Empty,
            CallExpr call when call.Args.Count == 0 => call.Name,
            FunctionCallExpr call when call.Args.Count == 0 => call.Name,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(unitText))
            return string.Empty;

        return unitText!;
    }

    internal static int GetWeekdayIndex(DateTime dateTime)
        => ((int)dateTime.DayOfWeek) + 1;

    internal static string GetWeekdayName(DateTime dateTime)
        => dateTime.ToString("dddd", CultureInfo.InvariantCulture);

    internal static int GetWeekOfYear(DateTime dateTime)
        => CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(
            dateTime,
            CalendarWeekRule.FirstDay,
            DayOfWeek.Sunday);

    internal static DateTime TruncateToWeekStart(DateTime dateTime)
        => dateTime.Date.AddDays(-(int)dateTime.DayOfWeek);

    internal static DateTime TruncateToIsoWeekStart(DateTime dateTime)
    {
        var daysToMonday = ((int)dateTime.DayOfWeek - (int)DayOfWeek.Monday + 7) % 7;
        return DateTime.SpecifyKind(dateTime.Date.AddDays(-daysToMonday), dateTime.Kind);
    }

    internal static long GetWeekDifference(DateTime start, DateTime end)
        => (TruncateToWeekStart(end) - TruncateToWeekStart(start)).Days / 7L;

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

        var trailing = trimmed[(separatorIndex + 1)..].Trim();
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
