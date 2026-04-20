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

    internal static TemporalUnit Resolve(SqlExpr expr, Func<int, object?> evalArg)
    {
        var unitText = expr switch
        {
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => col.Name,
            LiteralExpr lit => lit.Value?.ToString() ?? string.Empty,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(unitText))
            unitText = evalArg(0)?.ToString() ?? string.Empty;

        return Resolve(unitText);
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
        var isoYear = ISOWeek.GetYear(dateTime);
        var isoWeek = ISOWeek.GetWeekOfYear(dateTime);
        var start = ISOWeek.ToDateTime(isoYear, isoWeek, DayOfWeek.Monday);
        return DateTime.SpecifyKind(start, dateTime.Kind);
    }

    internal static long GetWeekDifference(DateTime start, DateTime end)
        => (TruncateToWeekStart(end) - TruncateToWeekStart(start)).Days / 7L;
}
