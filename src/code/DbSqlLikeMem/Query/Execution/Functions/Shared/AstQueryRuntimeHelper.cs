namespace DbSqlLikeMem;

internal static class AstQueryRuntimeHelper
{
    private static readonly IReadOnlyDictionary<string, DayOfWeek> _oracleDayOfWeekMap = new Dictionary<string, DayOfWeek>(StringComparer.OrdinalIgnoreCase)
    {
        ["SUN"] = DayOfWeek.Sunday,
        ["SUNDAY"] = DayOfWeek.Sunday,
        ["MON"] = DayOfWeek.Monday,
        ["MONDAY"] = DayOfWeek.Monday,
        ["TUE"] = DayOfWeek.Tuesday,
        ["TUES"] = DayOfWeek.Tuesday,
        ["TUESDAY"] = DayOfWeek.Tuesday,
        ["WED"] = DayOfWeek.Wednesday,
        ["WEDNESDAY"] = DayOfWeek.Wednesday,
        ["THU"] = DayOfWeek.Thursday,
        ["THUR"] = DayOfWeek.Thursday,
        ["THURSDAY"] = DayOfWeek.Thursday,
        ["FRI"] = DayOfWeek.Friday,
        ["FRIDAY"] = DayOfWeek.Friday,
        ["SAT"] = DayOfWeek.Saturday,
        ["SATURDAY"] = DayOfWeek.Saturday,
    };

    internal static bool TryParseOracleDayOfWeek(string value, out DayOfWeek day)
    {
        day = default;
        var normalized = value.Trim().ToUpperInvariant();
        return _oracleDayOfWeekMap.TryGetValue(normalized, out day);
    }

    internal static string ApplyInitCap(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        var builder = new StringBuilder(value.Length);
        var makeUpper = true;
        foreach (var ch in value)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(makeUpper
                    ? char.ToUpperInvariant(ch)
                    : char.ToLowerInvariant(ch));
                makeUpper = false;
            }
            else
            {
                builder.Append(ch);
                makeUpper = true;
            }
        }

        return builder.ToString();
    }

    internal static bool TryNormalizeHexPayload(string trimmed, out string hex)
    {
        hex = string.Empty;

        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hex = trimmed[2..];
            return true;
        }

        if (trimmed.Length >= 3
            && (trimmed[0] == 'x' || trimmed[0] == 'X')
            && trimmed[1] == '\''
            && trimmed[^1] == '\'')
        {
            hex = trimmed[2..^1];
            return true;
        }

        hex = trimmed;
        return true;
    }

    internal static double Log2(double value)
        => Math.Log(value, 2d);

    internal static long NextRandomInt64()
    {
        var buffer = new byte[8];
        lock (AstQueryExecutorBase._randomLock)
            AstQueryExecutorBase._sharedRandom.NextBytes(buffer);
        return BitConverter.ToInt64(buffer, 0);
    }

    internal static double NextRandomDouble()
    {
        lock (AstQueryExecutorBase._randomLock)
            return AstQueryExecutorBase._sharedRandom.NextDouble();
    }
}
