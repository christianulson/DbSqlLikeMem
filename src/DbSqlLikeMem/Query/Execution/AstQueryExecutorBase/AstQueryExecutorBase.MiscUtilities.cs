namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private static bool TryParseOracleDayOfWeek(string value, out DayOfWeek day)
    {
        day = default;
        var normalized = value.Trim().ToUpperInvariant();
        if (_oracleDayOfWeekMap.TryGetValue(normalized, out day))
            return true;

        return false;
    }

    private static string ApplyInitCap(string value)
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

    private static bool TryNormalizeHexPayload(string trimmed, out string hex)
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
        lock (_randomLock)
            _sharedRandom.NextBytes(buffer);
        return BitConverter.ToInt64(buffer, 0);
    }

    internal static double NextRandomDouble()
    {
        lock (_randomLock)
            return _sharedRandom.NextDouble();
    }
}
