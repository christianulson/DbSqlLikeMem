namespace DbSqlLikeMem;

internal static class ReadOnlySpanCompatibility
{
    internal static string NormalizeName(this ReadOnlySpan<char> value)
        => value.ToString().NormalizeName();

    internal static bool TryParseByte(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out byte result)
        => byte.TryParse(value.ToString(), styles, provider, out result);

    internal static bool TryParseInt32(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out int result)
        => int.TryParse(value.ToString(), styles, provider, out result);

    internal static bool TryParseInt64(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out long result)
        => long.TryParse(value.ToString(), styles, provider, out result);

    internal static bool TryParseDouble(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out double result)
        => double.TryParse(value.ToString(), styles, provider, out result);

    internal static bool TryParseDecimal(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out decimal result)
        => decimal.TryParse(value.ToString(), styles, provider, out result);

    internal static bool TryParseTimeSpan(
        ReadOnlySpan<char> value,
        IFormatProvider? provider,
        out TimeSpan result)
        => TimeSpan.TryParse(value.ToString(), provider, out result);

    internal static bool TryParseDateTime(
        ReadOnlySpan<char> value,
        IFormatProvider? provider,
        DateTimeStyles styles,
        out DateTime result)
        => DateTime.TryParse(value.ToString(), provider, styles, out result);
}
