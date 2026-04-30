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
    {
#if NET8_0_OR_GREATER
        return byte.TryParse(value, styles, provider, out result);
#else
        return byte.TryParse(value.ToString(), styles, provider, out result);
#endif
    }

    internal static bool TryParseInt32(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out int result)
    {
#if NET8_0_OR_GREATER
        return int.TryParse(value, styles, provider, out result);
#else
        return int.TryParse(value.ToString(), styles, provider, out result);
#endif
    }

    internal static bool TryParseInt64(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out long result)
    {
#if NET8_0_OR_GREATER
        return long.TryParse(value, styles, provider, out result);
#else
        return long.TryParse(value.ToString(), styles, provider, out result);
#endif
    }

    internal static bool TryParseDouble(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out double result)
    {
#if NET8_0_OR_GREATER
        return double.TryParse(value, styles, provider, out result);
#else
        return double.TryParse(value.ToString(), styles, provider, out result);
#endif
    }

    internal static bool TryParseDecimal(
        ReadOnlySpan<char> value,
        NumberStyles styles,
        IFormatProvider? provider,
        out decimal result)
    {
#if NET8_0_OR_GREATER
        return decimal.TryParse(value, styles, provider, out result);
#else
        return decimal.TryParse(value.ToString(), styles, provider, out result);
#endif
    }

    internal static bool TryParseTimeSpan(
        ReadOnlySpan<char> value,
        IFormatProvider? provider,
        out TimeSpan result)
    {
#if NET8_0_OR_GREATER
        return TimeSpan.TryParse(value, provider, out result);
#else
        return TimeSpan.TryParse(value.ToString(), provider, out result);
#endif
    }

    internal static bool TryParseDateTime(
        ReadOnlySpan<char> value,
        IFormatProvider? provider,
        DateTimeStyles styles,
        out DateTime result)
    {
#if NET8_0_OR_GREATER
        return DateTime.TryParse(value, provider, styles, out result);
#else
        return DateTime.TryParse(value.ToString(), provider, styles, out result);
#endif
    }
}
