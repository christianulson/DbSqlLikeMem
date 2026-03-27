namespace DbSqlLikeMem;

using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
using System.Text;

internal static class AstQueryFormatFunctionHelper
{
    private static readonly ConcurrentDictionary<string, OracleFormatMaskCacheEntry> _oracleFormatMaskCache = new(StringComparer.Ordinal);

    internal static bool TryParseOracleDateTime(string text, string? mask, out DateTime result)
    {
        if (string.IsNullOrWhiteSpace(mask))
        {
            return AstQueryExecutorBase.TryParseCachedDateTime(text, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        var netFormat = NormalizeOracleFormatMask(mask, out var hasTz);
        if (netFormat is null || string.IsNullOrWhiteSpace(netFormat))
        {
            return AstQueryExecutorBase.TryParseCachedDateTime(text, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        if (hasTz && AstQueryExecutorBase.TryParseExactCachedDateTimeOffset(text, netFormat, DateTimeStyles.AllowWhiteSpaces, out var dto))
        {
            result = dto.DateTime;
            return true;
        }

        return AstQueryExecutorBase.TryParseExactCachedDateTime(text, netFormat, DateTimeStyles.AllowWhiteSpaces, out result);
    }

    internal static bool TryParseOracleDateTimeOffset(string text, string? mask, out DateTimeOffset result)
    {
        if (string.IsNullOrWhiteSpace(mask))
        {
            return AstQueryExecutorBase.TryParseCachedDateTimeOffset(text, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        var netFormat = NormalizeOracleFormatMask(mask, out var _);
        if (netFormat is null || string.IsNullOrWhiteSpace(netFormat))
        {
            return AstQueryExecutorBase.TryParseCachedDateTimeOffset(text, DateTimeStyles.AllowWhiteSpaces, out result);
        }

        return AstQueryExecutorBase.TryParseExactCachedDateTimeOffset(text, netFormat, DateTimeStyles.AllowWhiteSpaces, out result);
    }

    internal static string? NormalizeOracleFormatMask(string? mask, out bool hasTimeZone)
    {
        if (mask is null || string.IsNullOrWhiteSpace(mask))
        {
            hasTimeZone = false;
            return null;
        }

        var entry = _oracleFormatMaskCache.GetOrAdd(mask, static rawMask =>
        {
            var upper = ReplaceInsensitive(rawMask.Trim().ToUpperInvariant(), "FM", string.Empty);
            var hasTimeZone = upper.IndexOf("TZH", StringComparison.OrdinalIgnoreCase) >= 0
                || upper.IndexOf("TZM", StringComparison.OrdinalIgnoreCase) >= 0
                || upper.IndexOf("TZR", StringComparison.OrdinalIgnoreCase) >= 0
                || upper.IndexOf("TZD", StringComparison.OrdinalIgnoreCase) >= 0;

            var net = upper;

            net = ReplaceInsensitive(net, "TZH:TZM", "zzz");
            net = ReplaceInsensitive(net, "TZH", "zz");
            net = ReplaceInsensitive(net, "TZM", "mm");
            net = ReplaceInsensitive(net, "RRRR", "yyyy");
            net = ReplaceInsensitive(net, "YYYY", "yyyy");
            net = ReplaceInsensitive(net, "YYY", "yyy");
            net = ReplaceInsensitive(net, "YY", "yy");
            net = ReplaceInsensitive(net, "Y", "y");
            net = ReplaceInsensitive(net, "MONTH", "MMMM");
            net = ReplaceInsensitive(net, "MON", "MMM");
            net = ReplaceInsensitive(net, "MM", "MM");
            net = ReplaceInsensitive(net, "DD", "dd");
            net = ReplaceInsensitive(net, "HH24", "HH");
            net = ReplaceInsensitive(net, "HH12", "hh");
            net = ReplaceInsensitive(net, "HH", "hh");
            net = ReplaceInsensitive(net, "MI", "mm");
            net = ReplaceInsensitive(net, "SS", "ss");
            net = ReplaceInsensitive(net, "FF", "fffffff");

            return new OracleFormatMaskCacheEntry(net.Replace("\"", "'"), hasTimeZone);
        });

        hasTimeZone = entry.HasTimeZone;
        return entry.NetFormat;
    }

    internal static bool IsNumericValue(object? value)
        => value is sbyte or byte or short or ushort or int or uint or long or ulong
            or float or double or decimal;

    internal static bool TryParseOracleNumber(string text, string? mask, out decimal result)
    {
        result = 0m;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        if (string.IsNullOrWhiteSpace(mask))
            return decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out result);

        var normalizedMask = mask!.ToUpperInvariant();
        var trimmed = text.Trim();
        var isNegative = false;
        if (trimmed.StartsWith("(") && trimmed.EndsWith(")"))
        {
            isNegative = true;
            trimmed = trimmed[1..^1];
        }

        var decimalSeparator = normalizedMask.Contains('D') ? '.' : ',';
        var groupSeparator = normalizedMask.Contains('G') ? ',' : '.';
        var builder = new StringBuilder(trimmed.Length);
        foreach (var ch in trimmed)
        {
            if (char.IsWhiteSpace(ch))
                continue;

            if (ch == '(' || ch == ')')
                continue;

            if (ch == groupSeparator)
                continue;

            if (ch == decimalSeparator)
            {
                builder.Append('.');
                continue;
            }

            builder.Append(ch);
        }

        var cleaned = builder.ToString();
        if (!decimal.TryParse(cleaned, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out result))
            return false;

        if (isNegative)
            result = -result;
        return true;
    }

    internal static string FormatOracleNumber(object value, string mask)
    {
        if (!AstQueryExecutorBase.TryConvertNumericToDecimal(value, out var number))
            return value.ToString() ?? string.Empty;

        var normalizedMask = ReplaceInsensitive(mask.ToUpperInvariant(), "FM", string.Empty);
        var formatBuilder = new StringBuilder(normalizedMask.Length);
        foreach (var ch in normalizedMask)
        {
            formatBuilder.Append(ch switch
            {
                '9' => '#',
                '0' => '0',
                'D' => '.',
                'G' => ',',
                _ => ch
            });
        }

        var format = formatBuilder.ToString();
        try
        {
            return number.ToString(format, CultureInfo.InvariantCulture);
        }
        catch
        {
            return number.ToString(CultureInfo.InvariantCulture);
        }
    }

    internal static string FormatPostgreSqlNumber(object value, string mask)
    {
        if (!AstQueryExecutorBase.TryConvertNumericToDecimal(value, out var number))
            return value.ToString() ?? string.Empty;

        var normalizedMask = ReplaceInsensitive(mask.ToUpperInvariant(), "FM", string.Empty);
        var decimalIndex = normalizedMask.IndexOf('D');
        if (decimalIndex < 0)
            decimalIndex = normalizedMask.IndexOf('.');

        var integerMask = decimalIndex >= 0 ? normalizedMask[..decimalIndex] : normalizedMask;
        var fractionalMask = decimalIndex >= 0 ? normalizedMask[(decimalIndex + 1)..] : string.Empty;

        var fractionalDigits = fractionalMask.Count(ch => ch is '9' or '0');
        var rounded = Math.Round(number, fractionalDigits, MidpointRounding.AwayFromZero);
        var absText = Math.Abs(rounded).ToString($"F{fractionalDigits}", CultureInfo.InvariantCulture);
        var absParts = absText.Split('.');

        var integerDigits = absParts[0];
        var fractionalDigitsText = absParts.Length > 1 ? absParts[1] : string.Empty;

        var integerPlaceholders = integerMask.Count(ch => ch is '9' or '0');
        if (integerDigits.Length < integerPlaceholders)
        {
            var padded = integerDigits.PadLeft(integerPlaceholders, ' ');
            var chars = padded.ToCharArray();
            var digitIndex = chars.Length - integerDigits.Length;
            for (var i = 0; i < integerMask.Length && i < chars.Length; i++)
            {
                if (integerMask[i] == '0' && i < digitIndex)
                    chars[i] = '0';
            }

            integerDigits = new string(chars);
        }

        if (fractionalDigits > 0 && fractionalDigitsText.Length < fractionalDigits)
            fractionalDigitsText = fractionalDigitsText.PadRight(fractionalDigits, '0');

        var sign = rounded < 0m ? "-" : " ";
        return fractionalDigits > 0
            ? $"{sign}{integerDigits}.{fractionalDigitsText}"
            : $"{sign}{integerDigits}";
    }

    internal static string FormatPrintf(string format, IReadOnlyList<object?> args)
    {
        var builder = new StringBuilder();
        var argIndex = 0;
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            if (token == '%')
            {
                builder.Append('%');
                continue;
            }

            var value = argIndex < args.Count ? args[argIndex++] : null;
            var text = token switch
            {
                'd' or 'i' => AstQueryExecutorBase.IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
                'f' => AstQueryExecutorBase.IsNullish(value) ? "0" : Convert.ToDouble(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
                's' => value?.ToString() ?? string.Empty,
                'x' => AstQueryExecutorBase.IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString("x", CultureInfo.InvariantCulture),
                'X' => AstQueryExecutorBase.IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString("X", CultureInfo.InvariantCulture),
                _ => value?.ToString() ?? string.Empty
            };

            builder.Append(text);
        }

        return builder.ToString();
    }

    private static string ReplaceInsensitive(string value, string oldValue, string newValue)
    {
        if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(oldValue))
            return value;

        var sb = new StringBuilder(value.Length);
        var index = 0;
        while (true)
        {
            var found = value.IndexOf(oldValue, index, StringComparison.OrdinalIgnoreCase);
            if (found < 0)
            {
                sb.Append(value, index, value.Length - index);
                break;
            }

            sb.Append(value, index, found - index);
            sb.Append(newValue);
            index = found + oldValue.Length;
        }

        return sb.ToString();
    }

    private readonly record struct OracleFormatMaskCacheEntry(string NetFormat, bool HasTimeZone);
}
