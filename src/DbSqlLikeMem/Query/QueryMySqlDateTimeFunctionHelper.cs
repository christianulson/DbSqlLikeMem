using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal delegate bool TryConvertNumericToDoubleDelegate(object? value, out double result);
internal delegate bool TryParseExactCachedDateTimeDelegate(string text, string format, DateTimeStyles styles, out DateTime dt);

internal static class QueryMySqlDateTimeFunctionHelper
{
    private static readonly ConcurrentDictionary<string, string> _dateFormatCache = new(StringComparer.Ordinal);

    public static bool TryEvalFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        TryParseExactCachedDateTimeDelegate tryParseExactCachedDateTime,
        out object? result)
    {
        return TryEvalMySqlDateFormatFunction(fn, dialect, evalArg, tryCoerceDateTime, out result)
            || TryEvalMySqlStrToDateFunction(fn, dialect, evalArg, tryParseExactCachedDateTime, out result)
            || TryEvalMySqlFromUnixTimeFunction(fn, dialect, evalArg, tryConvertNumericToDouble, out result)
            || TryEvalMySqlFromDaysFunction(fn, dialect, evalArg, tryConvertNumericToInt64, out result)
            || TryEvalMySqlGetFormatFunction(fn, dialect, evalArg, out result)
            || TryEvalMySqlConvertTzFunction(fn, dialect, evalArg, tryCoerceDateTime, out result);
    }

    private static bool TryEvalMySqlDateFormatFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (!fn.Name.Equals("DATE_FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("DATE_FORMAT() espera data e formato.");

        var value = evalArg(0);
        var formatValue = evalArg(1)?.ToString();
        if (IsNullish(value) || string.IsNullOrWhiteSpace(formatValue) || !tryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        result = dateTime.ToString(GetMySqlDateFormatPattern(formatValue!), CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalMySqlStrToDateFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryParseExactCachedDateTimeDelegate tryParseExactCachedDateTime,
        out object? result)
    {
        if (!fn.Name.Equals("STR_TO_DATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STR_TO_DATE() espera texto e formato.");

        var textValue = evalArg(0)?.ToString();
        var formatValue = evalArg(1)?.ToString();
        if (string.IsNullOrWhiteSpace(textValue) || string.IsNullOrWhiteSpace(formatValue))
        {
            result = null;
            return true;
        }

        var text = textValue!;
        var format = formatValue!;

        if (tryParseExactCachedDateTime(
            text,
            GetMySqlDateFormatPattern(format),
            DateTimeStyles.AllowWhiteSpaces,
            out var parsed))
        {
            result = parsed;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalMySqlFromUnixTimeFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (!fn.Name.Equals("FROM_UNIXTIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FROM_UNIXTIME() espera um argumento.");

        var value = evalArg(0);
        if (IsNullish(value) || !tryConvertNumericToDouble(value, out var seconds))
        {
            result = null;
            return true;
        }

        var dateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seconds);
        if (fn.Args.Count > 1)
        {
            var formatValue = evalArg(1)?.ToString();
            if (string.IsNullOrWhiteSpace(formatValue))
            {
                result = null;
                return true;
            }

            result = dateTime.ToString(GetMySqlDateFormatPattern(formatValue!), CultureInfo.InvariantCulture);
            return true;
        }

        result = dateTime;
        return true;
    }

    private static bool TryEvalMySqlFromDaysFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        if (!fn.Name.Equals("FROM_DAYS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FROM_DAYS() espera um argumento.");

        var value = evalArg(0);
        if (IsNullish(value) || !tryConvertNumericToInt64(value!, out var days) || days < 1)
        {
            result = null;
            return true;
        }

        result = new DateTime(1, 1, 1).AddDays(days - 1);
        return true;
    }

    private static bool TryEvalMySqlGetFormatFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("GET_FORMAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("GET_FORMAT() espera tipo e formato.");

        var typeValue = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(typeValue))
        {
            typeValue = fn.Args[0] switch
            {
                IdentifierExpr id => id.Name,
                RawSqlExpr raw => raw.Sql,
                _ => null
            };
        }

        var formatValue = evalArg(1)?.ToString();
        if (string.IsNullOrWhiteSpace(formatValue))
        {
            formatValue = fn.Args[1] switch
            {
                IdentifierExpr id => id.Name,
                RawSqlExpr raw => raw.Sql,
                _ => null
            };
        }

        if (string.IsNullOrWhiteSpace(typeValue) || string.IsNullOrWhiteSpace(formatValue))
        {
            result = null;
            return true;
        }

        result = ResolveMySqlGetFormatPattern(typeValue!, formatValue!);
        return true;
    }

    private static bool TryEvalMySqlConvertTzFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryCoerceDateTimeDelegate tryCoerceDateTime,
        out object? result)
    {
        if (!fn.Name.Equals("CONVERT_TZ", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("CONVERT_TZ() espera data e dois fusos.");

        var value = evalArg(0);
        var fromValue = evalArg(1)?.ToString();
        var toValue = evalArg(2)?.ToString();
        if (IsNullish(value) || string.IsNullOrWhiteSpace(fromValue) || string.IsNullOrWhiteSpace(toValue) || !tryCoerceDateTime(value, out var dateTime))
        {
            result = null;
            return true;
        }

        if (!TryParseMySqlTimeZoneOffset(fromValue!, out var fromOffset)
            || !TryParseMySqlTimeZoneOffset(toValue!, out var toOffset))
        {
            result = null;
            return true;
        }

        result = dateTime - fromOffset + toOffset;
        return true;
    }

    private static string GetMySqlDateFormatPattern(string format)
        => _dateFormatCache.GetOrAdd(format, static rawFormat => ConvertMySqlDateFormatToDotNet(rawFormat));

    private static string ConvertMySqlDateFormatToDotNet(string format)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%')
            {
                builder.Append(ch);
                continue;
            }

            if (i + 1 >= format.Length)
                break;

            i++;
            builder.Append(format[i] switch
            {
                'Y' => "yyyy",
                'y' => "yy",
                'm' => "MM",
                'c' => "M",
                'd' => "dd",
                'e' => "d",
                'H' => "HH",
                'k' => "H",
                'h' or 'I' => "hh",
                'l' => "h",
                'i' => "mm",
                's' or 'S' => "ss",
                'f' => "ffffff",
                'p' => "tt",
                'T' => "HH:mm:ss",
                'r' => "hh:mm:ss tt",
                'b' => "MMM",
                'M' => "MMMM",
                'a' => "ddd",
                'W' => "dddd",
                '%' => "%",
                _ => format[i].ToString()
            });
        }

        return builder.ToString();
    }

    private static string? ResolveMySqlGetFormatPattern(string type, string format)
    {
        var typeKey = type.Trim().ToUpperInvariant();
        var formatKey = format.Trim().ToUpperInvariant();
        return typeKey switch
        {
            "DATE" => formatKey switch
            {
                "USA" => "%m.%d.%Y",
                "JIS" or "ISO" => "%Y-%m-%d",
                "EUR" => "%d.%m.%Y",
                "INTERNAL" => "%Y%m%d",
                _ => null
            },
            "TIME" => formatKey switch
            {
                "USA" => "%h:%i:%s %p",
                "JIS" or "ISO" => "%H:%i:%s",
                "EUR" => "%H.%i.%s",
                "INTERNAL" => "%H%i%s",
                _ => null
            },
            "DATETIME" or "TIMESTAMP" => formatKey switch
            {
                "USA" => "%m.%d.%Y %h:%i:%s %p",
                "JIS" or "ISO" => "%Y-%m-%d %H:%i:%s",
                "EUR" => "%d.%m.%Y %H.%i.%s",
                "INTERNAL" => "%Y%m%d%H%i%s",
                _ => null
            },
            _ => null
        };
    }

    private static bool TryParseMySqlTimeZoneOffset(string text, out TimeSpan offset)
    {
        offset = TimeSpan.Zero;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var normalized = text.Trim().ToUpperInvariant();
        if (normalized is "UTC" or "GMT" or "SYSTEM")
        {
            offset = TimeSpan.Zero;
            return true;
        }

        if (normalized.Length == 6 && (normalized[0] == '+' || normalized[0] == '-') && normalized[3] == ':')
        {
            var hoursText = normalized.Substring(1, 2);
            var minutesText = normalized.Substring(4, 2);
            if (int.TryParse(hoursText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours)
                && int.TryParse(minutesText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes))
            {
                offset = new TimeSpan(hours, minutes, 0);
                if (normalized[0] == '-')
                    offset = -offset;
                return true;
            }
        }

        return false;
    }

    private static bool IsNullish(object? value) => value is null or DBNull;
}
