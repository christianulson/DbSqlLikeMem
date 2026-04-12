namespace DbSqlLikeMem;

internal static class SqlSimpleValueParserHelper
{
    internal static string NormalizeSimpleSqlValueRaw(string raw, ISqlDialect dialect)
    {
        raw = raw.Trim();
        if (string.IsNullOrWhiteSpace(raw))
            return raw;

        return NormalizeSimpleSqlValueRawTrimmed(raw, dialect);
    }

    internal static string NormalizeSimpleSqlValueRawTrimmed(string raw, ISqlDialect dialect)
    {
        if (TryParseQuotedStringValue(raw, dialect, out var normalizedString))
            return normalizedString;

        return raw;
    }

    internal static bool TryParseSimpleSqlValueExpression(
        string raw,
        ISqlDialect dialect,
        out SqlExpr expr)
    {
        expr = default!;
        raw = raw.Trim();
        if (string.IsNullOrWhiteSpace(raw))
            return false;

        return TryParseSimpleSqlValueExpressionTrimmed(raw, dialect, out expr);
    }

    internal static bool TryParseSimpleSqlValueExpressionTrimmed(
        string raw,
        ISqlDialect dialect,
        out SqlExpr expr)
    {
        if (TryParseQuotedStringValue(raw, dialect, out var normalizedString))
        {
            expr = new LiteralExpr(normalizedString);
            return true;
        }

        if (TryParseNullTrueFalse(raw, out expr))
            return true;

        if (TryParseParameter(raw, dialect, out expr))
            return true;

        if (TryParseHexBinaryLiteralValue(raw, out var binaryValue))
        {
            expr = new LiteralExpr(binaryValue);
            return true;
        }

        if (TryParseNumericLiteralValue(raw, out var numericValue))
        {
            expr = new LiteralExpr(numericValue);
            return true;
        }

        return false;
    }

    private static bool TryParseQuotedStringValue(
        string raw,
        ISqlDialect dialect,
        out string normalized)
    {
        normalized = string.Empty;
        if (raw.Length < 2)
            return false;

        var quote = raw[0];
        if (!dialect.IsStringQuote(quote) || raw[^1] != quote)
            return false;

        var inner = raw.AsSpan(1, raw.Length - 2);
        if (inner.Length == 0)
        {
            normalized = string.Empty;
            return true;
        }

        var sb = new StringBuilder(inner.Length);
        for (var i = 0; i < inner.Length; i++)
        {
            var ch = inner[i];

            if (dialect.StringEscapeStyle == SqlStringEscapeStyle.backslash && ch == '\\')
            {
                if (i + 1 >= inner.Length)
                    return false;

                sb.Append(inner[++i]);
                continue;
            }

            if (ch == quote)
            {
                if (dialect.StringEscapeStyle != SqlStringEscapeStyle.backslash
                    && i + 1 < inner.Length
                    && inner[i + 1] == quote)
                {
                    sb.Append(quote);
                    i++;
                    continue;
                }

                return false;
            }

            sb.Append(ch);
        }

        normalized = sb.ToString();
        return true;
    }

    private static bool TryParseNullTrueFalse(string raw, out SqlExpr expr)
    {
        expr = default!;

        if (raw.Equals(SqlConst.NULL, StringComparison.OrdinalIgnoreCase))
        {
            expr = new LiteralExpr(null);
            return true;
        }

        if (raw.Equals(SqlConst.TRUE, StringComparison.OrdinalIgnoreCase))
        {
            expr = new LiteralExpr(true);
            return true;
        }

        if (raw.Equals(SqlConst.FALSE, StringComparison.OrdinalIgnoreCase))
        {
            expr = new LiteralExpr(false);
            return true;
        }

        return false;
    }

    private static bool TryParseParameter(
        string raw,
        ISqlDialect dialect,
        out SqlExpr expr)
    {
        expr = default!;

        if (raw.Length == 0 || !dialect.IsParameterPrefix(raw[0]))
            return false;

        if (raw.Length > 1)
        {
            for (var i = 1; i < raw.Length; i++)
            {
                if (!IsParameterChar(raw[i], dialect))
                    return false;
            }
        }

        expr = new ParameterExpr(raw);
        return true;
    }

    private static bool TryParseHexBinaryLiteralValue(string raw, out byte[] binaryValue)
    {
        binaryValue = [];

        if (!raw.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            return false;

        var hex = raw.AsSpan(2);
        if (hex.Length == 0 || hex.Length % 2 != 0)
            return false;

        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            if (!ReadOnlySpanCompatibility.TryParseByte(hex.Slice(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
                return false;

            buffer[i / 2] = part;
        }

        binaryValue = buffer;
        return true;
    }

    private static bool TryParseNumericLiteralValue(string raw, out object numericValue)
    {
        numericValue = default!;

        if (raw.Length == 0)
            return false;

        if (!HasValidNumericLiteralSyntax(raw, out var hasDecimalPoint, out var hasExponent))
            return false;

        if (!hasDecimalPoint && !hasExponent)
        {
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue))
            {
                numericValue = intValue;
                return true;
            }

            if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
            {
                numericValue = longValue;
                return true;
            }
        }

        if (hasExponent
            && double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleValue))
        {
            numericValue = doubleValue;
            return true;
        }

        if (decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue))
        {
            numericValue = decimalValue;
            return true;
        }

        if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var fallbackDouble))
            return false;

        numericValue = fallbackDouble;
        return true;
    }

    private static bool HasValidNumericLiteralSyntax(
        string raw,
        out bool hasDecimalPoint,
        out bool hasExponent)
    {
        hasDecimalPoint = false;
        hasExponent = false;

        var index = 0;
        if (raw[index] is '+' or '-')
        {
            index++;
            if (index == raw.Length)
                return false;
        }

        var digitsBeforeDecimal = 0;
        while (index < raw.Length && char.IsDigit(raw[index]))
        {
            index++;
            digitsBeforeDecimal++;
        }

        if (index < raw.Length && raw[index] == '.')
        {
            hasDecimalPoint = true;
            index++;

            while (index < raw.Length && char.IsDigit(raw[index]))
                index++;
        }

        if (digitsBeforeDecimal == 0 && !hasDecimalPoint)
            return false;

        if (index < raw.Length && (raw[index] == 'e' || raw[index] == 'E'))
        {
            hasExponent = true;
            index++;

            if (index < raw.Length && (raw[index] is '+' or '-'))
                index++;

            var exponentDigits = 0;
            while (index < raw.Length && char.IsDigit(raw[index]))
            {
                index++;
                exponentDigits++;
            }

            if (exponentDigits == 0)
                return false;
        }

        return index == raw.Length;
    }

    private static bool IsParameterChar(char ch, ISqlDialect dialect)
        => char.IsLetterOrDigit(ch)
           || ch is '_' or '$'
           || (dialect.AllowsHashIdentifiers && ch == '#');
}
