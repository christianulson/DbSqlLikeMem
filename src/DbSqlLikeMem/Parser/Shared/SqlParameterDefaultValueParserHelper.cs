using System.Globalization;

namespace DbSqlLikeMem;

internal static class SqlParameterDefaultValueParserHelper
{
    internal static bool TryParseParameterDefaultValue(
        this SqlQueryParserContext ctx,
        IReadOnlyList<SqlToken> tokens,
        out object? defaultValue)
    {
        defaultValue = null;

        if (tokens.Count == 0)
            return false;

        var firstToken = tokens[0];
        if (firstToken.Text != "="
            && !firstToken.Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var valueTokens = tokens.Skip(1).ToList();
        if (valueTokens.Count == 0)
            throw new InvalidOperationException("Parameter default value requires a literal.");

        defaultValue = ParseParameterDefaultLiteralValue(valueTokens);
        return true;
    }

    private static object? ParseParameterDefaultLiteralValue(
        IReadOnlyList<SqlToken> tokens)
    {
        var index = 0;
        var sign = 1;

        if (tokens[index].Kind == SqlTokenKind.Symbol
            && (tokens[index].Text == "+" || tokens[index].Text == "-"))
        {
            sign = tokens[index].Text == "-" ? -1 : 1;
            index++;
        }

        if (index >= tokens.Count)
            throw new InvalidOperationException("Parameter default value requires a literal.");

        if (tokens.Count != index + 1)
            throw new NotSupportedException("Parameter default values are not supported in the mock yet.");

        var token = tokens[index];
        return token.Kind switch
        {
            SqlTokenKind.String => token.Text,
            SqlTokenKind.Number => ParseNumericLiteral(token.Text, sign),
            SqlTokenKind.Identifier or SqlTokenKind.Keyword => ParseKeywordLiteral(token.Text),
            _ => throw new NotSupportedException("Parameter default values are not supported in the mock yet."),
        };
    }

    private static object? ParseKeywordLiteral(string text)
        => text.Equals(SqlConst.NULL, StringComparison.OrdinalIgnoreCase)
            ? null
            : text.Equals(SqlConst.TRUE, StringComparison.OrdinalIgnoreCase)
                ? true
                : text.Equals(SqlConst.FALSE, StringComparison.OrdinalIgnoreCase)
                    ? false
                    : throw new NotSupportedException("Parameter default values are not supported in the mock yet.");

    private static object ParseNumericLiteral(
        string text,
        int sign)
    {
        var negative = sign < 0;
        if (text.StartsWith("+", StringComparison.Ordinal))
            text = text[1..];

        if (text.StartsWith("-", StringComparison.Ordinal))
        {
            negative = !negative;
            text = text[1..];
        }

        if (text.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            var hexText = text[2..];
            var parsedHex = Convert.ToInt64(hexText, 16);
            return negative ? -parsedHex : parsedHex;
        }

        var signedText = negative ? $"-{text}" : text;

        if (long.TryParse(signedText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
            return parsedLong;

        if (decimal.TryParse(signedText, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedDecimal))
            return parsedDecimal;

        if (double.TryParse(signedText, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsedDouble))
            return parsedDouble;

        throw new NotSupportedException("Parameter default values are not supported in the mock yet.");
    }
}
