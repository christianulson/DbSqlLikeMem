namespace DbSqlLikeMem;

internal static class SqlRawCommaSplitterHelper
{
    internal static List<string> SplitRawByComma(
        this string? rawBlock)
    {
        var res = new List<string>();
        if (string.IsNullOrEmpty(rawBlock))
            return res;

        var rawSpan = rawBlock.AsSpan();
        if (!ContainsAny(rawSpan, '(', ')', '\'', '"'))
            return SplitRawByCommaSimple(rawSpan);

        var depth = 0;
        var start = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = 0; i < rawSpan.Length; i++)
        {
            var ch = rawSpan[i];

            if (inSingleQuote)
            {
                if (ch == '\\' && i + 1 < rawSpan.Length)
                {
                    i++;
                    continue;
                }

                if (ch == '\'' && i + 1 < rawSpan.Length && rawSpan[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                if (ch == '\'')
                    inSingleQuote = false;

                continue;
            }

            if (inDoubleQuote)
            {
                if (ch == '\\' && i + 1 < rawSpan.Length)
                {
                    i++;
                    continue;
                }

                if (ch == '"' && i + 1 < rawSpan.Length && rawSpan[i + 1] == '"')
                {
                    i++;
                    continue;
                }

                if (ch == '"')
                    inDoubleQuote = false;

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '"')
            {
                inDoubleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (depth == 0 && ch == ',')
            {
                res.Add(TrimToString(rawSpan.Slice(start, i - start)));
                start = i + 1;
            }
        }

        if (start <= rawSpan.Length)
            res.Add(TrimToString(rawSpan[start..]));

        return res;
    }

    private static List<string> SplitRawByCommaSimple(
        ReadOnlySpan<char> rawSpan)
    {
        if (rawSpan.IndexOf(',') < 0)
            return new List<string>(1) { TrimToString(rawSpan) };

        var res = new List<string>();
        var start = 0;
        for (var i = 0; i < rawSpan.Length; i++)
        {
            if (rawSpan[i] == ',')
            {
                res.Add(TrimToString(rawSpan.Slice(start, i - start)));
                start = i + 1;
            }
        }

        if (start <= rawSpan.Length)
            res.Add(TrimToString(rawSpan[start..]));

        return res;
    }

    private static bool ContainsAny(ReadOnlySpan<char> rawSpan, params char[] chars)
    {
        for (var i = 0; i < rawSpan.Length; i++)
        {
            var ch = rawSpan[i];
            for (var j = 0; j < chars.Length; j++)
            {
                if (ch == chars[j])
                    return true;
            }
        }

        return false;
    }

    private static string TrimToString(ReadOnlySpan<char> value)
    {
        var start = 0;
        var end = value.Length - 1;

        while (start <= end && char.IsWhiteSpace(value[start]))
            start++;

        while (end >= start && char.IsWhiteSpace(value[end]))
            end--;

        return end < start ? string.Empty : value.Slice(start, end - start + 1).ToString();
    }
}
