namespace DbSqlLikeMem;

internal static class SqlRawCommaSplitterHelper
{
    internal static List<string> SplitRawByComma(
        this string? rawBlock)
    {
        var res = new List<string>();
        if (string.IsNullOrEmpty(rawBlock))
            return res;

        if (StringCompatibility.IndexOfAny(rawBlock!, '(', ')', '\'', '"') < 0)
            return SplitRawByCommaSimple(rawBlock!);

        var depth = 0;
        var start = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = 0; i < rawBlock!.Length; i++)
        {
            var ch = rawBlock[i];

            if (inSingleQuote)
            {
                if (ch == '\\' && i + 1 < rawBlock.Length)
                {
                    i++;
                    continue;
                }

                if (ch == '\'' && i + 1 < rawBlock.Length && rawBlock[i + 1] == '\'')
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
                if (ch == '\\' && i + 1 < rawBlock.Length)
                {
                    i++;
                    continue;
                }

                if (ch == '"' && i + 1 < rawBlock.Length && rawBlock[i + 1] == '"')
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
                res.Add(rawBlock[start..i].Trim());
                start = i + 1;
            }
        }

        if (start <= rawBlock.Length)
            res.Add(rawBlock[start..].Trim());

        return res;
    }

    private static List<string> SplitRawByCommaSimple(
        string rawBlock)
    {
        if (rawBlock.IndexOf(',') < 0)
            return new List<string>(1) { rawBlock.Trim() };

        var res = new List<string>();
        var start = 0;
        for (var i = 0; i < rawBlock.Length; i++)
        {
            if (rawBlock[i] == ',')
            {
                res.Add(rawBlock[start..i].Trim());
                start = i + 1;
            }
        }

        if (start <= rawBlock.Length)
            res.Add(rawBlock[start..].Trim());

        return res;
    }
}
