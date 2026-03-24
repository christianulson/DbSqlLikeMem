namespace DbSqlLikeMem;

internal static class SqlRawCommaSplitterHelper
{
    internal static List<string> SplitRawByComma(
        this string? rawBlock)
    {
        var res = new List<string>();
        if (rawBlock?.Length == 0)
            return res;

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
}
