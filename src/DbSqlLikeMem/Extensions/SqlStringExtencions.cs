namespace DbSqlLikeMem;

internal static class SqlStringExtencions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static string NormalizeString(this string str)
    {
        ArgumentNullException.ThrowIfNull(str);

        if (str.Length == 0)
            return string.Empty;

        var sb = new System.Text.StringBuilder(str.Length);
        var pendingSpace = false;

        for (var i = 0; i < str.Length; i++)
        {
            var ch = str[i];
            var isWhitespace = ch is ' ' or '\n' or '\r' or '\t';

            if (isWhitespace)
            {
                if (sb.Length > 0)
                    pendingSpace = true;

                continue;
            }

            if (pendingSpace)
            {
                sb.Append(' ');
                pendingSpace = false;
            }

            sb.Append(ch);
        }

        return sb.ToString();
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static string NormalizeName(this string name)
    {
        name = name.Trim();
        name = name.Trim('`');       // remove `User`
        name = name.Trim();          // de novo por segurança
        return name;
    }
}
