namespace DbSqlLikeMem;

internal static class SqlStringExtencions
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static string NormalizeString(this string str)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(str, nameof(str));

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
        ArgumentNullExceptionCompatible.ThrowIfNull(name, nameof(name));

        var trimmed = name.Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        if (!trimmed.Contains('.'))
            return StripIdentifierWrappers(trimmed);

        var parts = trimmed.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        for (var i = 0; i < parts.Length; i++)
            parts[i] = StripIdentifierWrappers(parts[i]);

        return string.Join('.', parts);
    }

    private static string StripIdentifierWrappers(string identifier)
    {
        var normalized = identifier.Trim();

        while (normalized.Length >= 2)
        {
            var first = normalized[0];
            var last = normalized[^1];
            var hasWrapperPair =
                (first == '`' && last == '`') ||
                (first == '"' && last == '"') ||
                (first == '[' && last == ']');

            if (!hasWrapperPair)
                break;

            normalized = normalized[1..^1].Trim();
        }

        return normalized;
    }
}
