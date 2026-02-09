namespace DbSqlLikeMem;

internal static class SqlStringExtencions
{
    public static string NormalizeString(this string str)
    {
        var s = str.Replace("\n", " ", StringComparison.Ordinal)
              .Replace("\r", " ", StringComparison.Ordinal)
              .Replace("\t", " ", StringComparison.Ordinal);
        while (s.Contains("  ", StringComparison.Ordinal))
            s = s.Replace("  ", " ", StringComparison.Ordinal);
        return s.Trim();
    }

    public static string NormalizeName(this string name)
    {
        name = name.Trim();
        name = name.Trim('`');       // remove `User`
        name = name.Trim();          // de novo por segurança
        return name;
    }
}
