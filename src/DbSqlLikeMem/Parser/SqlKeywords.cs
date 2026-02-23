namespace DbSqlLikeMem;
internal static class SqlKeywords
{
    private static readonly HashSet<string> _keywords =
        new(StringComparer.OrdinalIgnoreCase)
        {
            "AND","OR","NOT",
            "IN","LIKE","IS","NULL",
            "TRUE","FALSE",
            "AS",
            "BETWEEN",
            "DISTINCT",
            "CASE","WHEN","THEN","ELSE","END"
        };

    /// <summary>
    /// EN: Implements IsKeyword.
    /// PT: Implementa IsKeyword.
    /// </summary>
    public static bool IsKeyword(string text) => _keywords.Contains(text);
}
