namespace DbSqlLikeMem;
internal static class SqlKeywords
{
    private static readonly HashSet<string> _keywords =
        new(StringComparer.OrdinalIgnoreCase)
        {
            SqlConst.AND,SqlConst.OR,SqlConst.NOT,
            SqlConst.IN,"LIKE",SqlConst.IS,SqlConst.NULL,"ESCAPE",
            SqlConst.TRUE,SqlConst.FALSE,
            SqlConst.AS,
            SqlConst.BETWEEN,
            SqlConst.DISTINCT,
            "CASE",SqlConst.WHEN,SqlConst.THEN,SqlConst.ELSE,SqlConst.END
        };

    /// <summary>
    /// EN: Implements IsKeyword.
    /// PT: Implementa IsKeyword.
    /// </summary>
    public static bool IsKeyword(string text) => _keywords.Contains(text);
}
