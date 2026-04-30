namespace DbSqlLikeMem;

internal static class SqlKeywords
{
    private static readonly HashSet<string> _keywords =
        new(StringComparer.OrdinalIgnoreCase)
        {
            SqlConst.AND,SqlConst.OR,SqlConst.NOT,
            SqlConst.IN,"LIKE",SqlConst.IS,SqlConst.NULL,"ESCAPE",
            SqlConst.TRUE,SqlConst.FALSE,
            SqlConst.NO,
            SqlConst.AS,
            SqlConst.BETWEEN,
            SqlConst.EXISTS,
            SqlConst.DISTINCT,
            SqlConst.BEFORE,SqlConst.AFTER,SqlConst.TRIGGER,SqlConst.EACH,SqlConst.REFERENCING,SqlConst.MODE,SqlConst.ATOMIC,
            "CASE",SqlConst.WHEN,SqlConst.THEN,SqlConst.ELSE,SqlConst.END,
            SqlConst.CYCLE,SqlConst.MINVALUE,SqlConst.MAXVALUE,
            SqlConst.OWNED,SqlConst.NONE
        };

    /// <summary>
    /// EN: Implements IsKeyword.
    /// PT: Implementa IsKeyword.
    /// </summary>
    public static bool IsKeyword(string text) => _keywords.Contains(text);

    /// <summary>
    /// EN: Checks whether the span matches a SQL keyword without allocating a string.
    /// PT: Verifica se o span corresponde a uma palavra-chave SQL sem alocar string.
    /// </summary>
    public static bool IsKeyword(ReadOnlySpan<char> text)
    {
        foreach (var keyword in _keywords)
        {
            if (text.Equals(keyword, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
