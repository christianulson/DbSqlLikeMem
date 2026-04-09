namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Provides PostgreSQL-specific reserved words and function tokens.
/// PT: Fornece palavras reservadas e tokens de funcoes especificos do PostgreSQL.
/// </summary>
public static class NpgsqlConst
{
    /// <summary>
    /// EN: PostgreSQL ILIKE token.
    /// PT: Token ILIKE do PostgreSQL.
    /// </summary>
    public const string ILIKE = "ILIKE";

    /// <summary>
    /// EN: PostgreSQL sequence next value token.
    /// PT: Token de proxima sequencia do PostgreSQL.
    /// </summary>
    public const string NEXTVAL = "NEXTVAL";

    /// <summary>
    /// EN: PostgreSQL sequence current value token.
    /// PT: Token de valor atual de sequencia do PostgreSQL.
    /// </summary>
    public const string CURRVAL = "CURRVAL";

    /// <summary>
    /// EN: PostgreSQL sequence set value token.
    /// PT: Token de definicao de valor de sequencia do PostgreSQL.
    /// </summary>
    public const string SETVAL = "SETVAL";

    /// <summary>
    /// EN: PostgreSQL sequence last value token.
    /// PT: Token de ultimo valor de sequencia do PostgreSQL.
    /// </summary>
    public const string LASTVAL = "LASTVAL";
}
