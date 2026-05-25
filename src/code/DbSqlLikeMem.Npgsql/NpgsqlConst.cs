namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Provides PostgreSQL-specific reserved words and function tokens.
/// PT-br: Fornece palavras reservadas e tokens de funcoes especificos do PostgreSQL.
/// </summary>
public static class NpgsqlConst
{
    /// <summary>
    /// EN: PostgreSQL ILIKE token.
    /// PT-br: Token ILIKE do PostgreSQL.
    /// </summary>
    public const string ILIKE = "ILIKE";

    /// <summary>
    /// EN: PostgreSQL sequence next value token.
    /// PT-br: Token de proxima sequencia do PostgreSQL.
    /// </summary>
    public const string NEXTVAL = "NEXTVAL";

    /// <summary>
    /// EN: PostgreSQL sequence current value token.
    /// PT-br: Token de valor atual de sequencia do PostgreSQL.
    /// </summary>
    public const string CURRVAL = "CURRVAL";

    /// <summary>
    /// EN: PostgreSQL sequence set value token.
    /// PT-br: Token de definicao de valor de sequencia do PostgreSQL.
    /// </summary>
    public const string SETVAL = "SETVAL";

    /// <summary>
    /// EN: PostgreSQL sequence last value token.
    /// PT-br: Token de ultimo valor de sequencia do PostgreSQL.
    /// </summary>
    public const string LASTVAL = "LASTVAL";
}
