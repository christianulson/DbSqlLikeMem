namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Provides MySQL-specific reserved words and function tokens.
/// PT: Fornece palavras reservadas e tokens de funcoes especificos do MySQL.
/// </summary>
public static class MySqlConst
{
    /// <summary>
    /// EN: MySQL AUTO_INCREMENT token.
    /// PT: Token AUTO_INCREMENT do MySQL.
    /// </summary>
    public const string AUTO_INCREMENT = "AUTO_INCREMENT";

    /// <summary>
    /// EN: MySQL SQL_CALC_FOUND_ROWS token.
    /// PT: Token SQL_CALC_FOUND_ROWS do MySQL.
    /// </summary>
    public const string SQL_CALC_FOUND_ROWS = "SQL_CALC_FOUND_ROWS";

    /// <summary>
    /// EN: MySQL VALUES token used by INSERT ... ON DUPLICATE KEY UPDATE.
    /// PT: Token VALUES do MySQL usado em INSERT ... ON DUPLICATE KEY UPDATE.
    /// </summary>
    public const string VALUES = "VALUES";

    /// <summary>
    /// EN: MySQL VALUE token used by INSERT ... ON DUPLICATE KEY UPDATE.
    /// PT: Token VALUE do MySQL usado em INSERT ... ON DUPLICATE KEY UPDATE.
    /// </summary>
    public const string VALUE = "VALUE";

    /// <summary>
    /// EN: MySQL LOW_PRIORITY token.
    /// PT: Token LOW_PRIORITY do MySQL.
    /// </summary>
    public const string LOW_PRIORITY = "LOW_PRIORITY";

    /// <summary>
    /// EN: MySQL DELAYED token.
    /// PT: Token DELAYED do MySQL.
    /// </summary>
    public const string DELAYED = "DELAYED";

    /// <summary>
    /// EN: MySQL HIGH_PRIORITY token.
    /// PT: Token HIGH_PRIORITY do MySQL.
    /// </summary>
    public const string HIGH_PRIORITY = "HIGH_PRIORITY";

    /// <summary>
    /// EN: MySQL IGNORE token.
    /// PT: Token IGNORE do MySQL.
    /// </summary>
    public const string IGNORE = "IGNORE";
}
