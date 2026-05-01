namespace DbSqlLikeMem;
/// <summary>
/// EN: Token kinds produced by the SQL tokenizer.
/// PT-br: Tipos de tokens produzidos pelo tokenizador SQL.
/// </summary>
internal enum SqlTokenKind
{
    Identifier,
    Keyword,
    Number,
    String,
    Operator,
    Symbol,
    Parameter,
    EndOfFile
}

internal readonly record struct SqlToken(SqlTokenKind Kind, string Text, int Position)
{
    /// <summary>
    /// EN: Implements new.
    /// PT-br: Implementa new.
    /// </summary>
    public static readonly SqlToken EOF = new(SqlTokenKind.EndOfFile, "<EOF>", -1);
    /// <summary>
    /// EN: Implements ToString.
    /// PT-br: Implementa ToString.
    /// </summary>
    public override string ToString() => $"{Kind}: {Text} @{Position}";
}
