namespace DbSqlLikeMem;
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
    public static readonly SqlToken EOF = new(SqlTokenKind.EndOfFile, "<EOF>", -1);
    public override string ToString() => $"{Kind}: {Text} @{Position}";
}
