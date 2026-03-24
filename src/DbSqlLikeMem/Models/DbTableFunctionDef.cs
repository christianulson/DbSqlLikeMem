namespace DbSqlLikeMem.Models;

internal sealed record DbTableFunctionDef(
    string Name,
    int MinArguments,
    int MaxArguments)
    : ProcessDef(Name)
{
    internal bool AllowsArgumentCount(int count)
        => count >= MinArguments && count <= MaxArguments;
}
