namespace DbSqlLikeMem;

/// <summary>
/// Signature-only stored procedure contract used by <see cref="MySqlConnectionMock"/>.
/// No execution body is modeled yet; only parameter validation and OUT defaults.
/// </summary>
public sealed record ProcParam(
    string Name,
    DbType DbType,
    bool Required = true,
    object? Value = null);

public sealed record ProcedureDef(
    IReadOnlyList<ProcParam> RequiredIn,
    IReadOnlyList<ProcParam> OptionalIn,
    IReadOnlyList<ProcParam> OutParams,
    ProcParam? ReturnParam = null)
{
    public static string NormalizeParamName(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) return string.Empty;
        name = name.Trim();
        if (name.StartsWith('@')) name = name[1..];
        return name;
    }
}
