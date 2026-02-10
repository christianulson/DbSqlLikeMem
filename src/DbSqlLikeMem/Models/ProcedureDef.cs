namespace DbSqlLikeMem;

/// <summary>
/// EN: Represents a stored procedure parameter with type and default value.
/// PT: Representa um parâmetro de procedimento armazenado, com tipo e valor padrão.
/// </summary>
public sealed record ProcParam(
    string Name,
    DbType DbType,
    bool Required = true,
    object? Value = null);

/// <summary>
/// EN: Defines the signature of an in-memory stored procedure.
/// PT: Define a assinatura de um procedimento armazenado em memória.
/// </summary>
public sealed record ProcedureDef(
    IReadOnlyList<ProcParam> RequiredIn,
    IReadOnlyList<ProcParam> OptionalIn,
    IReadOnlyList<ProcParam> OutParams,
    ProcParam? ReturnParam = null)
{
    /// <summary>
    /// EN: Normalizes the parameter name by removing prefixes and spaces.
    /// PT: Normaliza o nome do parâmetro removendo prefixos e espaços.
    /// </summary>
    /// <param name="name">EN: Name to normalize. PT: Nome a normalizar.</param>
    /// <returns>EN: Normalized name. PT: Nome normalizado.</returns>
    public static string NormalizeParamName(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) return string.Empty;
        name = name.Trim();
        if (name.StartsWith("@")) name = name[1..];
        return name;
    }
}
