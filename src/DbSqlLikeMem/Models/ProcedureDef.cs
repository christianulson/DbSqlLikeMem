namespace DbSqlLikeMem;

/// <summary>
/// Representa um parâmetro de procedimento armazenado, com tipo e valor padrão.
/// </summary>
public sealed record ProcParam(
    string Name,
    DbType DbType,
    bool Required = true,
    object? Value = null);

/// <summary>
/// Define a assinatura de um procedimento armazenado em memória.
/// </summary>
public sealed record ProcedureDef(
    IReadOnlyList<ProcParam> RequiredIn,
    IReadOnlyList<ProcParam> OptionalIn,
    IReadOnlyList<ProcParam> OutParams,
    ProcParam? ReturnParam = null)
{
    /// <summary>
    /// Normaliza o nome do parâmetro removendo prefixos e espaços.
    /// </summary>
    /// <param name="name">Nome a normalizar.</param>
    /// <returns>Nome normalizado.</returns>
    public static string NormalizeParamName(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) return string.Empty;
        name = name.Trim();
        if (name.StartsWith('@')) name = name[1..];
        return name;
    }
}
