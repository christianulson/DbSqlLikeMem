namespace DbSqlLikeMem.Models;

/// <summary>
/// EN: Describes a window function supported by a dialect registry.
/// PT: Descreve uma funcao de janela suportada por um registry de dialeto.
/// </summary>
internal sealed record DbWindowFunctionDef(
    string Name,
    int MinArguments,
    int MaxArguments,
    bool RequiresOrderBy)
    : ProcessDef(Name)
{
    /// <summary>
    /// EN: Checks whether the provided argument count is accepted by the function.
    /// PT: Verifica se a quantidade de argumentos informada e aceita pela funcao.
    /// </summary>
    /// <param name="count">EN: Argument count to validate. PT: Quantidade de argumentos a validar.</param>
    /// <returns>EN: True when the count is within range. PT: True quando a quantidade esta no intervalo.</returns>
    internal bool AllowsArgumentCount(int count)
        => count >= MinArguments && count <= MaxArguments;
}
