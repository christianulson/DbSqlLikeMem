namespace DbSqlLikeMem.Compatibility;

/// <summary>
/// EN: Exposes indexed access to tuple-like values used by compatibility helpers.
/// PT: Expõe acesso por índice a valores semelhantes a tuplas usados pelos helpers de compatibilidade.
/// </summary>
public interface ITuple
{
    /// <summary>
    /// EN: Gets the number of values available in the tuple-like instance.
    /// PT: Obtém a quantidade de valores disponíveis na instância semelhante a tupla.
    /// </summary>
    int Length { get; }

    /// <summary>
    /// EN: Gets the value stored at the specified zero-based position.
    /// PT: Obtém o valor armazenado na posição baseada em zero informada.
    /// </summary>
    object? this[int index] { get; }
}
