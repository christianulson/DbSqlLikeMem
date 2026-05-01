namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides the skip reason for a fidelity test marker attribute.
/// PT-br: Fornece o motivo de skip para um atributo marcador de teste de fidelidade.
/// </summary>
public interface IFidelityTestSkipProvider
{
    /// <summary>
    /// EN: Returns the skip reason when the associated provider is unavailable.
    /// PT-br: Retorna o motivo de skip quando o provedor associado estiver indisponivel.
    /// </summary>
    /// <returns>EN: The skip reason, or null when the test should run. PT-br: O motivo de skip, ou null quando o teste deve executar.</returns>
    string? GetSkipReason();
}
