namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Represents a fidelity-test skip triggered by unavailable provider infrastructure or missing container configuration.
/// PT: Representa um skip de teste de fidelidade disparado por infraestrutura do provedor indisponivel ou configuracao de container ausente.
/// </summary>
public sealed class FidelityTestSkippedException : Exception
{
    /// <summary>
    /// EN: Creates a new skip exception with the provided reason.
    /// PT: Cria uma nova excecao de skip com o motivo informado.
    /// </summary>
    /// <param name="message">EN: Skip reason reported to the test runner. PT: Motivo do skip informado ao runner de testes.</param>
    public FidelityTestSkippedException(string message)
        : base(message)
    {
    }
}
