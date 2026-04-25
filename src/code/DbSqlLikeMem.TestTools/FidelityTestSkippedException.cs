namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Represents a fidelity-test skip triggered by unavailable provider infrastructure or missing container configuration.
/// PT: Representa um skip de teste de fidelidade disparado por infraestrutura do provedor indisponivel ou configuracao de container ausente.
/// </summary>
public sealed class FidelityTestSkippedException : Exception
{
    /// <summary>
    /// EN: Creates a new skip exception with a default reason.
    /// PT: Cria uma nova excecao de skip com um motivo padrao.
    /// </summary>
    public FidelityTestSkippedException()
        : this("A fidelity test was skipped.")
    {
    }

    /// <summary>
    /// EN: Creates a new skip exception with the provided reason.
    /// PT: Cria uma nova excecao de skip com o motivo informado.
    /// </summary>
    /// <param name="message">EN: Skip reason reported to the test runner. PT: Motivo do skip informado ao runner de testes.</param>
    public FidelityTestSkippedException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// EN: Creates a new skip exception with the provided reason and includes the inner exception message.
    /// PT: Cria uma nova excecao de skip com o motivo informado e inclui a mensagem da excecao interna.
    /// </summary>
    /// <param name="message">EN: Skip reason reported to the test runner. PT: Motivo do skip informado ao runner de testes.</param>
    /// <param name="innerException">EN: The exception whose message is appended to the skip reason. PT: A excecao cuja mensagem e adicionada ao motivo do skip.</param>
    public FidelityTestSkippedException(string message, Exception innerException)
        : base($"{message} ({innerException.Message})", innerException)
    {
    }
}
