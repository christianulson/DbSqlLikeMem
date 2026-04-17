namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Describes the contract for creating and dropping scenario data.
/// PT: Descreve o contrato para criar e remover dados de cenario.
/// </summary>
public interface ITestScenario
{
    /// <summary>
    /// EN: Creates the scenario data using the supplied test service.
    /// PT: Cria os dados do cenario usando o servico de teste informado.
    /// </summary>
    Task CreateScenarioAsync();

    /// <summary>
    /// EN: Removes the scenario data using the supplied test service.
    /// PT: Remove os dados do cenario usando o servico de teste informado.
    /// </summary>
    Task DropScenarioAsync();
}
