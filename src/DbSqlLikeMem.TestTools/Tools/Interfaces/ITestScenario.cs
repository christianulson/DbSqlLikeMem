namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Describes the contract for creating and dropping scenario data.
/// PT: Descreve o contrato para criar e remover dados de cenario.
/// </summary>
public interface ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the scenario data using the supplied test service.
    /// PT: Cria os dados do cenario usando o servico de teste informado.
    /// </summary>
    /// <param name="service">EN: The test service that executes SQL commands. PT: O servico de teste que executa comandos SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars);

    /// <summary>
    /// EN: Removes the scenario data using the supplied test service.
    /// PT: Remove os dados do cenario usando o servico de teste informado.
    /// </summary>
    /// <param name="service">EN: The test service that executes SQL commands. PT: O servico de teste que executa comandos SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars);
}
