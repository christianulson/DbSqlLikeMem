namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Describes a no-op scenario setup for workflows that only need an open connection.
/// PT-br: Descreve uma configuracao de cenario sem operacao para fluxos que precisam apenas de uma conexao aberta.
/// </summary>
public sealed class NoopScenario(
    RepoService repo,
       FidelityTestContext context
    ) : BaseScenario(repo, context), ITestScenario
{
    /// <summary>
    /// EN: Leaves scenario creation empty.
    /// PT-br: Deixa a criação do cenário vazia.
    /// </summary>
    public Task CreateScenarioAsync()
    => Task.CompletedTask;

    /// <summary>
    /// EN: Leaves scenario cleanup empty.
    /// PT-br: Deixa a limpeza do cenário vazia.
    /// </summary>
    public Task DropScenarioAsync()
    => Task.CompletedTask;
}
