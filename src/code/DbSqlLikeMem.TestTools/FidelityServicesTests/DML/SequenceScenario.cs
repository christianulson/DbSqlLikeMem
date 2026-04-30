namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops a sequence used by the sequence benchmark workflow.
/// PT: Cria e remove uma sequência usada pelo fluxo de benchmark de sequência.
/// </summary>
public sealed class SequenceScenario(
    RepoService repo,
       FidelityTestContext context
    ) : BaseScenario(repo, context), ITestScenario
{
    /// <summary>
    /// EN: Creates the requested sequence.
    /// PT: Cria a sequência solicitada.
    /// </summary>
    public Task CreateScenarioAsync()
    => Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateSequence(Context));
    

    /// <summary>
    /// EN: Drops the requested sequence.
    /// PT: Remove a sequência solicitada.
    /// </summary>
    public Task DropScenarioAsync()
    => Repo.ExecuteNonQueryAsync(Repo.Dialect.DropSequence(Context));
}
