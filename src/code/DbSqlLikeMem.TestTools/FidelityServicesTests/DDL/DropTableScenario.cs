namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Drops the users table used by the cleanup scenario.
/// PT-br: Remove a tabela de usuarios usada pelo cenario de limpeza.
/// </summary>
public class DropTableScenario(
    RepoService repo,
       FidelityTestContext context
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <summary>
    /// EN: Seeds the users table so the drop scenario has a table to remove.
    /// PT-br: Preenche a tabela de usuarios para que o cenario de remocao tenha uma tabela para excluir.
    /// </summary>
    public Task CreateScenarioAsync()
        => Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));

    /// <summary>
    /// EN: Leaves the drop step empty because the cleanup scenario does not need extra setup.
    /// PT-br: Deixa a etapa de remocao vazia porque o cenario de limpeza nao precisa de preparacao extra.
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    public virtual Task DropScenarioAsync()
        => Task.CompletedTask;
}
