namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the users table used by insert workflows.
/// PT: Cria e remove a tabela de usuarios usada pelos fluxos de insert.
/// </summary>
public class InsertUsersScenario(
    RepoService repo,
       FidelityTestContext context
    ) : BaseScenario(repo, context), ITestScenario
{
    /// <summary>
    /// EN: Creates the users table required by the insert workflow.
    /// PT: Cria a tabela de usuarios exigida pelo fluxo de insert.
    /// </summary>
    public Task CreateScenarioAsync()
    => Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));

    /// <summary>
    /// EN: Drops the users table created for the insert workflow.
    /// PT: Remove a tabela de usuarios criada para o fluxo de insert.
    /// </summary>
    public Task DropScenarioAsync()
    => Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
}
