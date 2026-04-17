namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops a users table with optional seed rows for DML mutation workflows.
/// PT: Cria e remove uma tabela de usuarios com linhas iniciais opcionais para fluxos de mutacao DML.
/// </summary>
public sealed class UsersScenario(
        RepoService repo,
        FidelityTestContext context,
        params (int id, string name)[] seedRows
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <summary>
    /// EN: Creates the users table and seeds the configured rows.
    /// PT: Cria a tabela de usuarios e preenche as linhas configuradas.
    /// </summary>
    public async Task CreateScenarioAsync()
    {

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));
        foreach (var (id, name) in seedRows)
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, name));
        }
    }

    /// <summary>
    /// EN: Drops the users table created for the workflow.
    /// PT: Remove a tabela de usuarios criada para o fluxo.
    /// </summary>
    public Task DropScenarioAsync()
    => Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
}
