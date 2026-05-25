namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Creates the users table and seed row required by the primary-key select scenario.
/// PT-br: Cria a tabela de usuarios e a linha inicial exigidas pelo cenario de selecao por chave primaria.
/// </summary>
public class SelectTableScenario(
        RepoService repo,
        FidelityTestContext context
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <summary>
    /// EN: Creates the users table and inserts the seed row used by the select scenario.
    /// PT-br: Cria a tabela de usuarios e insere a linha base usada pelo cenario de select.
    /// </summary>
    public async Task CreateScenarioAsync()
    {
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 1, "Alice"));
    }

    /// <summary>
    /// EN: Drops the users table created for the select scenario.
    /// PT-br: Remove a tabela de usuarios criada para o cenario de select.
    /// </summary>
    public Task DropScenarioAsync()
    {
        return Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
    }
}
