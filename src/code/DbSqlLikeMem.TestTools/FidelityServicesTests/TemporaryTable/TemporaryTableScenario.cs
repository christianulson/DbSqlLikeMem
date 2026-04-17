namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Creates and drops the source users table used by temporary-table workflows.
/// PT: Cria e remove a tabela fonte de usuarios usada pelos fluxos de tabela temporaria.
/// </summary>
public sealed class TemporaryTableScenario(
        RepoService repo,
        FidelityTestContext context
    ) : TemporaryUsersScenario(repo, context)
{
    /// <summary>
    /// EN: Creates the source users table and seeds the rows used by the temporary-table tests.
    /// PT: Cria a tabela fonte de usuarios e preenche as linhas usadas pelos testes de tabela temporaria.
    /// </summary>
    public override async Task CreateScenarioAsync()
    {
        await base.CreateScenarioAsync();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertTemporaryUsersTable(Context, 1, "John", 10));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertTemporaryUsersTable(Context, 2, "Bob", 10));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertTemporaryUsersTable(Context, 3, "Jane", 20));
    }
}
