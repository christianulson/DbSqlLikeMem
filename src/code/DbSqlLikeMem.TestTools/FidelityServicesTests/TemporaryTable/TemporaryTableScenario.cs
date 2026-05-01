namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Creates and drops the source users table used by temporary-table workflows.
/// PT-br: Cria e remove a tabela fonte de usuarios usada pelos fluxos de tabela temporaria.
/// </summary>
public sealed class TemporaryTableScenario(
        RepoService repo,
        FidelityTestContext context
    ) : TemporaryUsersScenario(repo, context)
{
    /// <summary>
    /// EN: Creates the source users table and seeds the rows used by the temporary-table tests.
    /// PT-br: Cria a tabela fonte de usuarios e preenche as linhas usadas pelos testes de tabela temporaria.
    /// </summary>
    public override async Task CreateScenarioAsync()
    {
        await base.CreateScenarioAsync();
        var tempUsersTable = Repo.Dialect.Provider == ProviderId.Db2 && Repo.Cnn is not DbConnectionMockBase
            ? $"SESSION.{Repo.Dialect.TemporaryUsersTableName(Context)}"
            : Repo.Dialect.TemporaryUsersTableName(Context);

        await Repo.ExecuteNonQueryAsync(InsertTemporaryUsersTableSql(tempUsersTable, 1, "John", 10));
        await Repo.ExecuteNonQueryAsync(InsertTemporaryUsersTableSql(tempUsersTable, 2, "Bob", 10));
        await Repo.ExecuteNonQueryAsync(InsertTemporaryUsersTableSql(tempUsersTable, 3, "Jane", 20));
    }

    private static string InsertTemporaryUsersTableSql(string tableName, int id, string name, int tenantId)
        => $"INSERT INTO {tableName} (Id, Name, TenantId) VALUES ({id}, '{name}', {tenantId})";
}
