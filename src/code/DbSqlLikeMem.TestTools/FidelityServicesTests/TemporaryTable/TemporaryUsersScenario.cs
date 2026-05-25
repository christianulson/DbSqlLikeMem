namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Creates and drops the temporary users table used by rollback and isolation workflows.
/// PT-br: Cria e remove a tabela temporaria de usuarios usada pelos fluxos de rollback e isolamento.
/// </summary>
public class TemporaryUsersScenario(
        RepoService repo,
        FidelityTestContext context
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <summary>
    /// EN: Creates the temporary users table required by the workflow.
    /// PT-br: Cria a tabela temporaria de usuarios exigida pelo fluxo.
    /// </summary>
    public virtual Task CreateScenarioAsync()
    {
        if (Repo.Dialect.Provider == ProviderId.Oracle
            && Repo.Cnn is not DbConnectionMockBase)
        {
            return CreateOracleTemporaryUsersTableAsync();
        }

        return Repo.ExecuteNonQueryAsync(GetCreateTemporaryUsersTableSql());
    }

    /// <summary>
    /// EN: Drops the temporary users table created for the workflow.
    /// PT-br: Remove a tabela temporaria de usuarios criada para o fluxo.
    /// </summary>
    public async Task DropScenarioAsync()
    {
        if (Repo.Dialect.Provider == ProviderId.Oracle
            && Repo.Cnn is not DbConnectionMockBase)
        {
            await CleanupOracleTemporaryTableAsync(Repo.Dialect.TemporaryUsersTableName(Context));
            return;
        }

        try
        {
            await Repo.ExecuteNonQueryAsync(GetDropTemporaryUsersTableSql());
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {

        }
    }

    private string GetDropTemporaryUsersTableSql()
    {
        return Repo.Dialect.Provider == ProviderId.Db2 && Repo.Cnn is DbConnectionMockBase
            ? $"DROP TABLE {Repo.Dialect.TemporaryUsersTableName(Context)}"
            : Repo.Dialect.DropTemporaryUsersTable(Context);
    }

    private string GetCreateTemporaryUsersTableSql()
    {
        return Repo.Dialect.Provider == ProviderId.Db2 && Repo.Cnn is DbConnectionMockBase
            ? $@"
CREATE TEMPORARY TABLE {Repo.Dialect.TemporaryUsersTableName(Context)} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    TenantId INT NOT NULL
)"
            : Repo.Dialect.CreateTemporaryUsersTable(Context);
    }

    private async Task CreateOracleTemporaryUsersTableAsync()
    {
        await CleanupOracleTemporaryTableAsync(Repo.Dialect.TemporaryUsersTableName(Context));
        await Repo.ExecuteNonQueryAsync(GetCreateTemporaryUsersTableSql());
    }

    private async Task CleanupOracleTemporaryTableAsync(string tableName)
    {
        try
        {
            await Repo.ExecuteNonQueryAsync($"TRUNCATE TABLE {tableName}");
        }
        catch
        {
            // Ignore cleanup failures during benchmark teardown.
        }

        try
        {
            await Repo.ExecuteNonQueryAsync($"DROP TABLE {tableName} PURGE");
        }
        catch
        {
            // Ignore cleanup failures during benchmark teardown.
        }
    }

    private static bool IsMissingTableException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-14452", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
