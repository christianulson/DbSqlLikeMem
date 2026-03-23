namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Creates and drops the temporary users table used by rollback and isolation workflows.
/// PT: Cria e remove a tabela temporaria de usuarios usada pelos fluxos de rollback e isolamento.
/// </summary>
public sealed class TemporaryUsersScenario<T>(ProviderSqlDialect dialect) : ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the temporary users table required by the workflow.
    /// PT: Cria a tabela temporaria de usuarios exigida pelo fluxo.
    /// </summary>
    /// <param name="service">EN: The shared test service used to execute SQL. PT: O servico de teste compartilhado usado para executar SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var tableName = dialect.TemporaryUsersTableName((string)pars[0]);
        if ((dialect.Provider == ProviderId.Sqlite
            || dialect.Provider == ProviderId.Oracle
            || dialect.Provider == ProviderId.Npgsql
            || dialect.Provider == ProviderId.Db2
            || dialect.Provider == ProviderId.SqlServer
            || dialect.Provider == ProviderId.SqlAzure)
            && service.Connection is DbSqlLikeMem.DbConnectionMockBase mockConnection)
        {
            var tempTable = mockConnection.AddTemporaryTable(tableName);
            tempTable.AddColumn("Id", DbType.Int32, false);
            tempTable.AddColumn("Name", DbType.String, false);
        }
        else
        {
            if (dialect.Provider == ProviderId.Oracle)
            {
                var oracleCreateSql = $@"
CREATE GLOBAL TEMPORARY TABLE {tableName} (
    Id NUMBER(10),
    Name VARCHAR2(100)
) ON COMMIT PRESERVE ROWS";
                service.ExecuteNonQuery(oracleCreateSql);
                return;
            }
            var createSql = dialect.CreateTemporaryUsersTable(tableName);
            service.ExecuteNonQuery(createSql);
        }
    }

    /// <summary>
    /// EN: Drops the temporary users table created for the workflow.
    /// PT: Remove a tabela temporaria de usuarios criada para o fluxo.
    /// </summary>
    /// <param name="service">EN: The shared test service used to execute SQL. PT: O servico de teste compartilhado usado para executar SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var tableName = dialect.TemporaryUsersTableName((string)pars[0]);
        try
        {
            if (dialect.Provider == ProviderId.Db2
                && service.Connection is DbSqlLikeMem.DbConnectionMockBase)
            {
                service.ExecuteNonQuery($"DROP TEMPORARY TABLE {tableName}");
            }
            else if (dialect.Provider == ProviderId.Db2)
            {
                // EN: Declared global temporary tables are session-scoped and are dropped when the session ends.
                // PT: Tabelas temporarias globais declaradas sao dependentes da sessao e sao removidas ao final da sessao.
                return;
            }
            else
            {
                service.ExecuteNonQuery(dialect.DropTemporaryUsersTable(tableName));
            }
        }
        catch (Exception ex)
        {
            if (!IsMissingTemporaryTableException(ex))
            {
                throw;
            }
        }
    }

    private static bool IsMissingTemporaryTableException(Exception ex)
    {
        if (ex is KeyNotFoundException)
        {
            return true;
        }

        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
