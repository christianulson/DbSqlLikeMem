namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Creates and drops the temporary users table used by rollback and isolation workflows.
/// PT: Cria e remove a tabela temporaria de usuarios usada pelos fluxos de rollback e isolamento.
/// </summary>
public sealed class TemporaryUsersScenario<T>(ProviderSqlDialect dialect) : ITestScenario<T>
    where T : DbConnection
{
    private static string ResolveTemporaryUsersTableName(
        ProviderSqlDialect dialect,
        BaseServiceTest<T> service,
        string rawTableName)
    {
        // NOTE: The tokenizer does not treat '#' as an identifier start, so mock flows must avoid it.
        // Real SQL Server / Azure SQL temp tables still use '#...' in container comparisons.
        if ((dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
            && service.Connection is DbConnectionMockBase)
        {
            return rawTableName;
        }

        return dialect.TemporaryUsersTableName(rawTableName);
    }

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
        var rawTableName = (string)pars[0];
        var tableName = ResolveTemporaryUsersTableName(dialect, service, rawTableName);
        if ((dialect.Provider == ProviderId.Sqlite
            || dialect.Provider == ProviderId.Oracle
            || dialect.Provider == ProviderId.Npgsql
            || dialect.Provider == ProviderId.Db2
            || dialect.Provider == ProviderId.SqlServer
            || dialect.Provider == ProviderId.SqlAzure)
            && service.Connection is DbConnectionMockBase mockConnection)
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
            var createSql = dialect.CreateTemporaryUsersTable(rawTableName);
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
        var rawTableName = (string)pars[0];
        var tableName = ResolveTemporaryUsersTableName(dialect, service, rawTableName);
        try
        {
            if (dialect.Provider == ProviderId.Oracle
                && service.Connection is not DbConnectionMockBase)
            {
                // EN: Oracle global temporary tables can remain in place after a test run.
                // PT: Tabelas temporarias globais do Oracle podem permanecer criadas apos a execucao do teste.
                return;
            }

            if (dialect.Provider == ProviderId.Db2
                && service.Connection is DbConnectionMockBase)
            {
                service.ExecuteNonQuery($"DROP TEMPORARY TABLE {tableName}");
            }
            else if (dialect.Provider == ProviderId.Db2)
            {
                // EN: Declared global temporary tables are session-scoped and are dropped when the session ends.
                // PT: Tabelas temporarias globais declaradas sao dependentes da sessao e sao removidas ao final da sessao.
                return;
            }
            else if ((dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
                && service.Connection is DbConnectionMockBase)
            {
                service.ExecuteNonQuery($"DROP TABLE {tableName}");
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
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
