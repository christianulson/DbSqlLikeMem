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
        if (dialect.Provider == ProviderId.SqlServer)
        {
            Console.WriteLine($"[TemporaryUsers][SqlServer] Connection: {service.Connection.GetType().FullName}");
            Console.WriteLine($"[TemporaryUsers][SqlServer] IsMock: {service.Connection is DbSqlLikeMem.DbConnectionMockBase}");
        }
        if (dialect.Provider == ProviderId.Npgsql)
        {
            Console.WriteLine($"[TemporaryUsers][Npgsql] Connection: {service.Connection.GetType().FullName}");
            Console.WriteLine($"[TemporaryUsers][Npgsql] IsMock: {service.Connection is DbSqlLikeMem.DbConnectionMockBase}");
        }
        if ((dialect.Provider == ProviderId.Sqlite
            || dialect.Provider == ProviderId.Oracle
            || dialect.Provider == ProviderId.Npgsql
            || dialect.Provider == ProviderId.Db2
            || dialect.Provider == ProviderId.SqlServer
            || dialect.Provider == ProviderId.SqlAzure)
            && service.Connection is DbSqlLikeMem.DbConnectionMockBase mockConnection)
        {
            if (dialect.Provider == ProviderId.Npgsql)
            {
                Console.WriteLine($"[TemporaryUsers][Npgsql] Branch: mock-temp-table-api");
            }
            var tempTable = mockConnection.AddTemporaryTable(tableName);
            tempTable.AddColumn("Id", DbType.Int32, false);
            tempTable.AddColumn("Name", DbType.String, false);
        }
        else
        {
            if (dialect.Provider == ProviderId.Db2
                && service.Connection is not DbSqlLikeMem.DbConnectionMockBase)
            {
                var sessionTableName = $"SESSION.{tableName}";
                var db2CreateSql = $@"
DECLARE GLOBAL TEMPORARY TABLE {sessionTableName} (
    Id INT,
    Name VARCHAR(100)
) ON COMMIT PRESERVE ROWS NOT LOGGED";
                Console.WriteLine($"[TemporaryUsers][Db2] Dialect: {dialect.GetType().FullName}");
                Console.WriteLine($"[TemporaryUsers][Db2] SQL: {db2CreateSql.Replace(Environment.NewLine, " ").Trim()}");
                service.ExecuteNonQuery(db2CreateSql);
                return;
            }

            if (dialect.Provider == ProviderId.SqlServer)
            {
                Console.WriteLine($"[TemporaryUsers][SqlServer] Branch: sql-create-temp-table");
            }
            if (dialect.Provider == ProviderId.Oracle)
            {
                var oracleCreateSql = $@"
CREATE GLOBAL TEMPORARY TABLE {tableName} (
    Id NUMBER(10),
    Name VARCHAR2(100)
) ON COMMIT PRESERVE ROWS";
                Console.WriteLine($"[TemporaryUsers][Oracle] Dialect: {dialect.GetType().FullName}");
                Console.WriteLine($"[TemporaryUsers][Oracle] SQL: {oracleCreateSql.Replace(Environment.NewLine, " ").Trim()}");
                service.ExecuteNonQuery(oracleCreateSql);
                return;
            }

            if (dialect.Provider == ProviderId.Npgsql)
            {
                Console.WriteLine($"[TemporaryUsers][Npgsql] Branch: sql-create-temp-table");
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
                service.ExecuteNonQuery($"DROP TABLE SESSION.{tableName}");
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
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
