namespace DbSqlLikeMem.TestTools.TemporaryTable;

public partial class TemporaryTableServiceTest<T>
{
    /// <summary>
    /// EN: Creates a temporary table from the source users table and returns the projected row identifiers.
    /// PT: Cria uma tabela temporaria a partir da tabela fonte de usuarios e retorna os identificadores projetados das linhas.
    /// </summary>
    public List<int> RunCreateTemporaryTableAsSelectThenSelect(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var sourceUsersTable = ResolveSourceUsersTableName(users, uId);
        var isMockConnection = Connection is DbConnectionMockBase;
        var tempTable = BuildTemporaryTableName(uId, isMockConnection);
        var sessionTempTable = Dialect.Provider == ProviderId.Db2 && !isMockConnection
            ? $"SESSION.{tempTable}"
            : tempTable;
        if (Dialect.Provider == ProviderId.Npgsql)
        {
            ExecuteNonQuery($@"
CREATE TEMP TABLE {tempTable} AS
SELECT Id, Name FROM {sourceUsersTable} WHERE TenantId = 10");
        }
        else if (Dialect.Provider == ProviderId.Db2 && !isMockConnection)
        {
            ExecuteNonQuery($@"
DECLARE GLOBAL TEMPORARY TABLE SESSION.{tempTable} (
    Id INT,
    Name VARCHAR(100)
) ON COMMIT PRESERVE ROWS NOT LOGGED");
            ExecuteNonQuery($@"
INSERT INTO SESSION.{tempTable} (Id, Name)
SELECT Id, Name FROM {sourceUsersTable} WHERE TenantId = 10");
        }
        else if (Dialect.Provider == ProviderId.Db2)
        {
            if (Connection is not DbConnectionMockBase mockConnection)
            {
                throw new InvalidOperationException("Db2 temporary table mock flow requires a mock connection.");
            }

            var tempTableMock = mockConnection.AddTemporaryTable(tempTable);
            tempTableMock.AddColumn("Id", DbType.Int32, false);
            tempTableMock.AddColumn("Name", DbType.String, false);
            ExecuteNonQuery($@"
INSERT INTO {tempTable} (Id, Name)
SELECT Id, Name FROM {sourceUsersTable} WHERE TenantId = 10");
        }
        else if ((Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure) && isMockConnection)
        {
            ExecuteNonQuery($@"
CREATE TEMPORARY TABLE {tempTable} AS
SELECT Id, Name FROM {sourceUsersTable} WHERE TenantId = 10");
        }
        else if (Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
        {
            ExecuteNonQuery($@"
SELECT Id, Name INTO {tempTable} FROM {sourceUsersTable} WHERE TenantId = 10");
        }
        else if (Dialect.Provider == ProviderId.Oracle
            && Connection is not DbSqlLikeMem.DbConnectionMockBase)
        {
            ExecuteNonQuery($@"
CREATE GLOBAL TEMPORARY TABLE {tempTable}
ON COMMIT PRESERVE ROWS
AS SELECT Id, Name
FROM {sourceUsersTable}
WHERE TenantId = 10");
        }
        else if (Dialect.Provider == ProviderId.Firebird
            && Connection is not DbConnectionMockBase)
        {
            ExecuteNonQuery($@"
CREATE GLOBAL TEMPORARY TABLE {tempTable} (
    Id INTEGER,
    Name VARCHAR(100)
) ON COMMIT PRESERVE ROWS");
            ExecuteNonQuery($@"
INSERT INTO {tempTable} (Id, Name)
SELECT Id, Name FROM {sourceUsersTable} WHERE TenantId = 10");
        }
        else
        {
            var createSql = BuildCreateTemporaryTableSql(tempTable, sourceUsersTable);
            ExecuteNonQuery(createSql);
        }

        var ids = new List<int>();
        try
        {
            using var command = Connection.CreateCommand();
            command.CommandText = $"SELECT Id FROM {sessionTempTable} ORDER BY Id";

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                ids.Add(reader.GetInt32(0));
            }

            if (ids.Count != 2 || ids[0] != 1 || ids[1] != 2)
            {
                throw new InvalidOperationException($"Unexpected temporary-table projected rows for {Dialect.DisplayName}: [{string.Join(",", ids)}].");
            }

            GC.KeepAlive(ids);
            return ids;
        }
        finally
        {
            TryDropTemporaryTable(tempTable);
        }
    }

    private string BuildTemporaryTableName(string uId, bool isMockConnection)
    {
        if (Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
        {
            return isMockConnection ? $"tmp_users_{uId}" : $"#tmp_users_{uId}";
        }

        return Dialect.Provider == ProviderId.Npgsql
            ? $"pg_temp.tmp_users_{uId}"
            : $"tmp_users_{uId}";
    }

    private string BuildCreateTemporaryTableSql(
        string tempTable,
        string sourceUsersTable)
    {
        return $@"
CREATE TEMPORARY TABLE {tempTable} AS
SELECT Id, Name FROM {sourceUsersTable} WHERE TenantId = 10";
    }

    /// <summary>
    /// EN: Counts the rows available in the temporary-table scenario.
    /// PT: Conta as linhas disponiveis no cenario de tabela temporaria.
    /// </summary>
    public int RunTempTableCreateAndUse(params object[] pars)
    {
        var users = ResolveTemporaryUsersTableName((string)pars[0]);
        var sessionUsers = Dialect.Provider == ProviderId.Db2 && Connection is not DbSqlLikeMem.DbConnectionMockBase
            ? $"SESSION.{users}"
            : users;
        ExecuteNonQuery(InsertTemporaryRowSql(sessionUsers, 1, "Alice"));
        var count = Convert.ToInt32(ExecuteScalar(CountRowsSql(sessionUsers)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected temporary-table rowcount for {Dialect.DisplayName}: {count}.");
        }

        GC.KeepAlive(count);
        return count;
    }

    /// <summary>
    /// EN: Opens a transaction, uses a savepoint, and rolls the work back for the temporary-table scenario.
    /// PT: Abre uma transacao, usa um savepoint e desfaz o trabalho para o cenario de tabela temporaria.
    /// </summary>
    public void RunTempTableRollback(params object[] pars)
    {
        var users = ResolveTemporaryUsersTableName((string)pars[0]);
        var sessionUsers = Dialect.Provider == ProviderId.Db2 && Connection is not DbSqlLikeMem.DbConnectionMockBase
            ? $"SESSION.{users}"
            : users;
        using var tx = Connection.BeginTransaction();
        ExecuteNonQuery(InsertTemporaryRowSql(sessionUsers, 1, "Alice"), tx);
        ExecuteNonQuery(InsertTemporaryRowSql(sessionUsers, 2, "Bob"), tx);
        tx.Rollback();

        var count = Convert.ToInt32(ExecuteScalar(CountRowsSql(sessionUsers)), CultureInfo.InvariantCulture);
        if (count != 0)
        {
            throw new InvalidOperationException($"Unexpected temporary-table rollback rowcount for {Dialect.DisplayName}: {count}.");
        }
    }

    /// <summary>
    /// EN: Verifies that a temporary table is not visible or not populated from a secondary connection.
    /// PT: Verifica se uma tabela temporaria nao fica visivel ou nao fica populada a partir de uma conexao secundaria.
    /// </summary>
    /// <param name="pars">EN: The temporary users table name. PT: O nome da tabela temporaria de usuarios.</param>
    /// <returns>EN: Zero when the secondary connection cannot observe the inserted temporary-table row. PT: Zero quando a conexao secundaria nao consegue observar a linha inserida na tabela temporaria.</returns>
    public int RunTemporaryTableCrossConnectionIsolation(params object[] pars)
    {
        if (connectionFactory is null)
        {
            throw new InvalidOperationException($"Cross-connection temporary-table workflows require a connection factory for {Dialect.DisplayName}.");
        }

        var users = ResolveTemporaryUsersTableName((string)pars[0]);
        var sessionUsers = Dialect.Provider == ProviderId.Db2 && Connection is not DbSqlLikeMem.DbConnectionMockBase
            ? $"SESSION.{users}"
            : users;
        ExecuteNonQuery(InsertTemporaryRowSql(sessionUsers, 1, "Alice"));

        using var secondaryConnection = connectionFactory();
        secondaryConnection.Open();

        try
        {
            var count = Convert.ToInt32(ExecuteScalarOnConnection(secondaryConnection, CountRowsSql(sessionUsers)), CultureInfo.InvariantCulture);
            if (count != 0)
            {
                throw new InvalidOperationException($"Unexpected temporary-table isolation rowcount for {Dialect.DisplayName}: {count}.");
            }

            GC.KeepAlive(count);
            return count;
        }
        catch (Exception ex)
        {
            if (IsMissingTemporaryTableException(ex))
            {
                return 0;
            }

            throw;
        }
    }

    private static object? ExecuteScalarOnConnection(
        T connection,
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        return command.ExecuteScalar();
    }

    private static string CountRowsSql(string tableName)
        => $"SELECT COUNT(*) FROM {tableName}";

    private void TryDropTemporaryTable(string tempTable)
    {
        try
        {
            ExecuteNonQuery(Dialect.DropTemporaryUsersTable(tempTable));
        }
        catch
        {
            // Ignore cleanup failures during benchmark teardown.
        }
    }

    private static bool IsMissingTemporaryTableException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("no such table", StringComparison.OrdinalIgnoreCase)
            || message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("invalid object name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase);
    }

    private static string InsertTemporaryRowSql(string tableName, int id, string name)
        => $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{name}')";

    private string ResolveSourceUsersTableName(string users, string uId)
        => $"{users}_{uId}".ToLowerInvariant();

    private string ResolveTemporaryUsersTableName(string rawTableName)
        => (Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
            && Connection is DbConnectionMockBase
            ? rawTableName
            : Dialect.TemporaryUsersTableName(rawTableName);
}
