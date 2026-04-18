namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Executes temporary-table workflows used by the fidelity tests.
/// PT: Executa fluxos de tabela temporaria usados pelos testes de fidelidade.
/// </summary>
public class TemporaryTableServiceOpsTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context)
{
    /// <summary>
    /// EN: Creates a temporary table from the source users table and returns the projected row identifiers.
    /// PT: Cria uma tabela temporaria a partir da tabela fonte de usuarios e retorna os identificadores projetados das linhas.
    /// </summary>
    public async Task<object?> RunCreateTemporaryTableAsSelectThenSelect(params object[] pars)
    {
        var isMockConnection = Repo.Cnn is DbConnectionMockBase;
        var tempTable = BuildTemporaryTableName(Context.UId, isMockConnection);
        var sessionTempTable = Repo.Dialect.Provider == ProviderId.Db2 && !isMockConnection
            ? $"SESSION.{tempTable}"
            : tempTable;
        if (Repo.Dialect.Provider == ProviderId.Npgsql)
        {
            await Repo.ExecuteNonQueryAsync($@"
CREATE TEMP TABLE {tempTable} AS
SELECT Id, Name FROM {Context.TempTbFullName} WHERE TenantId = 10");
        }
        else if (Repo.Dialect.Provider == ProviderId.Db2 && !isMockConnection)
        {
            await Repo.ExecuteNonQueryAsync($@"
DECLARE GLOBAL TEMPORARY TABLE SESSION.{tempTable} (
    Id INT,
    Name VARCHAR(100)
) ON COMMIT PRESERVE ROWS NOT LOGGED");
            await Repo.ExecuteNonQueryAsync($@"
INSERT INTO SESSION.{tempTable} (Id, Name)
SELECT Id, Name FROM {Context.TempTbFullName} WHERE TenantId = 10");
        }
        else if (Repo.Dialect.Provider == ProviderId.Db2)
        {
            if (Repo.Cnn is not DbConnectionMockBase mockConnection)
            {
                throw new InvalidOperationException("Db2 temporary table mock flow requires a mock connection.");
            }

            await TryDropTemporaryTable(tempTable);
            var tempTableMock = mockConnection.AddTemporaryTable(tempTable);
            tempTableMock.AddColumn("Id", DbType.Int32, false);
            tempTableMock.AddColumn("Name", DbType.String, false);
            await Repo.ExecuteNonQueryAsync($@"
INSERT INTO {tempTable} (Id, Name)
SELECT Id, Name FROM {Context.TempTbFullName} WHERE TenantId = 10");
        }
        else if ((Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure) && isMockConnection)
        {
            await Repo.ExecuteNonQueryAsync($@"
CREATE TEMPORARY TABLE {tempTable} AS
SELECT Id, Name FROM {Context.TempTbFullName} WHERE TenantId = 10");
        }
        else if (Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
        {
            await Repo.ExecuteNonQueryAsync($@"
SELECT Id, Name INTO {tempTable} FROM {Context.TempTbFullName} WHERE TenantId = 10");
        }
        else if (Repo.Dialect.Provider == ProviderId.Oracle
            && Repo.Cnn is not DbConnectionMockBase)
        {
            await Repo.ExecuteNonQueryAsync($@"
CREATE GLOBAL TEMPORARY TABLE {tempTable}
ON COMMIT PRESERVE ROWS
AS SELECT Id, Name
FROM {Context.TempTbFullName}
WHERE TenantId = 10");
        }
        else if (Repo.Dialect.Provider == ProviderId.Firebird
            && Repo.Cnn is not DbConnectionMockBase)
        {
            await Repo.ExecuteNonQueryAsync($@"
CREATE GLOBAL TEMPORARY TABLE {tempTable} (
    Id INTEGER,
    Name VARCHAR(100)
) ON COMMIT PRESERVE ROWS");
            await Repo.ExecuteNonQueryAsync($@"
INSERT INTO {tempTable} (Id, Name)
SELECT Id, Name FROM {Context.TempTbFullName} WHERE TenantId = 10");
        }
        else
        {
            var createSql = BuildCreateTemporaryTableSql(tempTable, Context.TempTbFullName);
            await Repo.ExecuteNonQueryAsync(createSql);
        }

        var ids = new List<int>();
        try
        {
            using var command = Repo.Cnn.CreateCommand();
            command.CommandText = $"SELECT Id FROM {sessionTempTable} ORDER BY Id";

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                ids.Add(reader.GetInt32(0));
            }

            if (ids.Count != 2 || ids[0] != 1 || ids[1] != 2)
            {
                throw new InvalidOperationException($"Unexpected temporary-table projected rows for {Repo.Dialect.DisplayName}: [{string.Join(",", ids)}].");
            }

            GC.KeepAlive(ids);
            return ids;
        }
        finally
        {
            await TryDropTemporaryTable(tempTable);
        }
    }

    private string BuildTemporaryTableName(string uId, bool isMockConnection)
    {
        if (Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
        {
            return isMockConnection ? $"tmp_users_{uId}" : $"#tmp_users_{uId}";
        }

        return Repo.Dialect.Provider == ProviderId.Npgsql
            ? $"pg_temp.tmp_users_{uId}"
            : $"tmp_users_{uId}";
    }

    private string BuildCreateTemporaryTableSql(
        string tempTable,
        string sourceUsersTable)
    {
        return $@"
CREATE TEMPORARY TABLE {tempTable} AS
SELECT Id, Name FROM {Context.TempTbFullName} WHERE TenantId = 10";
    }

    /// <summary>
    /// EN: Counts the rows available in the temporary-table scenario.
    /// PT: Conta as linhas disponiveis no cenario de tabela temporaria.
    /// </summary>
    public async Task<object?> RunTempTableCreateAndUse(params object[] pars)
    {
        var users = pars.Length > 0
            ? (string)pars[0]
            : Context.TempTbFullName;
        var sessionUsers = Repo.Dialect.Provider == ProviderId.Db2 && Repo.Cnn is not DbSqlLikeMem.DbConnectionMockBase
            ? $"SESSION.{users}"
            : users;
        await Repo.ExecuteNonQueryAsync(InsertTemporaryRowSql(sessionUsers, 1, "Alice"));
        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(CountRowsSql(sessionUsers)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected temporary-table rowcount for {Repo.Dialect.DisplayName}: {count}.");
        }

        GC.KeepAlive(count);
        return count;
    }

    /// <summary>
    /// EN: Opens a transaction, uses a savepoint, and rolls the work back for the temporary-table scenario.
    /// PT: Abre uma transacao, usa um savepoint e desfaz o trabalho para o cenario de tabela temporaria.
    /// </summary>
    public async Task RunTempTableRollback(params object[] pars)
    {
        var users = pars.Length > 0
            ? (string)pars[0]
            : Context.TempTbFullName;
        var sessionUsers = Repo.Dialect.Provider == ProviderId.Db2 && Repo.Cnn is not DbSqlLikeMem.DbConnectionMockBase
            ? $"SESSION.{users}"
            : users;
        using var tx = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(InsertTemporaryRowSql(sessionUsers, 1, "Alice"), tx);
        await Repo.ExecuteNonQueryAsync(InsertTemporaryRowSql(sessionUsers, 2, "Bob"), tx);
        tx.Rollback();

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(CountRowsSql(sessionUsers)), CultureInfo.InvariantCulture);
        if (count != 0)
        {
            throw new InvalidOperationException($"Unexpected temporary-table rollback rowcount for {Repo.Dialect.DisplayName}: {count}.");
        }
    }

    /// <summary>
    /// EN: Verifies that a temporary table is not visible or not populated from a secondary connection.
    /// PT: Verifica se uma tabela temporaria nao fica visivel ou nao fica populada a partir de uma conexao secundaria.
    /// </summary>
    /// <param name="pars">EN: The temporary users table name. PT: O nome da tabela temporaria de usuarios.</param>
    /// <returns>EN: Zero when the secondary connection cannot observe the inserted temporary-table row. PT: Zero quando a conexao secundaria nao consegue observar a linha inserida na tabela temporaria.</returns>
    public async Task<object?> RunTemporaryTableCrossConnectionIsolation(params object[] pars)
    {
        var users = pars.Length > 0
            ? (string)pars[0]
            : Context.TempTbFullName;
        var sessionUsers = Repo.Dialect.Provider == ProviderId.Db2 && Repo.Cnn is not DbConnectionMockBase
            ? $"SESSION.{users}"
            : users;
        await Repo.ExecuteNonQueryAsync(InsertTemporaryRowSql(sessionUsers, 1, "Alice"));

        using var repo = Repo.Clone();

        try
        {
            var count = Convert.ToInt32(await repo.ExecuteScalarAsync(CountRowsSql(sessionUsers)), CultureInfo.InvariantCulture);
            if (count != 0)
            {
                throw new InvalidOperationException($"Unexpected temporary-table isolation rowcount for {Repo.Dialect.DisplayName}: {count}.");
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
        finally
        {
            try
            {
                await Repo.ExecuteNonQueryAsync($"DELETE FROM {sessionUsers} WHERE Id = 1");
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
        }
    }

    private static string CountRowsSql(string tableName)
        => $"SELECT COUNT(*) FROM {tableName}";

    private async Task TryDropTemporaryTable(string tempTable)
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(tempTable));
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
}
