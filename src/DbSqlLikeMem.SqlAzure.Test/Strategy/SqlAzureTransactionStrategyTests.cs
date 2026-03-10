using DbSqlLikeMem.SqlServer;

namespace DbSqlLikeMem.SqlAzure.Test.Strategy;

/// <summary>
/// EN: Verifies transaction and session lifecycle behavior exposed by the SQL Azure strategy surface.
/// PT: Verifica o comportamento de transacao e ciclo de vida de sessao exposto pela superficie de estrategia do SQL Azure.
/// </summary>
public sealed class SqlAzureTransactionStrategyTests
{
    /// <summary>
    /// EN: Ensures SQL Azure transactions commit inserted rows to the shared table state.
    /// PT: Garante que transacoes do SQL Azure confirmem linhas inseridas no estado compartilhado da tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldCommit()
    {
        var db = new SqlAzureDbMock();
        var table = AddUsersTable(db);

        using var connection = CreateOpenConnection(db);
        using var transaction = Assert.IsType<SqlServerTransactionMock>(connection.BeginTransaction());
        using var command = new SqlAzureCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        command.ExecuteNonQuery();
        transaction.Commit();

        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
    }

    /// <summary>
    /// EN: Ensures SQL Azure transactions rollback inserted rows from the shared table state.
    /// PT: Garante que transacoes do SQL Azure revertam linhas inseridas do estado compartilhado da tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldRollback()
    {
        var db = new SqlAzureDbMock();
        var table = AddUsersTable(db);

        using var connection = CreateOpenConnection(db);
        using var transaction = Assert.IsType<SqlServerTransactionMock>(connection.BeginTransaction());
        using var command = new SqlAzureCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        command.ExecuteNonQuery();
        transaction.Rollback();

        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Ensures explicit transaction isolation is visible during execution and reset after commit.
    /// PT: Garante que o isolamento explicito da transacao fique visivel durante a execucao e seja resetado apos commit.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void BeginTransaction_WithIsolationLevel_ShouldExposeAndResetOnCommit()
    {
        using var connection = CreateOpenConnection();

        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        Assert.Equal(IsolationLevel.Serializable, connection.CurrentIsolationLevel);
        Assert.Equal(IsolationLevel.Serializable, transaction.IsolationLevel);

        connection.CommitTransaction();

        Assert.Equal(IsolationLevel.Unspecified, connection.CurrentIsolationLevel);
        Assert.False(connection.HasActiveTransaction);
    }

    /// <summary>
    /// EN: Ensures closing the connection clears session-scoped temporary state and invalidates old savepoints.
    /// PT: Garante que fechar a conexao limpe o estado temporario da sessao e invalide savepoints antigos.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Close_ShouldClearConnectionSessionState()
    {
        using var connection = CreateOpenConnection();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        temp.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        connection.CreateSavepoint("sp_close");

        connection.Close();

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.False(connection.HasActiveTransaction);
        Assert.Equal(IsolationLevel.Unspecified, connection.CurrentIsolationLevel);
        Assert.False(connection.TryGetTemporaryTable("temp_users", out var _));

        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_close"));
        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures closing one SQL Azure connection preserves permanent and global temporary shared state.
    /// PT: Garante que fechar uma conexao SQL Azure preserve o estado compartilhado permanente e temporario global.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Close_ShouldPreservePermanentAndGlobalSharedState()
    {
        var db = new SqlAzureDbMock();
        var users = AddUsersTable(db);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connA = CreateOpenConnection(db);
        using var connB = CreateOpenConnection(db);

        using (var createGlobalTemp = new SqlAzureCommandMock(connA)
        {
            CommandText = "CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users"
        })
        {
            createGlobalTemp.ExecuteNonQuery();
        }

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);
        tempA.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Tmp-A" });

        connA.Close();

        Assert.Single(users);
        Assert.Single(connB.GetTable("gtmp_users"));
        Assert.False(connA.TryGetTemporaryTable("temp_users", out var _));
    }

    /// <summary>
    /// EN: Ensures reopening SQL Azure starts a fresh session while keeping shared database state available.
    /// PT: Garante que reabrir o SQL Azure inicie uma sessao limpa mantendo o estado compartilhado do banco disponivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState()
    {
        var db = new SqlAzureDbMock();
        var users = AddUsersTable(db);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = CreateOpenConnection(db);

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        temp.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Tmp-A" });

        connection.Close();
        connection.Open();

        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.False(connection.HasActiveTransaction);
        Assert.False(connection.TryGetTemporaryTable("temp_users", out var _));
        Assert.Single(users);

        var newTemp = connection.AddTemporaryTable("temp_users");
        newTemp.AddColumn("id", DbType.Int32, false);
        newTemp.AddColumn("name", DbType.String, false);
        Assert.Empty(newTemp);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint restores the connection-scoped temporary-table snapshot in SQL Azure.
    /// PT: Garante que rollback para savepoint restaure o snapshot da tabela temporaria no escopo da conexao no SQL Azure.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot()
    {
        using var connection = CreateOpenConnection();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);

        using var transaction = connection.BeginTransaction();
        temp.Add(new Dictionary<int, object?>
        {
            [0] = 1,
            [1] = "Ana"
        });

        connection.CreateSavepoint("sp_temp");

        temp.Add(new Dictionary<int, object?>
        {
            [0] = 2,
            [1] = "Bob"
        });

        connection.RollbackTransaction("sp_temp");
        transaction.Commit();

        Assert.Single(temp);
        Assert.Equal(1, temp[0][0]);
        Assert.Equal("Ana", temp[0][1]);
    }

    /// <summary>
    /// EN: Ensures volatile reset clears active savepoints by dropping transaction state in SQL Azure.
    /// PT: Garante que o reset volatil limpe savepoints ativos ao descartar o estado transacional no SQL Azure.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetAllVolatileData_ShouldInvalidateSavepoints()
    {
        using var connection = CreateOpenConnection();

        using var transaction = connection.BeginTransaction();
        connection.CreateSavepoint("sp_reset");

        connection.ResetAllVolatileData();

        Assert.False(connection.HasActiveTransaction);
        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_reset"));
        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures unsupported release-savepoint API calls keep the standardized diagnostic on SQL Azure.
    /// PT: Garante que chamadas nao suportadas da API de release savepoint mantenham o diagnostico padronizado no SQL Azure.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_ShouldUseStandardizedNotSupportedMessage()
    {
        var db = new SqlAzureDbMock();
        AddUsersTable(db);

        using var connection = CreateOpenConnection(db);
        using var transaction = connection.BeginTransaction();
        connection.CreateSavepoint("sp1");

        var ex = Assert.Throws<NotSupportedException>(() => connection.ReleaseSavepoint("sp1"));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RELEASE SAVEPOINT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static SqlAzureConnectionMock CreateOpenConnection(SqlAzureDbMock? db = null)
    {
        var connection = new SqlAzureConnectionMock(db ?? new SqlAzureDbMock());
        connection.Open();
        return connection;
    }

    private static ITableMock AddUsersTable(SqlAzureDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        return table;
    }
}
