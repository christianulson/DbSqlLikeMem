namespace DbSqlLikeMem.Oracle.Test.Strategy;
/// <summary>
/// EN: Defines the class OracleTransactionTests.
/// PT: Define a classe OracleTransactionTests.
/// </summary>
public sealed class OracleTransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests TransactionShouldCommit behavior.
    /// PT: Testa o comportamento de TransactionShouldCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldCommit()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new OracleConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        ArgumentNullExceptionCompatible.ThrowIfNull(transaction, nameof(transaction));
        using var command = new OracleCommandMock(
            connection,
            (OracleTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        command.ExecuteNonQuery();
        transaction.Commit();

        // Assert
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
    }

    /// <summary>
    /// EN: Tests TransactionShouldRollback behavior.
    /// PT: Testa o comportamento de TransactionShouldRollback.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldRollback()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new OracleConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        using var command = new OracleCommandMock(
            connection,
            (OracleTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        command.ExecuteNonQuery();
        transaction.Rollback();

        // Assert
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Ensures explicit transaction isolation level is exposed and reset after commit.
    /// PT: Garante que o nível de isolamento explícito seja exposto e resetado após commit.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void BeginTransaction_WithIsolationLevel_ShouldExposeAndResetOnCommit()
    {
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using var tx = connection.BeginTransaction(IsolationLevel.Serializable);
        Assert.Equal(IsolationLevel.Serializable, connection.CurrentIsolationLevel);
        Assert.Equal(IsolationLevel.Serializable, tx.IsolationLevel);

        connection.CommitTransaction();

        Assert.Equal(IsolationLevel.Unspecified, connection.CurrentIsolationLevel);
        Assert.False(connection.HasActiveTransaction);
    }

    /// <summary>
    /// EN: Ensures rollback resets current isolation level to unspecified.
    /// PT: Garante que rollback resete o nível de isolamento atual para não especificado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackTransaction_ShouldResetIsolationLevelToUnspecified()
    {
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using var tx = connection.BeginTransaction(IsolationLevel.RepeatableRead);
        Assert.Equal(IsolationLevel.RepeatableRead, connection.CurrentIsolationLevel);

        connection.RollbackTransaction();

        Assert.Equal(IsolationLevel.Unspecified, connection.CurrentIsolationLevel);
        Assert.False(connection.HasActiveTransaction);
    }

    /// <summary>
    /// EN: Ensures connection-scoped temporary tables stay isolated between different connections.
    /// PT: Garante que tabelas temporárias no escopo da conexão permaneçam isoladas entre conexões diferentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections()
    {
        var db = new OracleDbMock();
        using var connA = new OracleConnectionMock(db);
        using var connB = new OracleConnectionMock(db);
        connA.Open();
        connB.Open();

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);
        tempA.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        var tempB = connB.AddTemporaryTable("temp_users");
        tempB.AddColumn("id", DbType.Int32, false);
        tempB.AddColumn("name", DbType.String, false);
        tempB.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob" });

        Assert.Single(tempA);
        Assert.Single(tempB);
        Assert.Equal("Ana", tempA[0][1]);
        Assert.Equal("Bob", tempB[0][1]);
    }

    /// <summary>
    /// EN: Ensures closing the connection clears session-scoped lifecycle state.
    /// PT: Garante que fechar a conexão limpe estado de ciclo de vida no escopo da sessão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Close_ShouldClearConnectionSessionState()
    {
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        temp.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var tx = connection.BeginTransaction(IsolationLevel.Serializable);
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
    /// EN: Ensures closing a connection clears only session-scoped state and preserves shared database state.
    /// PT: Garante que fechar a conexão limpe apenas estado de sessão e preserve o estado compartilhado do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Close_ShouldPreservePermanentAndGlobalSharedState()
    {
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connA = new OracleConnectionMock(db);
        using var connB = new OracleConnectionMock(db);
        connA.Open();
        connB.Open();

        using var createGlobalTemp = new OracleCommandMock(connA)
        {
            CommandText = "CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users"
        };
        createGlobalTemp.ExecuteNonQuery();

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);
        tempA.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Tmp-A" });

        connA.Close();

        Assert.Single(users);
        var globalTempFromConnB = connB.GetTable("gtmp_users");
        Assert.Single(globalTempFromConnB);
        Assert.False(connA.TryGetTemporaryTable("temp_users", out var _));
    }

    /// <summary>
    /// EN: Ensures reopening a closed connection starts a fresh session while preserving shared database state.
    /// PT: Garante que reabrir conexão fechada inicie sessão limpa preservando o estado compartilhado do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState()
    {
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new OracleConnectionMock(db);
        connection.Open();

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

        var tempNew = connection.AddTemporaryTable("temp_users");
        tempNew.AddColumn("id", DbType.Int32, false);
        tempNew.AddColumn("name", DbType.String, false);
        Assert.Empty(tempNew);
    }

    /// <summary>
    /// EN: Ensures transaction rollback restores connection temporary-table state.
    /// PT: Garante que rollback de transação restaure o estado de tabela temporária da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionRollback_ShouldRestoreConnectionTemporaryTable()
    {
        // Arrange
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);

        using var transaction = connection.BeginTransaction();
        temp.Add(new Dictionary<int, object?>
        {
            [0] = 1,
            [1] = "Ana"
        });

        // Act
        transaction.Rollback();

        // Assert
        Assert.Empty(temp);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint restores connection temporary-table snapshot.
    /// PT: Garante que rollback para savepoint restaure snapshot de tabela temporária da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot()
    {
        // Arrange
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);
        connection.Open();

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

        // Act
        connection.RollbackTransaction("sp_temp");
        transaction.Commit();

        // Assert
        Assert.Single(temp);
        Assert.Equal(1, temp[0][0]);
        Assert.Equal("Ana", temp[0][1]);
    }

    /// <summary>
    /// EN: Ensures full volatile reset clears permanent and temporary data and resets identity counters.
    /// PT: Garante que reset volátil completo limpe dados permanentes/temporários e resete contadores de identidade.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetAllVolatileData_ShouldClearRowsAndResetIdentity()
    {
        // Arrange
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: true);
        users.AddColumn("name", DbType.String, false);

        using var connection = new OracleConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false, identity: true);
        temp.AddColumn("name", DbType.String, false);

        users.Add(new Dictionary<int, object?> { [1] = "Ana" });
        users.Add(new Dictionary<int, object?> { [1] = "Bia" });
        temp.Add(new Dictionary<int, object?> { [1] = "Tmp-A" });
        temp.Add(new Dictionary<int, object?> { [1] = "Tmp-B" });

        using var tx = connection.BeginTransaction();

        // Act
        connection.ResetAllVolatileData();

        // Assert
        Assert.False(connection.HasActiveTransaction);
        Assert.Empty(users);
        Assert.Equal(1, users.NextIdentity);
        Assert.Empty(temp);
        Assert.Equal(1, temp.NextIdentity);
        Assert.False(connection.TryGetTemporaryTable("temp_users", out var _));
    }

    /// <summary>
    /// EN: Ensures full volatile reset also clears rows from global temporary tables while preserving definitions.
    /// PT: Garante que reset volátil completo também limpe linhas de temporárias globais preservando definições.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows()
    {
        // Arrange
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using var createGlobalTemp = new OracleCommandMock(connection)
        {
            CommandText = "CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users"
        };
        createGlobalTemp.ExecuteNonQuery();

        var globalTemp = connection.GetTable("gtmp_users");
        Assert.Single(globalTemp);

        // Act
        connection.ResetAllVolatileData();

        // Assert
        Assert.Empty(globalTemp);
        Assert.Equal(2, globalTemp.Columns.Count);
    }

    /// <summary>
    /// EN: Ensures full volatile reset invalidates active savepoints by clearing transaction state.
    /// PT: Garante que reset volátil completo invalide savepoints ativos ao limpar o estado transacional.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetAllVolatileData_ShouldInvalidateSavepoints()
    {
        // Arrange
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        connection.CreateSavepoint("sp_reset");

        // Act
        connection.ResetAllVolatileData();

        // Assert
        Assert.False(connection.HasActiveTransaction);
        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_reset"));
        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures database-level volatile reset clears rows and identity while keeping table definitions.
    /// PT: Garante que reset volátil no banco limpe linhas/identidade mantendo definições de tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetVolatileData_OnDb_ShouldKeepTableDefinitions()
    {
        // Arrange
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: true);
        users.AddColumn("name", DbType.String, false);

        users.Add(new Dictionary<int, object?> { [1] = "Ana" });
        users.Add(new Dictionary<int, object?> { [1] = "Bia" });

        // Act
        db.ResetVolatileData();

        // Assert
        Assert.True(db.ContainsTable("users"));
        Assert.Equal(2, users.Columns.Count);
        Assert.Empty(users);
        Assert.Equal(1, users.NextIdentity);
    }

    /// <summary>
    /// EN: Ensures database-level volatile reset does not affect connection-scoped temporary tables.
    /// PT: Garante que reset volátil no banco não afete tabelas temporárias no escopo da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables()
    {
        // Arrange
        var db = new OracleDbMock();
        using var connection = new OracleConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        temp.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Tmp-A" });

        // Act
        db.ResetVolatileData(includeGlobalTemporaryTables: true);

        // Assert
        Assert.True(connection.TryGetTemporaryTable("temp_users", out var tempAfter));
        Assert.Single(tempAfter!);
        Assert.Equal("Tmp-A", tempAfter![0][1]);
    }

    /// <summary>
    /// EN: Ensures database volatile reset can preserve or clear global temporary table rows based on the input flag.
    /// PT: Garante que o reset volátil do banco preserve ou limpe linhas de tabela temporária global conforme o parâmetro.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag()
    {
        // Arrange
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using var createGlobalTemp = new OracleCommandMock(connection)
        {
            CommandText = "CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users"
        };
        createGlobalTemp.ExecuteNonQuery();

        var globalTemp = connection.GetTable("gtmp_users");
        Assert.Single(globalTemp);

        // Act 1: preserve global temporary rows
        db.ResetVolatileData(includeGlobalTemporaryTables: false);

        // Assert 1
        Assert.Empty(users);
        Assert.Single(globalTemp);

        // Act 2: clear global temporary rows
        db.ResetVolatileData(includeGlobalTemporaryTables: true);

        // Assert 2
        Assert.Empty(globalTemp);
        Assert.Equal(2, globalTemp.Columns.Count);
    }
}
