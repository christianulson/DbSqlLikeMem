namespace DbSqlLikeMem.MySql.Test.Strategy;
/// <summary>
/// EN: Covers transaction commit and rollback scenarios in the MySql mock.
/// PT-br: Cobre cenarios de commit e rollback de transacao no mock MySql.
/// </summary>
public sealed class MySqlTransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that committing a transaction persists the pending changes.
    /// PT-br: Verifica se o commit de uma transacao persiste as alteracoes pendentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldCommit()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        ArgumentNullExceptionCompatible.ThrowIfNull(transaction, nameof(transaction));
        using var command = new MySqlCommandMock(
            connection,
            (MySqlTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        command.ExecuteNonQuery();
        transaction.Commit();

        // Assert
        table.Should().ContainSingle();
        table[0][0].Should().Be(1);
        table[0][1].Should().Be("John Doe");
    }

    /// <summary>
    /// EN: Verifies that rolling back a transaction discards the pending changes.
    /// PT-br: Verifica se o rollback de uma transacao descarta as alteracoes pendentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldRollback()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        using var command = new MySqlCommandMock(
            connection,
            (MySqlTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        // Act
        command.ExecuteNonQuery();
        transaction.Rollback();

        // Assert
        table.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Ensures explicit transaction isolation level is exposed and reset after commit.
    /// PT-br: Garante que o nível de isolamento explícito seja exposto e resetado após commit.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void BeginTransaction_WithIsolationLevel_ShouldExposeAndResetOnCommit()
    {
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var tx = connection.BeginTransaction(IsolationLevel.Serializable);
        connection.CurrentIsolationLevel.Should().Be(IsolationLevel.Serializable);
        tx.IsolationLevel.Should().Be(IsolationLevel.Serializable);

        connection.CommitTransaction();

        connection.CurrentIsolationLevel.Should().Be(IsolationLevel.Unspecified);
        connection.HasActiveTransaction.Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures rollback resets current isolation level to unspecified.
    /// PT-br: Garante que rollback resete o nível de isolamento atual para não especificado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackTransaction_ShouldResetIsolationLevelToUnspecified()
    {
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var tx = connection.BeginTransaction(IsolationLevel.RepeatableRead);
        connection.CurrentIsolationLevel.Should().Be(IsolationLevel.RepeatableRead);

        connection.RollbackTransaction();

        connection.CurrentIsolationLevel.Should().Be(IsolationLevel.Unspecified);
        connection.HasActiveTransaction.Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures connection-scoped temporary tables stay isolated between different connections.
    /// PT-br: Garante que tabelas temporárias no escopo da conexão permaneçam isoladas entre conexões diferentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections()
    {
        var db = new MySqlDbMock();
        using var connA = new MySqlConnectionMock(db);
        using var connB = new MySqlConnectionMock(db);
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

        tempA.Should().ContainSingle();
        tempB.Should().ContainSingle();
        tempA[0][1].Should().Be("Ana");
        tempB[0][1].Should().Be("Bob");
    }

    /// <summary>
    /// EN: Ensures closing the connection clears session-scoped lifecycle state.
    /// PT-br: Garante que fechar a conexão limpe estado de ciclo de vida no escopo da sessão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Close_ShouldClearConnectionSessionState()
    {
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        temp.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var tx = connection.BeginTransaction(IsolationLevel.Serializable);
        connection.CreateSavepoint("sp_close");

        connection.Close();

        connection.State.Should().Be(ConnectionState.Closed);
        connection.HasActiveTransaction.Should().BeFalse();
        connection.CurrentIsolationLevel.Should().Be(IsolationLevel.Unspecified);
        connection.TryGetTemporaryTable("temp_users", out var _).Should().BeFalse();

        Action act = () => connection.RollbackTransaction("sp_close");
        act.Should().Throw<InvalidOperationException>()
            .Which.Message.Should().Contain("No active transaction");
    }

    /// <summary>
    /// EN: Ensures closing a connection clears only session-scoped state and preserves shared database state.
    /// PT-br: Garante que fechar a conexão limpe apenas estado de sessão e preserve o estado compartilhado do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Close_ShouldPreservePermanentAndGlobalSharedState()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connA = new MySqlConnectionMock(db);
        using var connB = new MySqlConnectionMock(db);
        connA.Open();
        connB.Open();

        using var createGlobalTemp = new MySqlCommandMock(connA)
        {
            CommandText = "CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users"
        };
        createGlobalTemp.ExecuteNonQuery();

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);
        tempA.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Tmp-A" });

        connA.Close();

        users.Should().ContainSingle();
        Action act = () => connB.GetTable("gtmp_users");
        act.Should().Throw<InvalidOperationException>();
        connA.TryGetTemporaryTable("temp_users", out var _).Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures reopening a closed connection starts a fresh session while preserving shared database state.
    /// PT-br: Garante que reabrir conexão fechada inicie sessão limpa preservando o estado compartilhado do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        temp.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Tmp-A" });

        connection.Close();
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);
        connection.HasActiveTransaction.Should().BeFalse();
        connection.TryGetTemporaryTable("temp_users", out var _).Should().BeFalse();
        users.Should().ContainSingle();

        var tempNew = connection.AddTemporaryTable("temp_users");
        tempNew.AddColumn("id", DbType.Int32, false);
        tempNew.AddColumn("name", DbType.String, false);
        tempNew.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Ensures savepoint operations on the transaction wrapper delegate to the connection lifecycle correctly.
    /// PT-br: Garante que operações de savepoint no wrapper de transação deleguem corretamente ao ciclo de vida da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionSavepointMethods_ShouldDelegateToConnectionState()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        using var insert = new MySqlCommandMock(connection, (MySqlTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')"
        };
        insert.ExecuteNonQuery();

        ((MySqlTransactionMock)transaction).Save("sp_users");

        using var update = new MySqlCommandMock(connection, (MySqlTransactionMock)transaction)
        {
            CommandText = "UPDATE users SET name = 'Bia' WHERE id = 1"
        };
        update.ExecuteNonQuery();

        ((MySqlTransactionMock)transaction).Rollback("sp_users");
        users.Single()[1].Should().Be("Ana");

        ((MySqlTransactionMock)transaction).Save("sp_release");
        ((MySqlTransactionMock)transaction).Release("sp_release");
        transaction.Commit();

        connection.HasActiveTransaction.Should().BeFalse();
        connection.CurrentIsolationLevel.Should().Be(IsolationLevel.Unspecified);
    }

    /// <summary>
    /// EN: Ensures releasing a savepoint without an active transaction keeps the actionable runtime message in MySQL.
    /// PT-br: Garante que liberar um savepoint sem uma transação ativa mantenha a mensagem acionável em runtime no MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new MySqlDbMock();
        db.AddTable("users");

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures creating a savepoint without an active transaction keeps the actionable runtime message in MySQL.
    /// PT-br: Garante que criar um savepoint sem uma transacao ativa mantenha a mensagem acionavel em runtime no MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new MySqlDbMock();
        db.AddTable("users");

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.CreateSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint without an active transaction keeps the actionable runtime message in MySQL.
    /// PT-br: Garante que rollback para savepoint sem uma transacao ativa mantenha a mensagem acionavel em runtime no MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new MySqlDbMock();
        db.AddTable("users");

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures releasing an unknown savepoint keeps the actionable runtime message in MySQL.
    /// PT-br: Garante que liberar um savepoint desconhecido mantenha a mensagem acionavel em runtime no MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseUnknownSavepoint_ShouldProvideActionableMessage()
    {
        var db = new MySqlDbMock();
        db.AddTable("users");

        using var connection = new MySqlConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        connection.CreateSavepoint("sp_known");

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_missing"));

        Assert.Contains("Savepoint", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("was not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures creating a savepoint with an empty name keeps the existing parameter validation message.
    /// PT-br: Garante que criar um savepoint com nome vazio mantenha a mensagem de validacao de parametro existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_WithBlankName_ShouldProvideParameterValidationMessage()
    {
        var db = new MySqlDbMock();
        db.AddTable("users");

        using var connection = new MySqlConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<ArgumentException>(() => connection.CreateSavepoint(" "));

        Assert.Contains("savepointName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures releasing a savepoint with an empty name keeps the existing parameter validation message.
    /// PT-br: Garante que liberar um savepoint com nome vazio mantenha a mensagem de validacao de parametro existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_WithBlankName_ShouldProvideParameterValidationMessage()
    {
        var db = new MySqlDbMock();
        db.AddTable("users");

        using var connection = new MySqlConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<ArgumentException>(() => connection.ReleaseSavepoint(" "));

        Assert.Contains("savepointName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rollback restores multiple tables correctly when the database runs in thread-safe mode.
    /// PT-br: Garante que rollback restaure varias tabelas corretamente quando o banco executa em modo thread-safe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionRollback_ShouldRestoreMultipleTables_WhenThreadSafe()
    {
        var db = new MySqlDbMock
        {
            ThreadSafe = true
        };

        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddPrimaryKeyIndexes("id");

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("user_id", DbType.Int32, false);
        orders.AddPrimaryKeyIndexes("id");

        using var connection = new MySqlConnectionMock(db);
        connection.Open();
        using var transaction = Assert.IsType<MySqlTransactionMock>(connection.BeginTransaction());
        using var insertUser = new MySqlCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')"
        };
        using var insertOrder = new MySqlCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO orders (id, user_id) VALUES (10, 1)"
        };

        insertUser.ExecuteNonQuery();
        insertOrder.ExecuteNonQuery();

        transaction.Rollback();

        users.Should().BeEmpty();
        orders.Should().BeEmpty();
        connection.HasActiveTransaction.Should().BeFalse();
        connection.CurrentIsolationLevel.Should().Be(IsolationLevel.Unspecified);
    }

    /// <summary>
    /// EN: Ensures transaction rollback restores connection temporary-table state.
    /// PT-br: Garante que rollback de transação restaure o estado de tabela temporária da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionRollback_ShouldRestoreConnectionTemporaryTable()
    {
        // Arrange
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
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
        temp.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint restores connection temporary-table snapshot.
    /// PT-br: Garante que rollback para savepoint restaure snapshot de tabela temporária da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot()
    {
        // Arrange
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
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
        temp.Should().ContainSingle();
        temp[0][0].Should().Be(1);
        temp[0][1].Should().Be("Ana");
    }

    /// <summary>
    /// EN: Ensures nested savepoints restore the transaction snapshot from the selected outer point.
    /// PT-br: Garante que savepoints aninhados restaurem o snapshot da transacao a partir do ponto externo selecionado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void NestedSavepoints_ShouldRollbackToSelectedOuterSnapshot()
    {
        // Arrange
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
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

        connection.CreateSavepoint("sp_outer");

        temp.Add(new Dictionary<int, object?>
        {
            [0] = 2,
            [1] = "Bob"
        });

        connection.CreateSavepoint("sp_inner");

        temp.Add(new Dictionary<int, object?>
        {
            [0] = 3,
            [1] = "Cara"
        });

        // Act
        connection.RollbackTransaction("sp_outer");
        transaction.Commit();

        // Assert
        temp.Should().ContainSingle();
        temp[0][0].Should().Be(1);
        temp[0][1].Should().Be("Ana");
    }

    /// <summary>
    /// EN: Ensures full volatile reset clears permanent and temporary data and resets identity counters.
    /// PT-br: Garante que reset volátil completo limpe dados permanentes/temporários e resete contadores de identidade.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetAllVolatileData_ShouldClearRowsAndResetIdentity()
    {
        // Arrange
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: true);
        users.AddColumn("name", DbType.String, false);

        using var connection = new MySqlConnectionMock(db);
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
        connection.HasActiveTransaction.Should().BeFalse();
        users.Should().BeEmpty();
        users.NextIdentity.Should().Be(1);
        temp.Should().BeEmpty();
        temp.NextIdentity.Should().Be(1);
        connection.TryGetTemporaryTable("temp_users", out var _).Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures full volatile reset also clears rows from global temporary tables while preserving definitions.
    /// PT-br: Garante que reset volátil completo também limpe linhas de temporárias globais preservando definições.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows()
    {
        // Arrange
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var createGlobalTemp = new MySqlCommandMock(connection)
        {
            CommandText = "CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users"
        };
        createGlobalTemp.ExecuteNonQuery();

        var globalTemp = connection.GetTable("gtmp_users");
        globalTemp.Should().ContainSingle();

        // Act
        connection.ResetAllVolatileData();

        // Assert
        globalTemp.Should().BeEmpty();
        globalTemp.Columns.Count.Should().Be(2);
    }

    /// <summary>
    /// EN: Ensures full volatile reset invalidates active savepoints by clearing transaction state.
    /// PT-br: Garante que reset volátil completo invalide savepoints ativos ao limpar o estado transacional.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetAllVolatileData_ShouldInvalidateSavepoints()
    {
        // Arrange
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        connection.CreateSavepoint("sp_reset");

        // Act
        connection.ResetAllVolatileData();

        // Assert
        connection.HasActiveTransaction.Should().BeFalse();
        Action act = () => connection.RollbackTransaction("sp_reset");
        act.Should().Throw<InvalidOperationException>()
            .Which.Message.Should().Contain("No active transaction");
    }

    /// <summary>
    /// EN: Ensures database-level volatile reset clears rows and identity while keeping table definitions.
    /// PT-br: Garante que reset volátil no banco limpe linhas/identidade mantendo definições de tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetVolatileData_OnDb_ShouldKeepTableDefinitions()
    {
        // Arrange
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: true);
        users.AddColumn("name", DbType.String, false);

        users.Add(new Dictionary<int, object?> { [1] = "Ana" });
        users.Add(new Dictionary<int, object?> { [1] = "Bia" });

        // Act
        db.ResetVolatileData();

        // Assert
        db.ContainsTable("users").Should().BeTrue();
        users.Columns.Count.Should().Be(2);
        users.Should().BeEmpty();
        users.NextIdentity.Should().Be(1);
    }

    /// <summary>
    /// EN: Ensures database-level volatile reset does not affect connection-scoped temporary tables.
    /// PT-br: Garante que reset volátil no banco não afete tabelas temporárias no escopo da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables()
    {
        // Arrange
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        temp.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Tmp-A" });

        // Act
        db.ResetVolatileData(includeGlobalTemporaryTables: true);

        // Assert
        connection.TryGetTemporaryTable("temp_users", out var tempAfter).Should().BeTrue();
        tempAfter.Should().ContainSingle();
        tempAfter![0][1].Should().Be("Tmp-A");
    }

    /// <summary>
    /// EN: Ensures database volatile reset can preserve or clear global temporary table rows based on the input flag.
    /// PT-br: Garante que o reset volátil do banco preserve ou limpe linhas de tabela temporária global conforme o parâmetro.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag()
    {
        // Arrange
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var createGlobalTemp = new MySqlCommandMock(connection)
        {
            CommandText = "CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users"
        };
        createGlobalTemp.ExecuteNonQuery();

        var globalTemp = connection.GetTable("gtmp_users");
        globalTemp.Should().ContainSingle();

        // Act 1: preserve global temporary rows
        db.ResetVolatileData(includeGlobalTemporaryTables: false);

        // Assert 1
        users.Should().BeEmpty();
        globalTemp.Should().ContainSingle();

        // Act 2: clear global temporary rows
        db.ResetVolatileData(includeGlobalTemporaryTables: true);

        // Assert 2
        globalTemp.Should().ContainSingle();
        globalTemp.Columns.Count.Should().Be(2);
    }
}
