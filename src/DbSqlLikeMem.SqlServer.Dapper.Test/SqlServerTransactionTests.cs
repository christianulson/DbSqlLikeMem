namespace DbSqlLikeMem.SqlServer.Dapper.Test;
/// <summary>
/// EN: Defines the class SqlServerTransactionTests.
/// PT: Define a classe SqlServerTransactionTests.
/// </summary>
public sealed class SqlServerTransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class User
    {
        /// <summary>
        /// EN: Gets or sets Id.
        /// PT: Obtém ou define Id.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// EN: Gets or sets Name.
        /// PT: Obtém ou define Name.
        /// </summary>
        public required string Name { get; set; }
        /// <summary>
        /// EN: Gets or sets Email.
        /// PT: Obtém ou define Email.
        /// </summary>
        public string? Email { get; set; }
    }

    /// <summary>
    /// EN: Tests TransactionCommitShouldPersistData behavior.
    /// PT: Testa o comportamento de TransactionCommitShouldPersistData.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void TransactionCommitShouldPersistData()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var user = new User { Id = 1, Name = "John Doe", Email = "john.doe@example.com" };

        // Act
        connection.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)", user, transaction);
        connection.CommitTransaction();

        // Assert
        Assert.Single(table);
        var insertedRow = table[0];
        Assert.Equal(user.Id, insertedRow[0]);
        Assert.Equal(user.Name, insertedRow[1]);
        Assert.Equal(user.Email, insertedRow[2]);
    }

    /// <summary>
    /// EN: Tests TransactionRollbackShouldNotPersistData behavior.
    /// PT: Testa o comportamento de TransactionRollbackShouldNotPersistData.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void TransactionRollbackShouldNotPersistData()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var user = new User { Id = 1, Name = "John Doe", Email = "john.doe@example.com" };

        // Act
        connection.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)", user, transaction);
        connection.RollbackTransaction();

        // Assert
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Ensures transaction rollback restores connection temporary-table state in Dapper flow.
    /// PT: Garante que rollback restaure estado de tabela temporária de conexão no fluxo Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void TransactionRollback_ShouldRestoreConnectionTemporaryTable_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("Id", DbType.Int32, false);
        temp.AddColumn("Name", DbType.String, false);

        using var transaction = connection.BeginTransaction();
        connection.Execute(
            "INSERT INTO temp_users (Id, Name) VALUES (@Id, @Name)",
            new { Id = 1, Name = "Ana" },
            transaction);

        // Act
        connection.RollbackTransaction();

        // Assert
        Assert.Empty(temp);
    }

    /// <summary>
    /// EN: Ensures rollback-to-savepoint restores temporary-table snapshot in Dapper flow.
    /// PT: Garante que rollback para savepoint restaure snapshot de tabela temporária no fluxo Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("Id", DbType.Int32, false);
        temp.AddColumn("Name", DbType.String, false);

        using var transaction = connection.BeginTransaction();
        connection.Execute(
            "INSERT INTO temp_users (Id, Name) VALUES (@Id, @Name)",
            new { Id = 1, Name = "Ana" },
            transaction);

        connection.CreateSavepoint("sp_temp");

        connection.Execute(
            "INSERT INTO temp_users (Id, Name) VALUES (@Id, @Name)",
            new { Id = 2, Name = "Bob" },
            transaction);

        // Act
        connection.RollbackTransaction("sp_temp");
        connection.CommitTransaction();

        // Assert
        Assert.Single(temp);
        Assert.Equal(1, temp[0][0]);
        Assert.Equal("Ana", temp[0][1]);
    }

    /// <summary>
    /// EN: Ensures full volatile reset clears permanent and temporary data and resets identities in Dapper context.
    /// PT: Garante que reset volátil completo limpe dados permanentes/temporários e resete identidades no contexto Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: true);
        users.AddColumn("name", DbType.String, false);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false, identity: true);
        temp.AddColumn("name", DbType.String, false);

        users.Add(new Dictionary<int, object?> { [1] = "Ana" });
        users.Add(new Dictionary<int, object?> { [1] = "Bia" });
        temp.Add(new Dictionary<int, object?> { [1] = "Tmp-A" });
        temp.Add(new Dictionary<int, object?> { [1] = "Tmp-B" });

        using var transaction = connection.BeginTransaction();

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
    /// EN: Ensures database volatile reset preserves or clears global temporary rows by flag in Dapper context.
    /// PT: Garante que reset volátil preserve ou limpe linhas de temporária global conforme flag no contexto Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        connection.Execute("CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users");

        var globalTemp = connection.GetTable("gtmp_users");
        Assert.Single(globalTemp);

        // Act 1
        db.ResetVolatileData(includeGlobalTemporaryTables: false);

        // Assert 1
        Assert.Empty(users);
        Assert.Single(globalTemp);

        // Act 2
        db.ResetVolatileData(includeGlobalTemporaryTables: true);

        // Assert 2
        Assert.Empty(globalTemp);
        Assert.Equal(2, globalTemp.Columns.Count);
    }

    /// <summary>
    /// EN: Ensures database-level volatile reset clears rows and identity while keeping table definitions in Dapper context.
    /// PT: Garante que reset volátil no banco limpe linhas/identidade mantendo definições de tabela no contexto Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
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
    /// EN: Ensures database-level volatile reset does not affect connection-scoped temporary tables in Dapper context.
    /// PT: Garante que reset volátil no banco não afete tabelas temporárias de conexão no contexto Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        connection.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 1, name = "Tmp-A" });

        // Act
        db.ResetVolatileData(includeGlobalTemporaryTables: true);

        // Assert
        Assert.True(connection.TryGetTemporaryTable("temp_users", out var tempAfter));
        Assert.Single(tempAfter!);
        Assert.Equal("Tmp-A", tempAfter[0][1]);
    }

    /// <summary>
    /// EN: Ensures full volatile reset also clears rows from global temporary tables in Dapper context.
    /// PT: Garante que reset volátil completo também limpe linhas de temporárias globais no contexto Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        connection.Execute("CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users");

        var globalTemp = connection.GetTable("gtmp_users");
        Assert.Single(globalTemp);

        // Act
        connection.ResetAllVolatileData();

        // Assert
        Assert.Empty(globalTemp);
        Assert.Equal(2, globalTemp.Columns.Count);
    }

    /// <summary>
    /// EN: Ensures full volatile reset invalidates active savepoints in Dapper context.
    /// PT: Garante que reset volátil completo invalide savepoints ativos no contexto Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetAllVolatileData_ShouldInvalidateSavepoints_Dapper()
    {
        // Arrange
        var db = new SqlServerDbMock();
        using var connection = new SqlServerConnectionMock(db);
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
    /// EN: Ensures connection temporary tables remain isolated between different connections in Dapper flow.
    /// PT: Garante que tabelas temporárias de conexão permaneçam isoladas entre conexões diferentes no fluxo Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Dapper()
    {
        var db = new SqlServerDbMock();
        using var connA = new SqlServerConnectionMock(db);
        using var connB = new SqlServerConnectionMock(db);
        connA.Open();
        connB.Open();

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);

        var tempB = connB.AddTemporaryTable("temp_users");
        tempB.AddColumn("id", DbType.Int32, false);
        tempB.AddColumn("name", DbType.String, false);

        connA.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 1, name = "Ana" });
        connB.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 2, name = "Bob" });

        Assert.Single(tempA);
        Assert.Single(tempB);
        Assert.Equal("Ana", tempA[0][1]);
        Assert.Equal("Bob", tempB[0][1]);
    }

    /// <summary>
    /// EN: Ensures closing the connection clears session-scoped lifecycle state in Dapper flow.
    /// PT: Garante que fechar a conexão limpe estado de sessão no fluxo Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void Close_ShouldClearConnectionSessionState_Dapper()
    {
        var db = new SqlServerDbMock();
        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        connection.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 1, name = "Ana" });

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
    /// EN: Ensures closing one connection preserves shared permanent/global data for another connection in Dapper flow.
    /// PT: Garante que fechar uma conexão preserve dados compartilhados permanentes/globais para outra conexão no fluxo Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void Close_ShouldPreservePermanentAndGlobalSharedState_Dapper()
    {
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connA = new SqlServerConnectionMock(db);
        using var connB = new SqlServerConnectionMock(db);
        connA.Open();
        connB.Open();

        connA.Execute("CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users");

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);
        connA.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 2, name = "Tmp-A" });

        connA.Close();

        Assert.Single(users);
        var globalTempFromConnB = connB.GetTable("gtmp_users");
        Assert.Single(globalTempFromConnB);
        Assert.False(connA.TryGetTemporaryTable("temp_users", out var _));
    }

    /// <summary>
    /// EN: Ensures reopening a closed connection starts a fresh reusable session in Dapper flow.
    /// PT: Garante que reabrir conexão fechada inicie sessão limpa e reutilizável no fluxo Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Dapper()
    {
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        connection.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 2, name = "Tmp-A" });

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
}
