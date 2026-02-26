namespace DbSqlLikeMem.SqlServer.Test.Strategy;
/// <summary>
/// EN: Defines the class SqlServerTransactionTests.
/// PT: Define a classe SqlServerTransactionTests.
/// </summary>
public sealed class SqlServerTransactionTests(
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
        var db = new SqlServerDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        ArgumentNullExceptionCompatible.ThrowIfNull(transaction, nameof(transaction));
        using var command = new SqlServerCommandMock(
            connection,
            (SqlServerTransactionMock)transaction)
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
        var db = new SqlServerDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();
        var transaction = connection.BeginTransaction();
        using var command = new SqlServerCommandMock(
            connection,
            (SqlServerTransactionMock)transaction)
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
    /// EN: Ensures unsupported savepoint operations use standardized runtime not-supported diagnostics.
    /// PT: Garante que operações de savepoint não suportadas usem diagnóstico padronizado de não suporte em runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_ShouldUseStandardizedNotSupportedMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        connection.CreateSavepoint("sp1");

        var ex = Assert.Throws<NotSupportedException>(() => connection.ReleaseSavepoint("sp1"));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RELEASE SAVEPOINT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("sqlserver", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint restores the table snapshot within an active transaction.
    /// PT: Garante que rollback para savepoint restaure o snapshot da tabela dentro de uma transação ativa.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackToSavepoint_ShouldRestoreSnapshot()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        using var cmd = new SqlServerCommandMock(connection, (SqlServerTransactionMock)transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')"
        };
        cmd.ExecuteNonQuery();

        connection.CreateSavepoint("sp_users");

        cmd.CommandText = "INSERT INTO users (id, name) VALUES (2, 'Bob')";
        cmd.ExecuteNonQuery();

        connection.RollbackTransaction("sp_users");
        transaction.Commit();

        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("Ana", table[0][1]);
    }

    /// <summary>
    /// EN: Ensures rollback to an unknown savepoint keeps the existing actionable runtime message.
    /// PT: Garante que rollback para um savepoint desconhecido mantenha a mensagem de runtime acionável existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackToUnknownSavepoint_ShouldProvideActionableMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_missing"));

        Assert.Contains("Savepoint", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("was not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures savepoint creation uses standardized not-supported diagnostics when provider disables savepoints.
    /// PT: Garante que criação de savepoint use diagnóstico padronizado de não suportado quando o provedor desabilita savepoints.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_UnsupportedProvider_ShouldUseStandardizedNotSupportedMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new UnsupportedSavepointSqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<NotSupportedException>(() => connection.CreateSavepoint("sp_unsupported"));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SAVEPOINT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("sqlserver", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rollback-to-savepoint uses standardized not-supported diagnostics when provider disables savepoints.
    /// PT: Garante que rollback para savepoint use diagnóstico padronizado de não suportado quando o provedor desabilita savepoints.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackSavepoint_UnsupportedProvider_ShouldUseStandardizedNotSupportedMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new UnsupportedSavepointSqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<NotSupportedException>(() => connection.RollbackTransaction("sp_unsupported"));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ROLLBACK TO SAVEPOINT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("sqlserver", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures releasing an unknown savepoint keeps the existing actionable runtime message when release is enabled.
    /// PT: Garante que liberar um savepoint desconhecido mantenha a mensagem de runtime acionável existente quando release está habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseUnknownSavepoint_WhenReleaseIsEnabled_ShouldProvideActionableMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new ReleaseEnabledSqlServerConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        connection.CreateSavepoint("sp_known");

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_missing"));

        Assert.Contains("Savepoint", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("was not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures creating a savepoint without an active transaction keeps the existing actionable message.
    /// PT: Garante que criar savepoint sem transação ativa mantenha a mensagem acionável existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.CreateSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint without an active transaction keeps the existing actionable message.
    /// PT: Garante que rollback para savepoint sem transação ativa mantenha a mensagem acionável existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures releasing savepoint without an active transaction keeps the existing actionable message.
    /// PT: Garante que liberar savepoint sem transação ativa mantenha a mensagem acionável existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new SqlServerDbMock();
        db.AddTable("users");

        using var connection = new ReleaseEnabledSqlServerConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class UnsupportedSavepointSqlServerConnectionMock(SqlServerDbMock db)
        : SqlServerConnectionMock(db)
    {
        protected override bool SupportsSavepoints => false;
    }


    private sealed class ReleaseEnabledSqlServerConnectionMock(SqlServerDbMock db)
        : SqlServerConnectionMock(db)
    {
        protected override bool SupportsReleaseSavepoint => true;
    }

}
