namespace DbSqlLikeMem.MariaDb.Test.Strategy;

/// <summary>
/// EN: Covers transaction commit, rollback, and savepoint scenarios in the MariaDB mock.
/// PT: Cobre cenarios de commit, rollback e savepoint na mock MariaDB.
/// </summary>
public sealed class MariaDbTransactionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that committing a transaction persists the pending changes.
    /// PT: Verifica se o commit de uma transacao persiste as alteracoes pendentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldCommit()
    {
        var db = new MariaDbDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();

        using var transaction = Assert.IsType<MySqlTransactionMock>(connection.BeginTransaction());
        using var command = new MySqlCommandMock(connection, transaction)
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
    /// EN: Verifies that rolling back a transaction discards the pending changes.
    /// PT: Verifica se o rollback de uma transacao descarta as alteracoes pendentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionShouldRollback()
    {
        var db = new MariaDbDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();

        using var transaction = Assert.IsType<MySqlTransactionMock>(connection.BeginTransaction());
        using var command = new MySqlCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        command.ExecuteNonQuery();
        transaction.Rollback();

        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint restores connection temporary-table snapshot.
    /// PT: Garante que rollback para savepoint restaure snapshot de tabela temporária da conexão.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot()
    {
        var db = new MariaDbDbMock();
        using var connection = new MariaDbConnectionMock(db);
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

        connection.RollbackTransaction("sp_temp");
        transaction.Commit();

        Assert.Single(temp);
        Assert.Equal(1, temp[0][0]);
        Assert.Equal("Ana", temp[0][1]);
    }

    /// <summary>
    /// EN: Ensures nested savepoints restore the transaction snapshot from the selected outer point.
    /// PT: Garante que savepoints aninhados restaurem o snapshot da transacao a partir do ponto externo selecionado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void NestedSavepoints_ShouldRollbackToSelectedOuterSnapshot()
    {
        var db = new MariaDbDbMock();
        using var connection = new MariaDbConnectionMock(db);
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

        connection.RollbackTransaction("sp_outer");
        transaction.Commit();

        Assert.Single(temp);
        Assert.Equal(1, temp[0][0]);
        Assert.Equal("Ana", temp[0][1]);
    }

    /// <summary>
    /// EN: Ensures releasing a savepoint without an active transaction keeps the actionable runtime message in MariaDB.
    /// PT: Garante que liberar um savepoint sem uma transacao ativa mantenha a mensagem acionavel em runtime no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new MariaDbDbMock();
        db.AddTable("users");

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures releasing an unknown savepoint keeps the actionable runtime message in MariaDB.
    /// PT: Garante que liberar um savepoint desconhecido mantenha a mensagem acionavel em runtime no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseUnknownSavepoint_ShouldProvideActionableMessage()
    {
        var db = new MariaDbDbMock();
        db.AddTable("users");

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        connection.CreateSavepoint("sp_known");

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_missing"));

        Assert.Contains("Savepoint", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("was not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures creating a savepoint with an empty name keeps the existing parameter validation message.
    /// PT: Garante que criar um savepoint com nome vazio mantenha a mensagem de validacao de parametro existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_WithBlankName_ShouldProvideParameterValidationMessage()
    {
        var db = new MariaDbDbMock();
        db.AddTable("users");

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<ArgumentException>(() => connection.CreateSavepoint(" "));

        Assert.Contains("savepointName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures releasing a savepoint with an empty name keeps the existing parameter validation message.
    /// PT: Garante que liberar um savepoint com nome vazio mantenha a mensagem de validacao de parametro existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_WithBlankName_ShouldProvideParameterValidationMessage()
    {
        var db = new MariaDbDbMock();
        db.AddTable("users");

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<ArgumentException>(() => connection.ReleaseSavepoint(" "));

        Assert.Contains("savepointName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures creating a savepoint without an active transaction keeps the actionable runtime message in MariaDB.
    /// PT: Garante que criar um savepoint sem uma transacao ativa mantenha a mensagem acionavel em runtime no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new MariaDbDbMock();
        db.AddTable("users");

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.CreateSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint without an active transaction keeps the actionable runtime message in MariaDB.
    /// PT: Garante que rollback para savepoint sem uma transacao ativa mantenha a mensagem acionavel em runtime no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new MariaDbDbMock();
        db.AddTable("users");

        using var connection = new MariaDbConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
