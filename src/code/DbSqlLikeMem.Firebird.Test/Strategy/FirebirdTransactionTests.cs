namespace DbSqlLikeMem.Firebird.Test.Strategy;

/// <summary>
/// EN: Covers transaction and savepoint behavior in the Firebird mock.
/// PT: Cobre o comportamento de transacao e savepoint no mock Firebird.
/// </summary>
public sealed class FirebirdTransactionTests
{
    /// <summary>
    /// EN: Ensures creating a savepoint without an active transaction keeps the actionable runtime message in Firebird.
    /// PT: Garante que criar um savepoint sem uma transacao ativa mantenha a mensagem acionavel em runtime no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new FirebirdDbMock();
        db.AddTable("users");

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.CreateSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rollback to savepoint without an active transaction keeps the actionable runtime message in Firebird.
    /// PT: Garante que rollback para savepoint sem uma transacao ativa mantenha a mensagem acionavel em runtime no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new FirebirdDbMock();
        db.AddTable("users");

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures releasing an unknown savepoint keeps the actionable runtime message in Firebird.
    /// PT: Garante que liberar um savepoint desconhecido mantenha a mensagem acionavel em runtime no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseUnknownSavepoint_ShouldProvideActionableMessage()
    {
        var db = new FirebirdDbMock();
        db.AddTable("users");

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        connection.CreateSavepoint("sp_known");

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_missing"));

        Assert.Contains("Savepoint", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("was not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures creating a savepoint with a blank name keeps the parameter validation message in Firebird.
    /// PT: Garante que criar um savepoint com nome em branco mantenha a mensagem de validacao de parametro no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void CreateSavepoint_WithBlankName_ShouldProvideParameterValidationMessage()
    {
        var db = new FirebirdDbMock();
        db.AddTable("users");

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<ArgumentException>(() => connection.CreateSavepoint(" "));

        Assert.Contains("savepointName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures rolling back to a savepoint with a blank name keeps the parameter validation message in Firebird.
    /// PT: Garante que executar rollback para um savepoint com nome em branco mantenha a mensagem de validacao de parametro no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackSavepoint_WithBlankName_ShouldProvideParameterValidationMessage()
    {
        var db = new FirebirdDbMock();
        db.AddTable("users");

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<ArgumentException>(() => connection.RollbackTransaction(" "));

        Assert.Contains("savepointName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures releasing a savepoint with a blank name keeps the parameter validation message in Firebird.
    /// PT: Garante que liberar um savepoint com nome em branco mantenha a mensagem de validacao de parametro no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_WithBlankName_ShouldProvideParameterValidationMessage()
    {
        var db = new FirebirdDbMock();
        db.AddTable("users");

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var ex = Assert.Throws<ArgumentException>(() => connection.ReleaseSavepoint(" "));

        Assert.Contains("savepointName", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies that rolling back to a savepoint restores the temporary-table snapshot in Firebird.
    /// PT: Verifica se o rollback para um savepoint restaura o snapshot da tabela temporaria no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot()
    {
        var db = new FirebirdDbMock();
        using var connection = new FirebirdConnectionMock(db);
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
        var db = new FirebirdDbMock();
        using var connection = new FirebirdConnectionMock(db);
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
    /// EN: Verifies that releasing a savepoint without an active transaction keeps the actionable runtime message in Firebird.
    /// PT: Verifica se liberar um savepoint sem uma transacao ativa mantem a mensagem acionavel em runtime no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ReleaseSavepoint_WithoutActiveTransaction_ShouldProvideActionableMessage()
    {
        var db = new FirebirdDbMock();
        db.AddTable("users");

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() => connection.ReleaseSavepoint("sp_no_tx"));

        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
