namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Adds focused coverage for FirebirdCommandMock surface behavior, transaction handling, and scalar reads.
/// PT-br: Adiciona cobertura focada para comportamento de superficie, tratamento de transacao e leituras escalares de FirebirdCommandMock.
/// </summary>
public sealed class FirebirdCommandMockCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies Cancel rolls back the active transaction and CreateParameter returns a provider-specific parameter.
    /// PT-br: Verifica se Cancel faz rollback da transacao ativa e se CreateParameter retorna um parametro especifico do provedor.
    /// </summary>
    [Fact]
    public void Cancel_ShouldRollbackTransaction_AndCreateParameterShouldBeTyped()
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        using var command = new FirebirdCommandMock(connection, (FirebirdTransactionMock)transaction)
        {
            CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')"
        };

        command.ExecuteNonQuery();
        command.Cancel();

        connection.GetTable("users").Should().BeEmpty();
        command.CreateParameter().Should().BeOfType<FbParameter>();
    }

    /// <summary>
    /// EN: Verifies replacing the connection clears the associated transaction and empty scalar reads return DBNull.
    /// PT-br: Verifica se substituir a conexao limpa a transacao associada e se leituras escalares vazias retornam DBNull.
    /// </summary>
    [Fact]
    public void ConnectionSwapAndEmptyScalar_ShouldResetTransactionAndReturnDbNull()
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var firstConnection = new FirebirdConnectionMock(db);
        using var secondConnection = new FirebirdConnectionMock(db);
        firstConnection.Open();
        secondConnection.Open();

        using var transaction = firstConnection.BeginTransaction();
        using var command = new FirebirdCommandMock(firstConnection);
        ((DbCommand)command).Transaction = transaction;

        command.Connection = secondConnection;

        ((DbCommand)command).Transaction.Should().BeNull();

        command.CommandText = "SELECT Name FROM Users WHERE Id = 999";
        command.ExecuteScalar().Should().Be(DBNull.Value);
    }

    /// <summary>
    /// EN: Verifies Prepare and Dispose can be called safely on the mock command surface.
    /// PT-br: Verifica se Prepare e Dispose podem ser chamados com seguranca na superficie simulada do comando.
    /// </summary>
    [Fact]
    public void PrepareAndDispose_ShouldBeSafeNoOps()
    {
        var command = new FirebirdCommandMock();

        command.Prepare();
        command.Dispose();
        command.Dispose();
    }
}
