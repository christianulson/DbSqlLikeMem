namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Adds focused coverage for MySqlCommandMock surface behavior, batching helpers, and cloning.
/// PT: Adiciona cobertura focada para comportamento de superficie, helpers de batching e clonagem de MySqlCommandMock.
/// </summary>
public sealed class MySqlCommandMockCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies Cancel rolls back the active transaction and CreateParameter returns a provider-specific parameter.
    /// PT: Verifica se Cancel faz rollback da transacao ativa e se CreateParameter retorna um parametro especifico do provedor.
    /// </summary>
    [Fact]
    public void Cancel_ShouldRollbackTransaction_AndCreateParameterShouldBeTyped()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        using var command = new MySqlCommandMock(connection, (MySqlTransactionMock)transaction)
        {
            CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')"
        };

        command.ExecuteNonQuery();
        command.Cancel();

        connection.GetTable("Users").Should().BeEmpty();
        command.CreateParameter().Should().BeOfType<MySqlParameter>();
    }

    /// <summary>
    /// EN: Verifies batching helpers keep track of grouped commands and derive batchable command text.
    /// PT: Verifica se os helpers de batching acompanham comandos agrupados e derivam o texto batchable do comando.
    /// </summary>
    [Fact]
    public void BatchingHelpers_ShouldTrackBatchAndCommandText()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        var insert = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')"
        };
        var sibling = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name) VALUES (2, 'Beto')"
        };
        var update = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE Users SET Name = 'Caio' WHERE Id = 1"
        };

        insert.AddToBatch(sibling);

        insert.Batch.Should().NotBeNull();
        insert.Batch.Should().ContainSingle().Which.Should().BeSameAs(sibling);
        insert.GetCommandTextForBatching().Should().NotBeNull();
        insert.BatchableCommandText.Should().NotBeNull();
        update.GetCommandTextForBatching().Should().Be(update.CommandText);

        var upsert = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name) VALUES (3, 'Caio') ON DUPLICATE KEY UPDATE Name = 'Caio'"
        };
        upsert.GetCommandTextForBatching().Should().BeNull();
        upsert.BatchableCommandText.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies cloning copies command surface state and parameter metadata into an independent command instance.
    /// PT: Verifica se a clonagem copia o estado da superficie do comando e os metadados de parametros para uma instancia independente.
    /// </summary>
    [Fact]
    public void Clone_ShouldCopyCommandStateAndParameters()
    {
        var db = new MySqlDbMock();
        using var connection = new MySqlConnectionMock(db);

        var command = new MySqlCommandMock(connection)
        {
            CommandText = "SELECT * FROM Users WHERE Id = @id",
            CommandTimeout = 42,
            CommandType = CommandType.Text,
            UpdatedRowSource = UpdateRowSource.FirstReturnedRecord,
            DesignTimeVisible = true
        };
        command.Parameters.Add(new MySqlParameter("@id", 7)
        {
            Direction = ParameterDirection.InputOutput,
            SourceColumn = "Id",
            SourceVersion = DataRowVersion.Original,
            IsNullable = false,
            Size = 4
        });

        var clone = (MySqlCommandMock)((ICloneable)command).Clone();
        var clonedParameter = (MySqlParameter)clone.Parameters[0];
        var originalParameter = (MySqlParameter)command.Parameters[0];

        clone.Should().NotBeSameAs(command);
        clone.Connection.Should().BeSameAs(command.Connection);
        clone.CommandText.Should().Be(command.CommandText);
        clone.CommandTimeout.Should().Be(42);
        clone.CommandType.Should().Be(CommandType.Text);
        clone.UpdatedRowSource.Should().Be(UpdateRowSource.FirstReturnedRecord);
        clone.DesignTimeVisible.Should().BeTrue();
        Assert.Single(clone.Parameters);
        clonedParameter.Should().NotBeSameAs(originalParameter);
        clonedParameter.ParameterName.Should().Be("@id");
        clonedParameter.Value.Should().Be(7);
        clonedParameter.Direction.Should().Be(ParameterDirection.InputOutput);
        clonedParameter.SourceColumn.Should().Be("Id");
        clonedParameter.SourceVersion.Should().Be(DataRowVersion.Original);
        clonedParameter.IsNullable.Should().BeFalse();
        clonedParameter.Size.Should().Be(4);

        clonedParameter.Value = 8;
        originalParameter.Value.Should().Be(7);
    }

    /// <summary>
    /// EN: Verifies Prepare and Dispose can be called safely on the mock command surface.
    /// PT: Verifica se Prepare e Dispose podem ser chamados com seguranca na superficie simulada do comando.
    /// </summary>
    [Fact]
    public void PrepareAndDispose_ShouldBeSafeNoOps()
    {
        var command = new MySqlCommandMock();

        command.Prepare();
        command.Dispose();
        command.Dispose();
    }

    /// <summary>
    /// EN: Verifies replacing the connection clears the associated transaction and empty scalar reads return DBNull.
    /// PT: Verifica se substituir a conexao limpa a transacao associada e se leituras escalares vazias retornam DBNull.
    /// </summary>
    [Fact]
    public void ConnectionSwapAndEmptyScalar_ShouldResetTransactionAndReturnDbNull()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var firstConnection = new MySqlConnectionMock(db);
        using var secondConnection = new MySqlConnectionMock(db);
        firstConnection.Open();
        secondConnection.Open();

        using var transaction = firstConnection.BeginTransaction();
        using var command = new MySqlCommandMock(firstConnection);
        ((DbCommand)command).Transaction = transaction;

        command.Connection = secondConnection;

        ((DbCommand)command).Transaction.Should().BeNull();

        command.CommandText = "SELECT Name FROM Users WHERE Id = 999";
        command.ExecuteScalar().Should().Be(DBNull.Value);
    }
}
