using System.Reflection;

namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Adds focused coverage for MySqlBatchMock and MySqlBatchCommandMock guard and surface behavior.
/// PT: Adiciona cobertura focada para comportamento de superficie e validacoes de MySqlBatchMock e MySqlBatchCommandMock.
/// </summary>
public sealed class MySqlBatchMockCoverageTests
{
    private static object? InvokeNonPublic(MySqlBatchMock batch, string methodName, params object?[] args)
    {
        var method = typeof(MySqlBatchMock).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(MySqlBatchMock).FullName, methodName);
        return method.Invoke(batch, args);
    }

    /// <summary>
    /// EN: Verifies batch commands expose their default provider-facing surface.
    /// PT: Verifica se comandos em lote expõem sua superficie padrao voltada ao provedor.
    /// </summary>
    [Fact]
    public void BatchCommand_ShouldExposeExpectedDefaults()
    {
        var batchCommand = new MySqlBatchCommandMock();

        batchCommand.CommandText.Should().BeEmpty();
        batchCommand.CommandType.Should().Be(CommandType.Text);
        batchCommand.RecordsAffected.Should().Be(0);
        batchCommand.Parameters.Should().NotBeNull();
        batchCommand.CreateParameter().Should().BeOfType<MySqlParameter>();
        batchCommand.CanCreateParameter.Should().BeTrue();

        var asProviderCommand = (IMySqlCommandMock)batchCommand;
        asProviderCommand.AllowUserVariables.Should().BeFalse();
        asProviderCommand.Connection.Should().BeNull();
        asProviderCommand.OutParameters.Should().BeNull();
        asProviderCommand.ReturnParameter.Should().BeNull();
        asProviderCommand.LastInsertedId.Should().Be(0);

        var outParameters = new MySqlCommand().Parameters;
        var returnParameter = new MySqlParameter("@return", 1);
        asProviderCommand.OutParameters = outParameters;
        asProviderCommand.ReturnParameter = returnParameter;
        asProviderCommand.SetLastInsertedId(99);

        asProviderCommand.OutParameters.Should().BeSameAs(outParameters);
        asProviderCommand.ReturnParameter.Should().BeSameAs(returnParameter);
        asProviderCommand.LastInsertedId.Should().Be(99);
    }

    /// <summary>
    /// EN: Verifies batch execution rejects invalid state such as missing connection, empty command list, and disposed instances.
    /// PT: Verifica se a execucao em lote rejeita estado invalido como conexao ausente, lista vazia de comandos e instancias descartadas.
    /// </summary>
    [Fact]
    public void BatchExecution_ShouldValidateStateBeforeRunning()
    {
        using var withoutConnection = new MySqlBatchMock();
        Action withoutConnectionAction = () => withoutConnection.ExecuteNonQuery();
        withoutConnectionAction.Should().Throw<InvalidOperationException>();

        using var connection = new MySqlConnectionMock(new MySqlDbMock());
        connection.Open();

        using var withoutCommands = new MySqlBatchMock(connection);
        Action withoutCommandsAction = () => withoutCommands.ExecuteScalar();
        withoutCommandsAction.Should().Throw<InvalidOperationException>();

        var disposed = new MySqlBatchMock(connection);
        disposed.Dispose();
        Action disposedAction = () => disposed.ExecuteScalar();
        disposedAction.Should().Throw<ObjectDisposedException>();
    }

    /// <summary>
    /// EN: Verifies prepare rejects non-text commands and async execution honors canceled tokens after validation succeeds.
    /// PT: Verifica se prepare rejeita comandos nao textuais e se a execucao assincrona respeita tokens cancelados apos a validacao.
    /// </summary>
    [Fact]
#pragma warning disable xUnit1051
    public async Task BatchPrepareAndAsyncExecution_ShouldHonorValidationAndCancellation()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var invalidPrepare = new MySqlBatchMock(connection);
        invalidPrepare.BatchCommands.Add(new MySqlBatchCommandMock("Users")
        {
            CommandType = CommandType.StoredProcedure
        });

        await Assert.ThrowsAsync<NotSupportedException>(() => invalidPrepare.PrepareAsync());

        using var validBatch = new MySqlBatchMock(connection);
        validBatch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT 1"));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => validBatch.ExecuteNonQueryAsync(cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => validBatch.ExecuteScalarAsync(cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => validBatch.ExecuteReaderAsync(cts.Token));
    }
#pragma warning restore xUnit1051

    /// <summary>
    /// EN: Verifies valid async execution runs commands, returns results, and preserves command settings in generated commands.
    /// PT: Verifica se a execucao assincrona valida executa comandos, retorna resultados e preserva configuracoes nos comandos gerados.
    /// </summary>
    [Fact]
#pragma warning disable AsyncFixer02
#pragma warning disable xUnit1051
    public async Task BatchAsyncExecution_ShouldExecuteCommandsAndCopyCommandSettings()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();
        using var batch = new MySqlBatchMock(connection, (MySqlTransactionMock)transaction)
        {
            Timeout = 15
        };

        var insert = new MySqlBatchCommandMock("INSERT INTO Users (Id, Name) VALUES (@id, @name)");
        insert.Parameters.Add(new MySqlParameter("@id", 1));
        insert.Parameters.Add(new MySqlParameter("@name", "Ana"));
        batch.BatchCommands.Add(insert);
        batch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT Name FROM Users WHERE Id = 1"));

        var affected = await batch.ExecuteNonQueryAsync();
        using var scalarBatch = new MySqlBatchMock(connection, (MySqlTransactionMock)transaction)
        {
            Timeout = 15
        };
        scalarBatch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT Name FROM Users WHERE Id = 1"));

        var scalar = await scalarBatch.ExecuteScalarAsync();
        using var reader = await scalarBatch.ExecuteReaderAsync();

        affected.Should().Be(1);
        scalar.Should().Be("Ana");
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");

        var executable = (MySqlCommandMock)InvokeNonPublic(batch, "CreateExecutableCommand", insert)!;
        executable.Connection.Should().BeSameAs(connection);
        ((DbCommand)executable).Transaction.Should().BeSameAs(transaction);
        executable.CommandText.Should().Be(insert.CommandText);
        executable.CommandTimeout.Should().Be(15);
        Assert.Equal(2, executable.Parameters.Count);
        ((MySqlParameter)executable.Parameters[0]).Value.Should().Be(1);
        ((MySqlParameter)executable.Parameters[1]).Value.Should().Be("Ana");
    }
#pragma warning restore xUnit1051
#pragma warning restore AsyncFixer02

    /// <summary>
    /// EN: Verifies prepare succeeds for text commands and binds batch commands back to the owning connection.
    /// PT: Verifica se o prepare tem sucesso para comandos textuais e vincula os comandos do lote de volta a conexao dona.
    /// </summary>
    [Fact]
    public void PrepareAsync_WithTextCommands_ShouldCompleteAndBindProviderState()
    {
        using var connection = new MySqlConnectionMock(new MySqlDbMock());
        connection.Open();

        using var batch = new MySqlBatchMock(connection);
        var batchCommand = new MySqlBatchCommandMock("SELECT 1");
        batch.BatchCommands.Add(batchCommand);

        batch.Prepare();
        batch.Prepare();

        var asProviderCommand = (IMySqlCommandMock)batchCommand;
        asProviderCommand.Connection.Should().BeSameAs(connection);
        asProviderCommand.CommandBehavior.Should().Be(CommandBehavior.Default);
    }
}
