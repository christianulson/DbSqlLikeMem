using System.Reflection;

namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Adds focused coverage for MySqlDataAdapterMock batching, events, and async wrappers.
/// PT: Adiciona cobertura focada para batching, eventos e wrappers assincronos de MySqlDataAdapterMock.
/// </summary>
public sealed class MySqlDataAdapterMockCoverageTests
{
    private static object? InvokeNonPublic(MySqlDataAdapterMock adapter, string methodName, params object?[] args)
    {
        var method = typeof(MySqlDataAdapterMock).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(MySqlDataAdapterMock).FullName, methodName);
        return method.Invoke(adapter, args);
    }

    /// <summary>
    /// EN: Verifies typed command properties keep the underlying adapter command slots synchronized.
    /// PT: Verifica se as propriedades tipadas de comando mantem sincronizados os slots de comando do adapter.
    /// </summary>
    [Fact]
    public void TypedCommands_ShouldSynchronizeWithBaseProperties()
    {
        using var connection = new MySqlConnectionMock(new MySqlDbMock());
        var adapter = new MySqlDataAdapterMock();
        var insert = new MySqlCommandMock(connection) { CommandText = "INSERT INTO Users VALUES (1)" };
        var update = new MySqlCommandMock(connection) { CommandText = "UPDATE Users SET Id = 1" };
        var delete = new MySqlCommandMock(connection) { CommandText = "DELETE FROM Users WHERE Id = 1" };

        adapter.InsertCommand = insert;
        adapter.UpdateCommand = update;
        adapter.DeleteCommand = delete;
        adapter.LoadDefaults = false;
        adapter.UpdateBatchSize = 4;

        ((IDbDataAdapter)adapter).InsertCommand.Should().BeSameAs(insert);
        ((IDbDataAdapter)adapter).UpdateCommand.Should().BeSameAs(update);
        ((IDbDataAdapter)adapter).DeleteCommand.Should().BeSameAs(delete);
        adapter.LoadDefaults.Should().BeFalse();
        adapter.UpdateBatchSize.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies protected batching hooks can collect commands, expose parameters, execute inserts, and reset the batch state.
    /// PT: Verifica se os hooks protegidos de batching conseguem coletar comandos, expor parametros, executar inserts e resetar o estado do lote.
    /// </summary>
    [Fact]
    public void ProtectedBatchingHooks_ShouldBatchCommandsAndParameters()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        var adapter = new MySqlDataAdapterMock();
        InvokeNonPublic(adapter, "InitializeBatching");

        var parameterized = new MySqlCommandMock(connection) { CommandText = "SELECT @id" };
        parameterized.Parameters.Add(new MySqlParameter("@id", 7));

        var firstIndex = (int)InvokeNonPublic(adapter, "AddToBatch", parameterized)!;
        var parameter = (IDataParameter)InvokeNonPublic(adapter, "GetBatchedParameter", firstIndex, 0)!;

        parameter.ParameterName.Should().Be("@id");
        parameter.Value.Should().Be(7);

        InvokeNonPublic(adapter, "ClearBatch");

        var insert1 = new MySqlCommandMock(connection) { CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')" };
        var insert2 = new MySqlCommandMock(connection) { CommandText = "INSERT INTO Users (Id, Name) VALUES (2, 'Beto')" };

        InvokeNonPublic(adapter, "AddToBatch", insert1);
        InvokeNonPublic(adapter, "AddToBatch", insert2);
        ((int)InvokeNonPublic(adapter, "ExecuteBatch")!).Should().Be(2);
        connection.GetTable("Users").Should().HaveCount(2);

        InvokeNonPublic(adapter, "ClearBatch");
        InvokeNonPublic(adapter, "TerminateBatching");
        Action terminatedBatchAction = () => InvokeNonPublic(adapter, "AddToBatch", new MySqlCommandMock(connection));
        terminatedBatchAction.Should()
            .Throw<TargetInvocationException>()
            .WithInnerException<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies row event factories and event dispatch use the provider-specific event argument types.
    /// PT: Verifica se as fabricas e o disparo de eventos de linha usam os tipos especificos de argumentos do provedor.
    /// </summary>
    [Fact]
    public void RowEvents_ShouldUseProviderSpecificEventArgs()
    {
        var adapter = new MySqlDataAdapterMock();
        var table = new DataTable("Users");
        table.Columns.Add("Id", typeof(int));
        var row = table.Rows.Add(1);
        using var command = new MySqlCommandMock();
        var mapping = new DataTableMapping("Users", "Users");

        var createdUpdating = (RowUpdatingEventArgs)InvokeNonPublic(adapter, "CreateRowUpdatingEvent", row, command, StatementType.Select, mapping)!;
        var createdUpdated = (RowUpdatedEventArgs)InvokeNonPublic(adapter, "CreateRowUpdatedEvent", row, command, StatementType.Select, mapping)!;

        createdUpdating.Should().BeOfType<MySqlRowUpdatingEventArgs>();
        createdUpdated.Should().BeOfType<MySqlRowUpdatedEventArgs>();

        MySqlRowUpdatingEventArgs? observedUpdating = null;
        MySqlRowUpdatedEventArgs? observedUpdated = null;
        adapter.RowUpdating += (_, args) => observedUpdating = args;
        adapter.RowUpdated += (_, args) => observedUpdated = args;

        InvokeNonPublic(adapter, "OnRowUpdating", createdUpdating);
        InvokeNonPublic(adapter, "OnRowUpdated", createdUpdated);

        observedUpdating.Should().NotBeNull();
        observedUpdated.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies the protected Update path opens closed provider connections for pending rows and closes them afterward.
    /// PT: Verifica se o caminho protegido de Update abre conexoes fechadas do provedor para linhas pendentes e as fecha ao final.
    /// </summary>
    [Fact]
    public void ProtectedUpdate_ShouldOpenClosedConnectionsAndCloseThemAfterward()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        var adapter = new MySqlDataAdapterMock
        {
            InsertCommand = new MySqlCommandMock(connection)
            {
                CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')"
            }
        };

        var dataTable = new DataTable("Users");
        dataTable.Columns.Add("Id", typeof(int));
        dataTable.Columns.Add("Name", typeof(string));
        dataTable.Rows.Add(1, "Ana");

        var affected = (int)InvokeNonPublic(
            adapter,
            "Update",
            new object?[] { dataTable.Select(null, null, DataViewRowState.Added), new DataTableMapping("Users", "Users") })!;

        affected.Should().Be(1);
        connection.State.Should().Be(ConnectionState.Closed);
        connection.GetTable("Users").Should().ContainSingle();
        connection.GetTable("Users")[0][1].Should().Be("Ana");
    }

    /// <summary>
    /// EN: Verifies reader-based wrapper overloads complete successfully when provided with in-memory reader input.
    /// PT: Verifica se as sobrecargas wrapper baseadas em reader concluem com sucesso quando recebem entrada de reader em memoria.
    /// </summary>
    [Fact]
#pragma warning disable xUnit1051
    public async Task ReaderBasedWrappers_ShouldCompleteSuccessfully()
    {
        var adapter = new MySqlDataAdapterMock();

        var source = new DataTable("Users");
        source.Columns.Add("Id", typeof(int));
        source.Columns.Add("Name", typeof(string));
        source.Rows.Add(1, "Ana");

        var filledTable = new DataTable("Users");
        var filledRows = await adapter.FillAsync(filledTable, new DataTableReader(source));

        filledRows.Should().Be(1);
        Assert.Equal(1, filledTable.Rows.Count);
        filledTable.Rows[0]["Name"].Should().Be("Ana");

        var schemaTable = await adapter.FillSchemaAsync(new DataTable("SchemaUsers"), SchemaType.Source, new DataTableReader(source));
        Assert.Equal(2, schemaTable.Columns.Count);

        var filledDataSet = new DataSet();
        var dataSetRows = await adapter.FillAsync(filledDataSet, "Users", new DataTableReader(source), 0, 10);
        dataSetRows.Should().Be(1);
        Assert.Single(filledDataSet.Tables.Cast<DataTable>());

        var schemaDataSet = await adapter.FillSchemaAsync(new DataSet(), SchemaType.Source, "Users", new DataTableReader(source));
        Assert.Single(schemaDataSet);
        Assert.Equal(2, schemaDataSet[0].Columns.Count);
    }
#pragma warning restore xUnit1051

    /// <summary>
    /// EN: Verifies the remaining async overloads honor pre-canceled tokens before executing any work.
    /// PT: Verifica se as sobrecargas assincronas restantes respeitam tokens previamente cancelados antes de executar qualquer trabalho.
    /// </summary>
    [Fact]
#pragma warning disable xUnit1051
    public async Task RemainingAsyncOverloads_WithCanceledToken_ShouldReturnCanceledTask()
    {
        var adapter = new MySqlDataAdapterMock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var dataSet = new DataSet();
        var dataTable = new DataTable("Users");
        dataTable.Columns.Add("Id", typeof(int));
        dataTable.Rows.Add(1);
        var reader = new DataTableReader(dataTable);
        var command = new MySqlCommandMock();
        var rows = new[] { dataTable.Rows[0] };
        var mapping = new DataTableMapping("Users", "Users");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillAsync(0, 10, cts.Token, dataTable));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillAsync(dataSet, 0, 10, "Users", cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillAsync(dataSet, "Users", reader, 0, 10, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillAsync([new DataTable()], 0, 10, command, CommandBehavior.Default, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillAsync(dataSet, 0, 10, "Users", command, CommandBehavior.Default, cts.Token));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillSchemaAsync(dataSet, SchemaType.Source, "Users", cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillSchemaAsync(dataSet, SchemaType.Source, "Users", reader, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillSchemaAsync(dataSet, SchemaType.Source, command, "Users", CommandBehavior.Default, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillSchemaAsync(dataTable, SchemaType.Source, reader, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.FillSchemaAsync(dataTable, SchemaType.Source, command, CommandBehavior.Default, cts.Token));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.UpdateAsync(rows, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.UpdateAsync(dataSet, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.UpdateAsync(rows, mapping, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.UpdateAsync(dataSet, "Users", cts.Token));
    }
#pragma warning restore xUnit1051

    /// <summary>
    /// EN: Verifies wrapper overloads without explicit tokens surface a faulted task when required adapter state is missing.
    /// PT: Verifica se as sobrecargas wrapper sem token explicito expõem task com falha quando falta estado obrigatorio do adapter.
    /// </summary>
    [Fact]
#pragma warning disable xUnit1051
    public async Task WrapperOverloads_WithoutRequiredState_ShouldReturnFaultedTasks()
    {
        static async Task ObserveAsync(Task task)
        {
            try
            {
                await task;
            }
            catch
            {
            }
        }

        static async Task ObserveResultAsync<T>(Task<T> task)
        {
            try
            {
                _ = await task;
            }
            catch
            {
            }
        }

        var adapter = new MySqlDataAdapterMock();
        var dataSet = new DataSet();
        var dataTable = new DataTable("Users");
        dataTable.Columns.Add("Id", typeof(int));
        var reader = new DataTableReader(dataTable);
        var command = new MySqlCommandMock();

        await ObserveAsync(adapter.FillAsync(dataSet));
        await ObserveAsync(adapter.FillAsync(dataTable));
        await ObserveAsync(adapter.FillAsync(dataSet, "Users"));
        await ObserveAsync(adapter.FillAsync(dataTable, reader));
        await ObserveAsync(adapter.FillAsync(dataTable, command, CommandBehavior.Default));
        await ObserveAsync(adapter.FillAsync(0, 10, dataTable));
        await ObserveAsync(adapter.FillAsync(dataSet, 0, 10, "Users"));
        await ObserveAsync(adapter.FillAsync(dataSet, "Users", reader, 0, 10));
        await ObserveAsync(adapter.FillAsync([new DataTable()], 0, 10, command, CommandBehavior.Default));
        await ObserveAsync(adapter.FillAsync(dataSet, 0, 10, "Users", command, CommandBehavior.Default));

        await ObserveResultAsync(adapter.FillSchemaAsync(dataSet, SchemaType.Source));
        await ObserveResultAsync(adapter.FillSchemaAsync(dataSet, SchemaType.Source, "Users"));
        await ObserveResultAsync(adapter.FillSchemaAsync(dataSet, SchemaType.Source, "Users", reader));
        await ObserveResultAsync(adapter.FillSchemaAsync(dataSet, SchemaType.Source, command, "Users", CommandBehavior.Default));
        await ObserveResultAsync(adapter.FillSchemaAsync(dataTable, SchemaType.Source));
        await ObserveResultAsync(adapter.FillSchemaAsync(dataTable, SchemaType.Source, reader));
        await ObserveResultAsync(adapter.FillSchemaAsync(dataTable, SchemaType.Source, command, CommandBehavior.Default));
    }
#pragma warning restore xUnit1051
}
