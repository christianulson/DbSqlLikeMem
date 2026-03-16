namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Contains tests for my sql provider surface mocks.
/// PT: Contém testes para my sql provedor surface mocks.
/// </summary>
public sealed class MySqlProviderSurfaceMocksTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures the typed SelectCommand property stays synchronized with the base SelectCommand.
    /// PT: Garante que a propriedade tipada SelectCommand permaneça sincronizada com a SelectCommand da classe base.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new MySqlConnectionMock(new MySqlDbMock());
        var adapter = new MySqlDataAdapterMock("SELECT 1", connection);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
    }

    /// <summary>
    /// EN: Ensures the data source mock creates a provider-specific connection bound to the same in-memory database.
    /// PT: Garante que o simulado de fonte de dados crie uma conexão específica do provedor vinculada ao mesmo banco em memória.
    /// </summary>
    [Fact]
    public void DataSource_ShouldCreateMySqlConnection()
    {
        var source = new MySqlDataSourceMock([]);
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        Assert.IsType<MySqlConnectionMock>(connection);
    }

    /// <summary>
    /// EN: Ensures default adapter state matches the provider contract surface.
    /// PT: Garante que o estado padrão do adapter corresponda à superfície contratual do provedor.
    /// </summary>
    [Fact]
    public void DataAdapter_DefaultCtor_ShouldExposeExpectedDefaults()
    {
        var adapter = new MySqlDataAdapterMock();

        Assert.True(adapter.LoadDefaults);
        Assert.Equal(1, adapter.UpdateBatchSize);
    }

    /// <summary>
    /// EN: Ensures async fill overload honors a pre-canceled token and returns a canceled task.
    /// PT: Garante que a sobrecarga assíncrona de fill respeite um token já cancelado e retorne task cancelada.
    /// </summary>
    [Fact]
    public async Task FillAsync_DataSet_WithCanceledToken_ShouldReturnCanceledTask()
    {
        var adapter = new MySqlDataAdapterMock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.FillAsync(new DataSet(), cts.Token));
    }

    /// <summary>
    /// EN: Ensures DataTable async overload also respects pre-canceled tokens.
    /// PT: Garante que a sobrecarga assíncrona de DataTable também respeite token previamente cancelado.
    /// </summary>
    [Fact]
    public async Task FillAsync_DataTable_WithCanceledToken_ShouldReturnCanceledTask()
    {
        var adapter = new MySqlDataAdapterMock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.FillAsync(new DataTable(), cts.Token));
    }

    /// <summary>
    /// EN: Ensures additional fill overloads also honor a pre-canceled token without touching the underlying operation.
    /// PT: Garante que sobrecargas adicionais de fill também respeitem um token previamente cancelado sem tocar na operacao subjacente.
    /// </summary>
    [Fact]
    public async Task FillAsync_AdditionalOverloads_WithCanceledToken_ShouldReturnCanceledTask()
    {
        var adapter = new MySqlDataAdapterMock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.FillAsync(new DataSet(), "Users", cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.FillAsync(new DataTable(), new DataTableReader(new DataTable()), cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.FillAsync(new DataTable(), new MySqlCommandMock(), CommandBehavior.Default, cts.Token));
    }

    /// <summary>
    /// EN: Ensures fill-schema and update async overloads also honor pre-canceled tokens.
    /// PT: Garante que as sobrecargas assincronas de fill-schema e update também respeitem tokens previamente cancelados.
    /// </summary>
    [Fact]
    public async Task FillSchemaAndUpdateAsync_WithCanceledToken_ShouldReturnCanceledTask()
    {
        var adapter = new MySqlDataAdapterMock();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.FillSchemaAsync(new DataSet(), SchemaType.Source, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.FillSchemaAsync(new DataTable(), SchemaType.Source, cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => adapter.UpdateAsync(new DataTable(), cts.Token));
    }
}
