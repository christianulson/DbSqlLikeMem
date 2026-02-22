namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Contains tests for my sql provider surface mocks.
/// PT: Contém testes para my sql provedor surface mocks.
/// </summary>
public sealed class MySqlProviderSurfaceMocksTests
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
        var source = new MySqlDataSourceMock(new MySqlDbMock());
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        Assert.IsType<MySqlConnectionMock>(connection);
    }
}
