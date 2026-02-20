namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Summary for MySqlProviderSurfaceMocksTests.
/// PT: Resumo para MySqlProviderSurfaceMocksTests.
/// </summary>
public sealed class MySqlProviderSurfaceMocksTests
{
    [Fact]
    /// <summary>
    /// EN: Summary for DataAdapter_ShouldKeepTypedSelectCommand.
    /// PT: Resumo para DataAdapter_ShouldKeepTypedSelectCommand.
    /// </summary>
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new MySqlConnectionMock(new MySqlDbMock());
        var adapter = new MySqlDataAdapterMock("SELECT 1", connection);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
    }

    [Fact]
    /// <summary>
    /// EN: Summary for DataSource_ShouldCreateMySqlConnection.
    /// PT: Resumo para DataSource_ShouldCreateMySqlConnection.
    /// </summary>
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
