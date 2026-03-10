namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Verifies MySqlDataSourceMock exposes the expected provider-facing surface.
/// PT: Verifica se MySqlDataSourceMock expõe a superfície esperada do provedor.
/// </summary>
public sealed class MySqlDataSourceMockTests
{
    /// <summary>
    /// EN: Verifies the mock data source exposes an empty connection string and creates connections bound to the supplied database.
    /// PT: Verifica se a fonte de dados simulada expõe string de conexão vazia e cria conexões vinculadas ao banco informado.
    /// </summary>
    [Fact]
    public void DataSource_ShouldExposeEmptyConnectionString_AndCreateBoundConnections()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1 });

        var dataSource = new MySqlDataSourceMock(db);

#if NET7_0_OR_GREATER
        dataSource.ConnectionString.Should().BeEmpty();
        using var connection = dataSource.CreateConnection();
#else
        using var connection = dataSource.CreateDbConnection();
#endif

        connection.Should().BeOfType<MySqlConnectionMock>();
        ((MySqlConnectionMock)connection).GetTable("Users").Should().ContainSingle();
    }
}
