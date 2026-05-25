namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for the Firebird data source mock surface.
/// PT-br: Contem testes para a superficie do mock de fonte de dados Firebird.
/// </summary>
public sealed class FirebirdDataSourceMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the mock data source exposes an empty connection string and creates connections bound to the supplied database.
    /// PT-br: Verifica se a fonte de dados simulada expõe string de conexão vazia e cria conexões vinculadas ao banco informado.
    /// </summary>
    [Fact]
    public void DataSource_ShouldExposeEmptyConnectionString_AndCreateBoundConnections()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1 });

        var dataSource = new FirebirdDataSourceMock(db);

#if NET7_0_OR_GREATER
        dataSource.ConnectionString.Should().BeEmpty();
        using var connection = dataSource.CreateConnection();
#else
        using var connection = dataSource.CreateDbConnection();
#endif

        connection.Should().BeOfType<FirebirdConnectionMock>();
        ((FirebirdConnectionMock)connection).GetTable("Users").Should().ContainSingle();
    }
}
