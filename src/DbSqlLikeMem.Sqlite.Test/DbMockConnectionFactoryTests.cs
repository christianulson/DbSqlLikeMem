namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Validates SQLite test-connection factory helpers.
/// PT: Valida os helpers da fábrica de conexões de teste SQLite.
/// </summary>
public class DbMockConnectionFactoryTests
{
    /// <summary>
    /// EN: Verifies CreateSqliteWithTables returns SQLite db and connection mocks.
    /// PT: Verifica que CreateSqliteWithTables retorna mocks de banco e conexão SQLite.
    /// </summary>
    [Fact]
    public void CreateSqliteWithTables_ShouldCreateSqliteDbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateSqliteWithTables();

        db.Should().BeOfType<SqliteDbMock>();
        connection.Should().BeOfType<SqliteConnectionMock>();
    }

    /// <summary>
    /// EN: Verifies table mapper callbacks are applied during factory creation.
    /// PT: Verifica que callbacks de mapeamento de tabela são aplicados na criação da fábrica.
    /// </summary>
    [Fact]
    public void CreateWithTables_ShouldApplyTableMappers()
    {
        var (db, _) = DbMockConnectionFactory.CreateWithTables(
            "Sqlite",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.GetTable("Users").Should().HaveCount(1);
    }
}
