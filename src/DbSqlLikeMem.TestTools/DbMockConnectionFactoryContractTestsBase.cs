namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides a shared readability-focused contract for provider-specific DbMockConnectionFactory tests.
/// PT: Fornece um contrato compartilhado, focado em legibilidade, para testes por provedor da DbMockConnectionFactory.
/// </summary>
public abstract class DbMockConnectionFactoryContractTestsBase
{
    protected abstract string ProviderHint { get; }
    protected abstract Type ExpectedDbType { get; }
    protected abstract Type ExpectedConnectionType { get; }
    protected abstract IReadOnlyList<string> ProviderAliases { get; }

    protected abstract (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers);

    [Fact]
    public void CreateViaProviderShortcut_ShouldCreateExpectedDbAndConnection()
    {
        var (db, connection) = CreateViaProviderShortcut();

        db.Should().BeOfType(ExpectedDbType);
        connection.Should().BeOfType(ExpectedConnectionType);
    }

    [Fact]
    public void CreateWithTables_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            ProviderHint,
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType(ExpectedDbType);
        connection.Should().BeOfType(ExpectedConnectionType);
        db.GetTable("Users").Should().HaveCount(1);
    }

    [Fact]
    public void CreateWithTables_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            ProviderHint,
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables(ProviderHint);

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }

    [Fact]
    public void CreateWithTables_ForAliases_ShouldResolveExpectedTypes()
    {
        foreach (var providerHint in ProviderAliases)
        {
            var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);
            db.Should().BeOfType(ExpectedDbType, because: $"alias '{providerHint}' must resolve to expected DbMock type");
            connection.Should().BeOfType(ExpectedConnectionType, because: $"alias '{providerHint}' must resolve to expected connection type");
        }
    }
}
