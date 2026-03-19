namespace DbSqlLikeMem.MariaDb.Test;

/// <summary>
/// EN: Validates MariaDb-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de MariaDb.
/// </summary>
public sealed class DbMockConnectionFactoryMariaDbTests(
        ITestOutputHelper helper
    ) : DbMockConnectionFactoryContractTestsBase(helper)
{
    /// <inheritdoc />
    protected override string ProviderHint => "MariaDb";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(MariaDbDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(MariaDbConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases => ["MariaDb", "mariadb", "maria-db", "maria_db", "  MARIADB  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateMariaDbWithTables(tableMappers);
}
