namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates MySql-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de MySql.
/// </summary>
public sealed class DbMockConnectionFactoryMySqlTests(
        ITestOutputHelper helper
    ) : DbMockConnectionFactoryContractTestsBase(helper)
{
    /// <inheritdoc />
    protected override string ProviderHint => "MySql";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(MySqlDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(MySqlConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases => ["MySql", "mysql", "my-sql", "mariadb", "  MYSQL  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateMySqlWithTables(tableMappers);
}
