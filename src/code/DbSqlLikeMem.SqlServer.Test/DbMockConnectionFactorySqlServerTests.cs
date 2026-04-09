namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Validates SqlServer-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de SqlServer.
/// </summary>
public sealed class DbMockConnectionFactorySqlServerTests(
        ITestOutputHelper helper
    ) : DbMockConnectionFactoryContractTestsBase(helper)
{
    /// <inheritdoc />
    protected override string ProviderHint => "SqlServer";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(SqlServerDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(SqlServerConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases => ["SqlServer", "sqlserver", "sql-server", "mssql", "sql-srv", "  SQLSERVER  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateSqlServerWithTables(tableMappers);
}
