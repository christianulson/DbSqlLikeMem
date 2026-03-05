namespace DbSqlLikeMem.SqlServer.Test;

public sealed class DbMockConnectionFactorySqlServerTests : DbMockConnectionFactoryContractTestsBase
{
    protected override string ProviderHint => "SqlServer";
    protected override Type ExpectedDbType => typeof(SqlServerDbMock);
    protected override Type ExpectedConnectionType => typeof(SqlServerConnectionMock);
    protected override IReadOnlyList<string> ProviderAliases => ["SqlServer", "sqlserver", "sql-server", "  SQLSERVER  "];

    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateSqlServerWithTables(tableMappers);
}
