namespace DbSqlLikeMem.Oracle.Test;

public sealed class DbMockConnectionFactoryOracleTests : DbMockConnectionFactoryContractTestsBase
{
    protected override string ProviderHint => "Oracle";
    protected override Type ExpectedDbType => typeof(OracleDbMock);
    protected override Type ExpectedConnectionType => typeof(OracleConnectionMock);
    protected override IReadOnlyList<string> ProviderAliases => ["Oracle", "oracle", "or_acle", "  ORACLE  "];

    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateOracleWithTables(tableMappers);
}
