namespace DbSqlLikeMem.Db2.Test;

public sealed class DbMockConnectionFactoryDb2Tests : DbMockConnectionFactoryContractTestsBase
{
    protected override string ProviderHint => "Db2";
    protected override Type ExpectedDbType => typeof(Db2DbMock);
    protected override Type ExpectedConnectionType => typeof(Db2ConnectionMock);
    protected override IReadOnlyList<string> ProviderAliases => ["Db2", "db2", "db-2", "  DB2  "];

    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateDb2WithTables(tableMappers);
}
