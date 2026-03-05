namespace DbSqlLikeMem.MySql.Test;

public sealed class DbMockConnectionFactoryMySqlTests : DbMockConnectionFactoryContractTestsBase
{
    protected override string ProviderHint => "MySql";
    protected override Type ExpectedDbType => typeof(MySqlDbMock);
    protected override Type ExpectedConnectionType => typeof(MySqlConnectionMock);
    protected override IReadOnlyList<string> ProviderAliases => ["MySql", "mysql", "my-sql", "  MYSQL  "];

    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateMySqlWithTables(tableMappers);
}
