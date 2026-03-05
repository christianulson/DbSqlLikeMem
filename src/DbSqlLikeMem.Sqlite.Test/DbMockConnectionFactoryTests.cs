namespace DbSqlLikeMem.Sqlite.Test;

public sealed class DbMockConnectionFactoryTests : DbMockConnectionFactoryContractTestsBase
{
    protected override string ProviderHint => "Sqlite";
    protected override Type ExpectedDbType => typeof(SqliteDbMock);
    protected override Type ExpectedConnectionType => typeof(SqliteConnectionMock);
    protected override IReadOnlyList<string> ProviderAliases => ["Sqlite", "sqlite", "sql_ite", "  SQLITE  "];

    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateSqliteWithTables(tableMappers);
}
