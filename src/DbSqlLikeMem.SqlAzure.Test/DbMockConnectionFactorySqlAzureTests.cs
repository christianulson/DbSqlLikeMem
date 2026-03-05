namespace DbSqlLikeMem.SqlAzure.Test;

public sealed class DbMockConnectionFactorySqlAzureTests : DbMockConnectionFactoryContractTestsBase
{
    protected override string ProviderHint => "SqlAzure";
    protected override Type ExpectedDbType => typeof(SqlAzureDbMock);
    protected override Type ExpectedConnectionType => typeof(SqlAzureConnectionMock);
    protected override IReadOnlyList<string> ProviderAliases =>
        ["SqlAzure", "sqlazure", "AzureSql", "azure-sql", "azure_sql", "  azure-sql  "];

    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateSqlAzureWithTables(tableMappers);
}
