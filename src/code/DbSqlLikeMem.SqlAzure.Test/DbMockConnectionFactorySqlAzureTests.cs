namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Covers the SqlAzure-specific DbMock connection factory contract.
/// PT: Cobre o contrato da factory de conexao DbMock especifica de SqlAzure.
/// </summary>
public sealed class DbMockConnectionFactorySqlAzureTests(
        ITestOutputHelper helper
    ) : DbMockConnectionFactoryContractTestsBase(helper)
{
    /// <inheritdoc />
    protected override string ProviderHint => "SqlAzure";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(SqlAzureDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(SqlAzureConnectionMock);
    /// <inheritdoc />
    protected override Type ExpectedParameterType => typeof(SqlParameter);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases =>
        ["SqlAzure", "sqlazure", "AzureSql", "azure-sql", "azure_sql", "azure-sql-db", "  azure-sql  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateSqlAzureWithTables(tableMappers);
}
