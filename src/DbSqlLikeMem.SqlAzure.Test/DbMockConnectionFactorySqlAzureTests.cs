namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Validates SqlAzure-specific DbMock connection factory contract behavior.
/// PT: Valida o comportamento do contrato da factory de conexao DbMock especifica de SqlAzure.
/// </summary>
public sealed class DbMockConnectionFactorySqlAzureTests : DbMockConnectionFactoryContractTestsBase
{
    /// <inheritdoc />
    protected override string ProviderHint => "SqlAzure";
    /// <inheritdoc />
    protected override Type ExpectedDbType => typeof(SqlAzureDbMock);
    /// <inheritdoc />
    protected override Type ExpectedConnectionType => typeof(SqlAzureConnectionMock);
    /// <inheritdoc />
    protected override IReadOnlyList<string> ProviderAliases =>
        ["SqlAzure", "sqlazure", "AzureSql", "azure-sql", "azure_sql", "azure-sql-db", "  azure-sql  "];

    /// <inheritdoc />
    protected override (DbMock Db, IDbConnection Connection) CreateViaProviderShortcut(params Action<DbMock>[] tableMappers)
        => DbMockConnectionFactory.CreateSqlAzureWithTables(tableMappers);
}
