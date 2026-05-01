namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Provides provider-specific Dapper smoke coverage through the shared generic contract base.
/// PT-br: Fornece cobertura smoke específica do provedor de Dapper através da base genérica de contrato compartilhada.
/// </summary>
public sealed class DapperSmokeTests(
    ITestOutputHelper helper
) : DapperSmokeTestsBase<SqlServerConnectionMock>(helper, static () => new SqlServerConnectionMock())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new DbSqlLikeMem.SqlServer.TestTools.SqlServerProviderSqlDialect();
}
