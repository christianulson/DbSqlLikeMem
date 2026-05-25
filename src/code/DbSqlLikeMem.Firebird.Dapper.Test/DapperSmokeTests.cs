namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Provides provider-specific Dapper smoke coverage through the shared generic contract base.
/// PT-br: Fornece cobertura smoke específica do provedor de Dapper através da base genérica de contrato compartilhada.
/// </summary>
public sealed class DapperSmokeTests(
    ITestOutputHelper helper
) : DapperSmokeTestsBase<FirebirdConnectionMock>(helper, static () => new FirebirdConnectionMock())
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } =
        new FirebirdProviderSqlDialect();
}
