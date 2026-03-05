namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Provides provider-specific Dapper smoke coverage through the shared generic contract base.
/// PT: Fornece cobertura smoke específica do provedor de Dapper através da base genérica de contrato compartilhada.
/// </summary>
public sealed class DapperSmokeTests(
    ITestOutputHelper helper
) : DapperSmokeTestsBase<MySqlConnectionMock>(helper);
