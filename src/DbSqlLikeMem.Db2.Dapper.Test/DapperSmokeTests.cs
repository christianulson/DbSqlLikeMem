using System.Data.Common;

namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Provides provider-specific Dapper smoke coverage through the shared contract base.
/// PT: Fornece cobertura smoke específica do provedor de Dapper através da base de contrato compartilhada.
/// </summary>
public sealed class DapperSmokeTests(
    ITestOutputHelper helper
) : DapperSupportTestsBase(helper)
{
    /// <summary>
    /// EN: Creates an opened mocked connection for the current provider.
    /// PT: Cria uma conexão mock aberta para o provedor atual.
    /// </summary>
    protected override DbConnection CreateOpenConnection()
    {
        var connection = new Db2ConnectionMock();
        connection.Open();
        return connection;
    }
}
