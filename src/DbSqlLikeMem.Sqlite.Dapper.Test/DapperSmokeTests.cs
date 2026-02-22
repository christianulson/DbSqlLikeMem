using System.Data.Common;

namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Provides provider-specific Dapper smoke coverage through the shared contract base.
/// PT: Fornece cobertura smoke específica do provedor de Dapper através da base de contrato compartilhada.
/// </summary>
public sealed class DapperSmokeTests : DapperSupportTestsBase
{
    /// <summary>
    /// EN: Creates an opened mocked connection for the current provider.
    /// PT: Cria uma conexão mock aberta para o provedor atual.
    /// </summary>
    protected override DbConnection CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock();
        connection.Open();
        return connection;
    }
}
