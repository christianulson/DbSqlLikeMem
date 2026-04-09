using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.TemporaryTable;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared temporary-table scenario.
/// PT: Executa testes de fidelidade Oracle para o cenario compartilhado de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
