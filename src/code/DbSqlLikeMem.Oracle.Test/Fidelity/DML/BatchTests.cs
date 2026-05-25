using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared batch workflows.
/// PT-br: Executa testes de fidelidade Oracle para os fluxos compartilhados de batch.
/// </summary>
public class BatchTests(
    ITestOutputHelper helper
    ) : BatchTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
