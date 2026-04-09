using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared insert workflows.
/// PT: Executa testes de fidelidade Oracle para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
