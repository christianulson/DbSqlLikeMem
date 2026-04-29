using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared check-constraint workflows.
/// PT: Executa testes de fidelidade Oracle para os fluxos compartilhados de restricao check.
/// </summary>
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    static () => new OracleConnectionMock(Db),
    s => new OracleConnection(s)
    )
{
    private static readonly OracleDbMock Db = new() { ThreadSafe = true };
}
