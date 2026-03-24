using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared scalar temporal workflow.
/// PT: Executa testes de fidelidade Oracle para o fluxo escalar temporal compartilhado.
/// </summary>
public class ScalarTemporalTests(
    ITestOutputHelper helper
    ) : ScalarTemporalTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
