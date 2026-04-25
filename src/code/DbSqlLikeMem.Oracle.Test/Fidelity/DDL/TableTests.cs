using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.DDL;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.DDL;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared table scenarios.
/// PT: Executa testes de fidelidade do Oracle para os cenarios compartilhados de tabela.
/// </summary>
public class TableTests(
    ITestOutputHelper helper
    ) : TableTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(Get(OracleDbVersions.Default, _ => new OracleDbMock(_) { ThreadSafe = true })),
    s => new OracleConnection(s)
    )
{
}
