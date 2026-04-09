using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared upsert workflows.
/// PT: Executa testes de fidelidade Oracle para os fluxos compartilhados de upsert.
/// </summary>
public class UpsertTests(
    ITestOutputHelper helper
    ) : UpsertTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
