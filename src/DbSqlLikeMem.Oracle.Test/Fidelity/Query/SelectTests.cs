using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared primary-key select scenario.
/// PT: Executa testes de fidelidade do Oracle para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
