using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.TemporaryTable;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared temporary-table scenario.
/// PT: Executa testes de fidelidade Db2 para o cenario compartilhado de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(Get(Db2DbVersions.Default, _ => new Db2DbMock(_) { ThreadSafe = true })),
    Db2ConnectionFactory.Create
    )
{
}
