using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.DDL;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.DDL;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared table scenarios.
/// PT: Executa testes de fidelidade do Db2 para os cenarios compartilhados de tabela.
/// </summary>
public class TableTests(
    ITestOutputHelper helper
    ) : TableTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(Get(Db2DbVersions.Default, _ => new Db2DbMock(_) { ThreadSafe = true })),
    Db2ConnectionFactory.Create
    )
{
}
