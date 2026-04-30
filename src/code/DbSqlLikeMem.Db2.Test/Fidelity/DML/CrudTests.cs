using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared CRUD workflows.
/// PT: Executa testes de fidelidade Db2 para os fluxos compartilhados de CRUD.
/// </summary>
[FidelityNativeClientSkip]
public class CrudTests(
    ITestOutputHelper helper
    ) : CrudTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(Get(Db2DbVersions.Default, _ => new Db2DbMock(_) { ThreadSafe = true })),
    Db2ConnectionFactory.Create
    )
{
}

