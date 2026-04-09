using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared insert workflows.
/// PT: Executa testes de fidelidade Db2 para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(),
    Db2ConnectionFactory.Create
    )
{
}
