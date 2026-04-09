using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.DML;

/// <summary>
/// EN: Runs DB2 fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade DB2 para os fluxos compartilhados de sequence.
/// </summary>
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(),
    Db2ConnectionFactory.Create
    )
{
}
