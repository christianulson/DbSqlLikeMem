using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.Transactions;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared transaction workflows.
/// PT: Executa testes de fidelidade Db2 para os fluxos compartilhados de transacao.
/// </summary>
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(),
    Db2ConnectionFactory.Create
    )
{
}
