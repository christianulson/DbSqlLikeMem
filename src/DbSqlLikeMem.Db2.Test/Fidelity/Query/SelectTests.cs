using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;
#if NET462
using DB2Connection = IBM.Data.DB2.iSeries.iDB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared primary-key select scenario.
/// PT: Executa testes de fidelidade do Db2 para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(),
    s => new DB2Connection(s)
    )
{
}
