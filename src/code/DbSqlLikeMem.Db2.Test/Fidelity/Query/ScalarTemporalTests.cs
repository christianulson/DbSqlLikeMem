using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared scalar temporal workflow.
/// PT: Executa testes de fidelidade Db2 para o fluxo escalar temporal compartilhado.
/// </summary>
public class ScalarTemporalTests(
    ITestOutputHelper helper
    ) : ScalarTemporalTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(),
    Db2ConnectionFactory.Create
    )
{
    /// <inheritdoc />
    protected override TimeSpan TemporalComparisonTolerance => TimeSpan.FromSeconds(60);
}
