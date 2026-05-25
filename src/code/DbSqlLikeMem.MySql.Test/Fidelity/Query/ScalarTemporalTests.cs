using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared scalar temporal workflow.
/// PT-br: Executa testes de fidelidade MySQL para o fluxo escalar temporal compartilhado.
/// </summary>
public class ScalarTemporalTests(
    ITestOutputHelper helper
    ) : ScalarTemporalTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
    /// <inheritdoc />
    protected override TimeSpan TemporalComparisonTolerance => TimeSpan.FromSeconds(60);
}
