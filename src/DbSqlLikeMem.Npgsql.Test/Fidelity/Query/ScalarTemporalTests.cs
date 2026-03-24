using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Npgsql fidelity tests for the shared scalar temporal workflow.
/// PT: Executa testes de fidelidade Npgsql para o fluxo escalar temporal compartilhado.
/// </summary>
public class ScalarTemporalTests(
    ITestOutputHelper helper
    ) : ScalarTemporalTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
}
