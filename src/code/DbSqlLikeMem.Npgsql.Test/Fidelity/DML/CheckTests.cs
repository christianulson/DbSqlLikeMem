using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared check-constraint workflows.
/// PT-br: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de restricao check.
/// </summary>
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    static () => new NpgsqlConnectionMock(Db),
    s => new NpgsqlConnection(s)
    )
{
    private static readonly NpgsqlDbMock Db = new() { ThreadSafe = true };
}
