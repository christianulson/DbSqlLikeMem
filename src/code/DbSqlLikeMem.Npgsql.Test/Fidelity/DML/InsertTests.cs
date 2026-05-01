using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared insert workflows.
/// PT-br: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    static () => new NpgsqlConnectionMock(Db),
    s => new NpgsqlConnection(s)
    )
{
    private static readonly NpgsqlDbMock Db = new() { ThreadSafe = true };
}
