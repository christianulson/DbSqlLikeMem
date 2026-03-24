using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de sequence.
/// </summary>
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
}
