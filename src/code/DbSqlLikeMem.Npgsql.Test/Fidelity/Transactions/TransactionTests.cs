using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Transactions;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared transaction workflows.
/// PT-br: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de transacao.
/// </summary>
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
}
