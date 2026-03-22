using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared primary-key select scenario.
/// PT: Executa testes de fidelidade do PostgreSQL para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
}
