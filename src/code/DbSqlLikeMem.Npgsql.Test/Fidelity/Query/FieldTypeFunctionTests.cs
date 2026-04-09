using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs PostgreSQL / Npgsql fidelity tests for the shared typed-field and function workflows.
/// PT: Executa testes de fidelidade PostgreSQL / Npgsql para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
}
