using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared typed-field and function workflows.
/// PT-br: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
    /// <summary>
    /// EN: Skipped: @@SPID varies per connection on real Azure.
    /// PT-br: Ignorado: @@SPID varia por conexao no Azure real.
    /// </summary>
    [FidelityFact(Skip = "@@SPID varies per connection on real Azure")]
    public new async Task SqlServerSessionFunctionsTest() => await base.SqlServerSessionFunctionsTest();
}
