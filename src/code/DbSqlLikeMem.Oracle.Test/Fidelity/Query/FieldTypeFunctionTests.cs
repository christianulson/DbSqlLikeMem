using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared typed-field and function workflows.
/// PT-br: Executa testes de fidelidade Oracle para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
    /// <summary>
    /// EN: Skipped: Oracle driver throws InvalidCastException on real container.
    /// PT-br: Ignorado: O driver Oracle lanca InvalidCastException no container real.
    /// </summary>
    [FidelityFact(Skip = "Oracle driver throws InvalidCastException on real container")]
    public override Task MathFunctionsTest() => base.MathFunctionsTest();

    /// <summary>
    /// EN: Skipped: Oracle driver throws InvalidCastException on real container.
    /// PT-br: Ignorado: O driver Oracle lanca InvalidCastException no container real.
    /// </summary>
    [FidelityFact(Skip = "Oracle driver throws InvalidCastException on real container")]
    public override Task MathTranscendentalFunctionsTest() => base.MathTranscendentalFunctionsTest();
}
