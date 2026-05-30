using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared typed-field and function workflows.
/// PT-br: Executa testes de fidelidade Db2 para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
[FidelityNativeClientSkip]
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(Get(Db2DbVersions.Default, _ => new Db2DbMock(_) { ThreadSafe = true })),
    Db2ConnectionFactory.Create
    )
{
    /// <summary>
    /// EN: Skipped: Various real DB2 container limitations.
    /// PT-br: Ignorado: Varias limitacoes do container Db2 real.
    /// </summary>
    [FidelityFact(Skip = "Various real DB2 container limitations")]
    public new async Task Db2AliasMathFunctionsTest() => await base.Db2AliasMathFunctionsTest();

    /// <summary>
    /// EN: Skipped: Various real DB2 container limitations.
    /// PT-br: Ignorado: Varias limitacoes do container Db2 real.
    /// </summary>
    [FidelityFact(Skip = "Various real DB2 container limitations")]
    public new async Task MathCotFunctionTest() => await base.MathCotFunctionTest();

    /// <summary>
    /// EN: Skipped: Various real DB2 container limitations.
    /// PT-br: Ignorado: Varias limitacoes do container Db2 real.
    /// </summary>
    [FidelityFact(Skip = "Various real DB2 container limitations")]
    public new async Task MathTranscendentalFunctionsTest() => await base.MathTranscendentalFunctionsTest();

    /// <summary>
    /// EN: Skipped: Various real DB2 container limitations.
    /// PT-br: Ignorado: Varias limitacoes do container Db2 real.
    /// </summary>
    [FidelityFact(Skip = "Various real DB2 container limitations")]
    public new async Task JsonQueryRootFragmentTest() => await base.JsonQueryRootFragmentTest();
}

