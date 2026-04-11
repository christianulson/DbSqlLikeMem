using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared parser benchmark fidelity tests for representative SQL snippets.
/// PT: Fornece testes de fidelidade compartilhados para benchmarks de parser sobre trechos SQL representativos.
/// </summary>
public abstract class ParseTestsBase
{
    /// <summary>
    /// EN: Verifies the simple SELECT parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de SELECT simples conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseSimpleSelectTest()
        => RunParseScenario(ParseServiceTest.RunParseSimpleSelect, 8);

    /// <summary>
    /// EN: Verifies the complex JOIN parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de JOIN complexo conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseComplexJoinTest()
        => RunParseScenario(ParseServiceTest.RunParseComplexJoin, 25);

    /// <summary>
    /// EN: Verifies the INSERT RETURNING parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de INSERT RETURNING conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseInsertReturningTest()
        => RunParseScenario(ParseServiceTest.RunParseInsertReturning, 10);

    /// <summary>
    /// EN: Verifies the ON CONFLICT DO UPDATE parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de ON CONFLICT DO UPDATE conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseOnConflictDoUpdateTest()
        => RunParseScenario(ParseServiceTest.RunParseOnConflictDoUpdate, 17);

    /// <summary>
    /// EN: Verifies the JSON extract parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de extracao JSON conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseJsonExtractTest()
        => RunParseScenario(ParseServiceTest.RunParseJsonExtract, 4);

    /// <summary>
    /// EN: Verifies the string aggregate WITHIN GROUP parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de agregacao de strings WITHIN GROUP conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseStringAggregateWithinGroupTest()
        => RunParseScenario(ParseServiceTest.RunParseStringAggregateWithinGroup, 12);

    /// <summary>
    /// EN: Verifies the auto-dialect TOP/LIMIT/FETCH parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de TOP/LIMIT/FETCH auto-dialect conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseAutoDialectTopLimitFetchTest()
        => RunParseScenario(ParseServiceTest.RunParseAutoDialectTopLimitFetch, 15);

    /// <summary>
    /// EN: Verifies the multi-statement batch parser benchmark counts the expected tokens.
    /// PT: Verifica se o benchmark de parser de lote com multiplas instrucoes conta os tokens esperados.
    /// </summary>
    [Fact]
    public void ParseMultiStatementBatchTest()
        => RunParseScenario(ParseServiceTest.RunParseMultiStatementBatch, 26);

    private static void RunParseScenario(Func<int> action, int expectedCount)
        => Assert.Equal(expectedCount, action());
}
