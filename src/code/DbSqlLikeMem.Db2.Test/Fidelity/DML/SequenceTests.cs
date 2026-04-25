using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Tests.DML;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.DML;

/// <summary>
/// EN: Runs DB2 fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade DB2 para os fluxos compartilhados de sequence.
/// </summary>
[FidelityNativeClientSkip]
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(Get(Db2DbVersions.Default, _ => new Db2DbMock(_) { ThreadSafe = true })),
    Db2ConnectionFactory.Create
    )
{
    /// <summary>
    /// EN: Verifies NEXT VALUE FOR can be used inside a filtered DB2 query and advances in execution order.
    /// PT: Verifica se NEXT VALUE FOR pode ser usado dentro de uma consulta filtrada do DB2 e avanca na ordem de execucao.
    /// </summary>
    [FidelityFact]
    [Trait("Category", "ExtendedDb2Mock")]
    public async Task SequenceExpressions_ShouldReturnExpectedValues()
    {
        using var testService = new FidelityTestService<Db2ConnectionMock, DB2Connection>(
            () => new Db2ConnectionMock(),
            Db2ConnectionFactory.Create,
            new Db2ProviderSqlDialect(),
            [[(1, "Alice")]]);

        var result = await testService.RunTestAsync<SequenceScenario, UsersScenario, SequenceExpressionFilterServiceTest>() as long[];

        _ = new long[] { 10L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies DB2 sequence values remain session-local across two independent connections.
    /// PT: Verifica se os valores de sequence do DB2 permanecem locais a sessao em duas conexoes independentes.
    /// </summary>
    [FidelityFact]
    [Trait("Category", "ExtendedDb2Mock")]
    public async Task SequenceValues_ShouldBeSessionLocal()
    {
        using var testService = new FidelityTestService<Db2ConnectionMock, DB2Connection>(
            () => new Db2ConnectionMock(),
            Db2ConnectionFactory.Create,
            new Db2ProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceSessionLocalServiceTest>() as object[];
        Assert.NotNull(result);

        _ = new object?[] { 10L, 10L, 11L, 11L, 10L }.Should().Equal(result);
    }
}


