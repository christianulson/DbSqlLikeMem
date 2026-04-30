using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared sequence fidelity tests for create and advance workflows across mock and container runs.
/// PT: Fornece testes de fidelidade de sequence compartilhados para fluxos de criacao e avancar entre mock e container.
/// </summary>
public abstract class SequenceTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a created sequence returns the expected first and second values for the current provider.
    /// PT: Verifica se uma sequence criada retorna os valores esperado primeiro e segundo para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceNextValuesTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<SequenceScenario, DmlMutationSequenceServiceTest, long[]>(
            (service, args) => service.RunSequenceNextValuesAsync(args));

        _ = new long[] { 10L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies that sequence values can be consumed by inserts and keep the expected row range for the current provider.
    /// PT: Verifica se valores de sequence podem ser consumidos por inserts e mantem a faixa esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceInsertRoundTripTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SequenceScenario, UsersScenario, DmlMutationSequenceInsertRoundTripServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that sequence expressions can be used directly inside inserts for the current provider.
    /// PT: Verifica se expressoes de sequence podem ser usadas diretamente dentro de inserts para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceInsertExpressionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SequenceScenario, UsersScenario, DmlMutationSequenceInsertExpressionServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the current sequence value follows the last consumed value for the current provider.
    /// PT: Verifica se o valor corrente da sequence acompanha o ultimo valor consumido para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceCurrentValueTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SequenceScenario, DmlMutationSequenceCurrentValueServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that a sequence value can be projected inside a SELECT for the current provider.
    /// PT: Verifica se um valor de sequence pode ser projetado dentro de um SELECT para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceSelectProjectionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SequenceScenario, DmlMutationSequenceSelectProjectionServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that a filtered sequence query advances sequence values in execution order for the current provider.
    /// PT: Verifica se uma consulta filtrada com sequence avanca os valores da sequence na ordem de execucao para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceExpressionFilterTest()
    {
        object?[][] initialData = [[(1, "Ana")]];
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        var result = await testService.RunTestAsync<SequenceScenario, UsersScenario, SequenceExpressionFilterServiceTest, long[]>(
            (service, args) => service.RunSequenceExpressionFilterAsync(args));

        _ = new long[] { 10L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies that sequence values can participate in CASE and WHERE logic inside a single query for the current provider.
    /// PT: Verifica se valores de sequence podem participar de logica CASE e WHERE dentro de uma unica consulta para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceCaseWhereMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (SequenceCaseWhereMatrixThrowsNotSupported)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<SequenceScenario, DmlMutationSequenceCaseWhereMatrixServiceTest>())
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<SequenceScenario, DmlMutationSequenceCaseWhereMatrixServiceTest>();
    }

    /// <summary>
    /// EN: Gets whether the sequence CASE and WHERE matrix is expected to be unsupported for the current provider.
    /// PT: Obtem se a matriz de CASE e WHERE de sequence deve ser tratada como sem suporte para o provedor atual.
    /// </summary>
    protected virtual bool SequenceCaseWhereMatrixThrowsNotSupported => false;

    /// <summary>
    /// EN: Verifies that sequence values can be combined with temporal expressions inside a single query for the current provider.
    /// PT: Verifica se valores de sequence podem ser combinados com expressoes temporais dentro de uma unica consulta para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceTemporalMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SequenceScenario, DmlMutationSequenceTemporalMatrixServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that sequence-generated keys can participate in a join aggregate workflow for the current provider.
    /// PT: Verifica se chaves geradas por sequence podem participar de um fluxo agregado com join para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SequenceJoinAggregateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SequenceScenario, UsersOrdersScenario, DmlMutationSequenceJoinAggregateServiceTest>();
    }
}

