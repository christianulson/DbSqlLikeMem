using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared string-aggregation fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de agregacao de strings compartilhados entre execucoes mock e container.
/// </summary>
public abstract class StringAggregateTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    private static readonly object?[] SeedUsersVariants = [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob")];
    private static readonly object?[] SeedUsersSummary = [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")];

    /// <summary>
    /// EN: Verifies ordered, distinct, custom-separator, and large-group string aggregation for the current provider.
    /// PT: Verifica agregacao de strings ordenada, distinta, com separador customizado e em grupo grande para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregationVariantsTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersVariants],
            async (s, a) =>
            {
                var plain = await s.RunStringAggregateAsync(a);
                var ordered = await s.RunStringAggregateOrderedAsync(a);
                var distinct = await s.RunStringAggregateDistinctAsync(a);
                var separator = await s.RunStringAggregateCustomSeparatorAsync(a);
                var largeGroup = await s.RunStringAggregateLargeGroupAsync(a);
                return (plain, ordered, distinct, separator, largeGroup);
            });

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies plain string aggregation returns the expected result for the current provider.
    /// PT: Verifica se a agregacao simples de strings retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersVariants],
            (s, a) => s.RunStringAggregateAsync(a));

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies ordered string aggregation returns the expected result for the current provider.
    /// PT: Verifica se a agregacao ordenada de strings retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregateOrderedTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersVariants],
            (s, a) => s.RunStringAggregateOrderedAsync(a));

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies distinct string aggregation returns the expected result for the current provider.
    /// PT: Verifica se a agregacao distinta de strings retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregateDistinctTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersVariants],
            (s, a) => s.RunStringAggregateDistinctAsync(a));

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies custom-separator string aggregation returns the expected result for the current provider.
    /// PT: Verifica se a agregacao de strings com separador customizado retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregateCustomSeparatorTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersVariants],
            (s, a) => s.RunStringAggregateCustomSeparatorAsync(a));

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies large-group string aggregation returns the expected result for the current provider.
    /// PT: Verifica se a agregacao de strings em grupo grande retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregateLargeGroupTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersVariants],
            (s, a) => s.RunStringAggregateLargeGroupAsync(a));

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies string aggregation summary matrix returns the expected result for the current provider.
    /// PT: Verifica se a matriz resumo de agregacao de strings retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregateSummaryMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersSummary],
            async (s, a) =>
            {
                var summary = (ValueTuple<string?, int, int, int>?)await s.RunStringAggregateSummaryMatrixAsync(a);
                summary?.Item1.Should().NotBeNull();
                summary?.Item2.Should().Be(5);
                summary?.Item3.Should().Be(4);
                summary?.Item4.Should().Be(2);
                return summary;
            });

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies grouped string aggregation matrix returns the expected result for the current provider.
    /// PT: Verifica se a matriz agrupada de agregacao de strings retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregateGroupCaseMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersSummary],
            async (s, a) =>
            {
                var groupCase = (int?)await s.RunStringAggregateGroupCaseMatrixAsync(a);
                groupCase.Should().Be(2);
                return groupCase;
            });

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies string aggregation together with total, distinct, and repeated-name counts for the current provider.
    /// PT: Verifica agregacao de strings junto com contagens total, distinta e de nomes repetidos para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregationSummaryMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersSummary],
            async (s, a) =>
            {
                var summary = (ValueTuple<string?, int, int, int>?)await s.RunStringAggregateSummaryMatrixAsync(a);
                summary?.Item1.Should().NotBeNull();
                summary?.Item2.Should().Be(5);
                summary?.Item3.Should().Be(4);
                summary?.Item4.Should().Be(2);
                return summary;
            });

        result.Should().NotBeNull();
    }

    /// <summary>
    /// EN: Verifies grouped string aggregation with CASE, COALESCE, and distinct counts for the current provider.
    /// PT: Verifica agregacao agrupada de strings com CASE, COALESCE e contagens distintas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task StringAggregationGroupCaseMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SeedUsersSummary],
            async (s, a) =>
            {
                var groupCase =  (int?)await s.RunStringAggregateGroupCaseMatrixAsync(a);
                groupCase.Should().Be(2);
                return groupCase;
            });

        result.Should().Be(2);
    }

    private async Task<object?> RunFidelityTestAsync<TScenario, TServiceTest>(
        object?[][] initialData,
        Func<TServiceTest, object[], Task<object?>> runTest,
        params object[] args)
        where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TServiceTest>(runTest, args);
    }
}
