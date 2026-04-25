using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared fidelity tests for JSON table-valued functions (json_each, json_tree) across mock and container runs.
/// PT: Fornece testes de fidelidade compartilhados para funções tabulares JSON (json_each, json_tree) entre execuções mock e container.
/// </summary>
public abstract class JsonTableFunctionTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies json_each returns keys and values from JSON array.
    /// PT: Verifica json_each retorna keys e valores de array JSON.
    /// </summary>
    [FidelityFact]
    public async Task JsonEachFromArrayTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonEachFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (s, a) => (object?)await s.RunJsonEachFromArrayAsync()))
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            async (s, a) => (object?)await s.RunJsonEachFromArrayAsync());
    }

    /// <summary>
    /// EN: Verifies json_each returns keys, values and types from JSON object.
    /// PT: Verifica json_each retorna keys, values e types de objeto JSON.
    /// </summary>
    [FidelityFact]
    public async Task JsonEachFromObjectTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonEachFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (s, a) => (object?)await s.RunJsonEachFromObjectAsync()))
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            async (s, a) => (object?)await s.RunJsonEachFromObjectAsync());
    }

    /// <summary>
    /// EN: Verifies json_tree returns full tree structure including path and parent.
    /// PT: Verifica json_tree retorna estrutura completa incluindo path e parent.
    /// </summary>
    [FidelityFact]
    public async Task JsonTreeStructureTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonTreeFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                async (s, a) => (object?)await s.RunJsonTreeStructureAsync()))
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            async (s, a) => (object?)await s.RunJsonTreeStructureAsync());
    }
}

