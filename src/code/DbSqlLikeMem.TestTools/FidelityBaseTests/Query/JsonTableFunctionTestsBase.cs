using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared fidelity tests for JSON table-valued functions (json_each, json_tree, OPENJSON) across mock and container runs.
/// PT-br: Fornece testes de fidelidade compartilhados para funções tabulares JSON (json_each, json_tree, OPENJSON) entre execuções mock e container.
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
    /// PT-br: Verifica json_each retorna keys e valores de array JSON.
    /// </summary>
    [FidelityFact]
    public async Task JsonEachFromArrayTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonEachFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
                async (s, a) => await s.RunJsonEachFromArrayAsync()))
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
            async (s, a) => await s.RunJsonEachFromArrayAsync());
    }

    /// <summary>
    /// EN: Verifies json_each returns keys, values and types from JSON object.
    /// PT-br: Verifica json_each retorna keys, values e types de objeto JSON.
    /// </summary>
    [FidelityFact]
    public async Task JsonEachFromObjectTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonEachFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
                async (s, a) => await s.RunJsonEachFromObjectAsync()))
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
            async (s, a) => await s.RunJsonEachFromObjectAsync());
    }

    /// <summary>
    /// EN: Verifies json_tree returns full tree structure including path and parent.
    /// PT-br: Verifica json_tree retorna estrutura completa incluindo path e parent.
    /// </summary>
    [FidelityFact]
    public async Task JsonTreeStructureTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonTreeFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
                async (s, a) => await s.RunJsonTreeStructureAsync()))
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
            async (s, a) => await s.RunJsonTreeStructureAsync());
    }

    /// <summary>
    /// EN: Verifies OPENJSON returns the expected rows from a JSON array for the current provider.
    /// PT-br: Verifica se OPENJSON retorna as linhas esperadas de um array JSON para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task OpenJsonArrayTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsOpenJsonFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
                async (s, a) => await s.RunOpenJsonArrayAsync()))
                .Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, List<Dictionary<string, object?>>>(
            async (s, a) => await s.RunOpenJsonArrayAsync());
    }
}
