 using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared DML fidelity tests for insert workflows across mock and container runs.
/// PT: Fornece testes de fidelidade DML compartilhados para fluxos de insert entre mock e container.
/// </summary>
public abstract class InsertTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a single insert persists one row for the current provider.
    /// PT: Verifica se um insert unico persiste uma linha para o provedor atual.
    /// </summary>
    [Fact]
    public Task InsertSingleTest()
        => RunInsertCountTest(1);

    /// <summary>
    /// EN: Verifies the parameter insert benchmark persists one row for the current provider.
    /// PT: Verifica se o benchmark de insert parametrizado persiste uma linha para o provedor atual.
    /// </summary>
    [Fact]
    public async Task ParameterInsertSingleTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertParameterUsersServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that ten sequential inserts persist ten rows for the current provider.
    /// PT: Verifica se dez inserts sequenciais persistem dez linhas para o provedor atual.
    /// </summary>
    [Fact]
    public Task InsertBatch10Test()
        => RunInsertCountTest(10);

    /// <summary>
    /// EN: Verifies that one hundred sequential inserts persist one hundred rows for the current provider.
    /// PT: Verifica se cem inserts sequenciais persistem cem linhas para o provedor atual.
    /// </summary>
    [Fact]
    public Task InsertBatch100Test()
        => RunInsertCountTest(100);

    /// <summary>
    /// EN: Verifies that one hundred parallel inserts persist one hundred rows for the current provider.
    /// PT: Verifica se cem inserts paralelos persistem cem linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task InsertBatch100ParallelTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertParallelUsersServiceTest>(100);
    }

    /// <summary>
    /// EN: Verifies that a single insert reports a valid affected-row count for the current provider.
    /// PT: Verifica se um insert unico retorna uma contagem valida de linhas afetadas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task RowCountAfterInsertTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertRowCountUsersServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that inserts starting from a custom id persist the expected key range and row names for the current provider.
    /// PT: Verifica se inserts iniciando em um id customizado persistem a faixa de chaves e os nomes esperados para o provedor atual.
    /// </summary>
    [Fact]
    public async Task InsertCustomStartIdTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertUsersServiceTest>(3, 10, 3);
    }

    private async Task RunInsertCountTest(int rowCount)
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertUsersServiceTest>(rowCount);
    }
}
