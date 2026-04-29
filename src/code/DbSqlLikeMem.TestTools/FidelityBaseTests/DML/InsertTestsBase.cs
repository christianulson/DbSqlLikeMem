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
    [FidelityFact]
    public Task InsertSingleTest()
        => RunInsertCountTest(1);

    /// <summary>
    /// EN: Verifies the parameter insert benchmark persists one row for the current provider.
    /// PT: Verifica se o benchmark de insert parametrizado persiste uma linha para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ParameterInsertSingleTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertParameterUsersServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that ten sequential inserts persist ten rows for the current provider.
    /// PT: Verifica se dez inserts sequenciais persistem dez linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public Task InsertBatch10Test()
        => RunInsertCountTest(10);

    /// <summary>
    /// EN: Verifies that one hundred sequential inserts persist one hundred rows for the current provider.
    /// PT: Verifica se cem inserts sequenciais persistem cem linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public Task InsertBatch100Test()
        => RunInsertCountTest(100);

    /// <summary>
    /// EN: Verifies that one hundred parallel inserts persist one hundred rows for the current provider.
    /// PT: Verifica se cem inserts paralelos persistem cem linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task InsertBatch100ParallelTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertParallelUsersServiceTest>(100);
    }

    /// <summary>
    /// EN: Verifies that a single insert reports a valid affected-row count for the current provider.
    /// PT: Verifica se um insert unico retorna uma contagem valida de linhas afetadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task RowCountAfterInsertTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, InsertRowCountUsersServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that inserting a row while omitting NOT NULL columns with defaults persists the provider defaults.
    /// PT: Verifica se inserir uma linha omitindo colunas NOT NULL com default persiste os valores padrao do provedor.
    /// </summary>
    [FidelityFact]
    public async Task InsertDefaultColumnsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, InsertDefaultsUsersServiceTest>())
            .Should()
            .BeEquivalentTo(new
            {
                affected = 1,
                name = "Alice",
                isActive = true,
                balance = 0m
            });
    }

    /// <summary>
    /// EN: Verifies that nullable columns with defaults are filled while nullable columns without defaults remain null.
    /// PT: Verifica se colunas anulaveis com default sao preenchidas enquanto colunas anulaveis sem default permanecem nulas.
    /// </summary>
    [FidelityFact]
    public async Task InsertNullableColumnsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertNullabilityScenario, InsertNullableColumnsServiceTest>())
            .Should()
            .BeEquivalentTo(new
            {
                requiredNoDefault = 10,
                nullableWithDefault = 7,
                nullableNoDefault = (int?)null
            });
    }

    /// <summary>
    /// EN: Verifies that omitting a required NOT NULL column without a default causes the provider to reject the insert.
    /// PT: Verifica se omitir uma coluna NOT NULL obrigatoria sem default faz o provedor rejeitar o insert.
    /// </summary>
    [FidelityFact]
    public async Task InsertNotNullWithoutDefaultTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertNullabilityScenario, InsertNotNullWithoutDefaultServiceTest>())
            .Should()
            .BeTrue();
    }

    /// <summary>
    /// EN: Verifies that inserts starting from a custom id persist the expected key range and row names for the current provider.
    /// PT: Verifica se inserts iniciando em um id customizado persistem a faixa de chaves e os nomes esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
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

