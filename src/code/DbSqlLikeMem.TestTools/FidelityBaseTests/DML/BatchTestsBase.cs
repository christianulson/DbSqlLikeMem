using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared batch fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de batch compartilhados entre execucoes mock e container.
/// </summary>
public abstract class BatchTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a batch insert of ten rows persists the expected count for the current provider.
    /// PT: Verifica se um insert em lote de dez linhas persiste a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchInsert10Test()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchInsertServiceTest>(10)).Should().Be(10);
    }

    /// <summary>
    /// EN: Verifies that a batch insert of one hundred rows persists the expected count for the current provider.
    /// PT: Verifica se um insert em lote de cem linhas persiste a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchInsert100Test()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchInsertServiceTest>(100)).Should().Be(100);
    }

    /// <summary>
    /// EN: Verifies that batched single-row inserts persist the expected row count for the current provider.
    /// PT: Verifica se inserts unitarios em lote persistem a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchRowCountInBatchTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchRowCountInServiceTest>()).Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that a mixed batch keeps reads and writes consistent for the current provider.
    /// PT: Verifica se um lote misto mantem leituras e escritas consistentes para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchMixedReadWriteTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchMixedReadWriteServiceTest>()).Should().Be("Alice");
    }

    /// <summary>
    /// EN: Verifies that a scalar batch keeps the expected row count and second value for the current provider.
    /// PT: Verifica se um lote escalar mantem a contagem esperada de linhas e o segundo valor para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchScalarTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchScalarServiceTest>())
            .Should()
            .BeEquivalentTo(new object?[] { 2, "Bob" });
    }

    /// <summary>
    /// EN: Verifies that a non-query batch keeps the expected final row count for the current provider.
    /// PT: Verifica se um lote sem consulta mantem a contagem final esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchNonQueryTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchNonQueryServiceTest>()).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a batch transaction control flow keeps the committed table name visible for the current provider.
    /// PT: Verifica se um fluxo de controle transacional em batch mantém o nome da tabela confirmado visivel para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchTransactionControlTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchTransactionControlServiceTest>() as string)
            .Should().StartWithEquivalentOf("Users");
    }

    /// <summary>
    /// EN: Verifies that a batch reader can iterate through multiple result sets for the current provider.
    /// PT: Verifica se um leitor em lote pode iterar por multiplos conjuntos de resultados para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchReaderMultiResultTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<InsertUsersScenario, BatchReaderMultiResultServiceTest>()).Should().Be("Alice");
    }
}
