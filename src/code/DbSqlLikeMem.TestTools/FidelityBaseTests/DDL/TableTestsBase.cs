using DbSqlLikeMem.TestTools.DDL;

namespace DbSqlLikeMem.TestTools.Tests.DDL;

/// <summary>
/// EN: Provides shared DDL fidelity tests for table scenarios across mock and container runs.
/// PT: Fornece testes de fidelidade DDL compartilhados para cenarios de tabela entre mock e container.
/// </summary>
public abstract class TableTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that table creation works for the current provider.
    /// PT: Verifica se a criacao de tabela funciona para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CreateTableTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<CreateTableScenario, CreateTableServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that table creation with a foreign key works for the current provider.
    /// PT: Verifica se a criacao de tabela com chave estrangeira funciona para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CreateTableWithFKTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<CreateTableWithFKScenario, CreateTableWithFKServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that a table created with a foreign key accepts a valid referenced insert for the current provider.
    /// PT: Verifica se uma tabela criada com chave estrangeira aceita um insert referenciado valido para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task InsertInTableWithFKTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<TableWithFKScenario, InsertInTableWithFKServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that table dropping works for the current provider.
    /// PT: Verifica se a remocao de tabela funciona para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task DropTableTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<DropTableScenario, DropTableServiceTest>();
    }
}

