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
    [Fact]
    public void CreateTableTest()
    {
        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new CreateTableScenario<T>();
        var serviceTest = new CreateTableServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.RunTest();
        serviceTest.DropScenario("Users");

        if (RunContainerTests.Value)
        {
            using var connContainer = connectionContainer("");// TestConfig.ConnectionString);
            connContainer.Open();
            var testScenarioContainer = new CreateTableScenario<T2>();
            var serviceTestContainer = new CreateTableServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.RunTest();
            serviceTestContainer.DropScenario("Users");
        }
    }

    /// <summary>
    /// EN: Verifies that table creation with a foreign key works for the current provider.
    /// PT: Verifica se a criacao de tabela com chave estrangeira funciona para o provedor atual.
    /// </summary>
    [Fact]
    public void CreateTableWithFKTest()
    {
        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new CreateTableWithFKScenario<T>();
        var serviceTest = new CreateTableWithFKServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.RunTest();
        serviceTest.DropScenario("Users");

        if (RunContainerTests.Value)
        {
            using var connContainer = connectionContainer("");// TestConfig.ConnectionString);
            connContainer.Open();
            var testScenarioContainer = new CreateTableWithFKScenario<T2>();
            var serviceTestContainer = new CreateTableWithFKServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.RunTest();
            serviceTestContainer.DropScenario("Users");
        }
    }

    /// <summary>
    /// EN: Verifies that table dropping works for the current provider.
    /// PT: Verifica se a remocao de tabela funciona para o provedor atual.
    /// </summary>
    [Fact]
    public void DropTableTest()
    {
        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new DropTableScenario<T>();
        var serviceTest = new DropTableServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario("Users");
        serviceTest.RunTest();

        if (RunContainerTests.Value)
        {
            using var connContainer = connectionContainer("");// TestConfig.ConnectionString);
            connContainer.Open();
            var testScenarioContainer = new DropTableScenario<T2>();
            var serviceTestContainer = new DropTableServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.CreateScenario("Users");
            serviceTestContainer.RunTest();
        }
    }
}
