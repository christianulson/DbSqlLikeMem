using DbSqlLikeMem.TestTools.DDL;

namespace DbSqlLikeMem.TestTools.Tests.DDL;

/// <summary>
/// TODO: Add a summary for this class.
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
    /// TODO: Add a summary for this class.
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
    /// TODO: Add a summary for this class.
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
    /// TODO: Add a summary for this class.
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
