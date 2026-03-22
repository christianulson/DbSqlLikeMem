using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public abstract class SelectTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this method.
    /// </summary>
    [Fact]
    public void SelectByPkTest()
    {
        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new SelectByPKServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario("Users");
        var resultMock = serviceTest.RunTest();
        serviceTest.DropScenario("Users");

        if (Environment.GetEnvironmentVariable("RUN_CONTAINER_TESTS") == "true")
        {
            using var connContainer = connectionContainer("");// TestConfig.ConnectionString);
            connContainer.Open();
            var testScenarioContainer = new SelectTableScenario<T2>(dialect);
            var serviceTestContainer = new SelectByPKServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTest.CreateScenario("Users");
            var resultContainer = serviceTestContainer.RunTest();
            serviceTestContainer.DropScenario("Users");

            Assert.Equal(resultMock, resultContainer);
        }
    }
}
