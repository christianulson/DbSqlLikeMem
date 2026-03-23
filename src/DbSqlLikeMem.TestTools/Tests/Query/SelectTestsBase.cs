using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared query fidelity tests for primary-key lookup scenarios across mock and container runs.
/// PT: Fornece testes de fidelidade de consulta para cenarios de busca por chave primaria entre mock e container.
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
    /// EN: Verifies that primary-key selection returns the expected row for the current provider.
    /// PT: Verifica se a selecao por chave primaria retorna a linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectByPkTest()
    {
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new SelectByPKServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario("Users", uId);
        var resultMock = serviceTest.RunTest("Users", uId);
        serviceTest.DropScenario("Users", uId);

        if (RunContainerTests.Value
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var testScenarioContainer = new SelectTableScenario<T2>(dialect);
            var serviceTestContainer = new SelectByPKServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.CreateScenario("Users", uId);
            var resultContainer = serviceTestContainer.RunTest("Users", uId);
            serviceTestContainer.DropScenario("Users", uId);

            Assert.Equal(resultMock, resultContainer);
        }
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
