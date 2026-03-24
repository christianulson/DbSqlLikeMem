using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared scalar temporal fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade temporal escalar compartilhados entre execucoes mock e container.
/// </summary>
public abstract class ScalarTemporalTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies scalar temporal functions, current-time predicates, and ordered reads for the current provider.
    /// PT: Verifica funcoes temporais escalares, predicados de tempo atual e leituras ordenadas para o provedor atual.
    /// </summary>
    [Fact]
    public void ScalarTemporalMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = RunScalarTemporalMatrix(serviceTest, users);
            Assert.True(resultMock.DateAdd > resultMock.DateScalar);
            Assert.True(resultMock.DateAdd > resultMock.CurrentTimestamp);
            Assert.Equal(3, resultMock.WhereCount);
            Assert.Equal("Aaron", resultMock.OrderedName);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = RunScalarTemporalMatrix(serviceTestContainer, users);
                    Assert.Equal(resultMock, resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private static (DateTime DateScalar, DateTime CurrentTimestamp, DateTime DateAdd, int WhereCount, string OrderedName) RunScalarTemporalMatrix<TConnection>(
        QueryServiceTest<TConnection> serviceTest,
        string users)
        where TConnection : DbConnection
    {
        var dateScalar = Convert.ToDateTime(serviceTest.RunDateScalar(), CultureInfo.InvariantCulture);
        var currentTimestamp = Convert.ToDateTime(serviceTest.RunTemporalCurrentTimestamp(), CultureInfo.InvariantCulture);
        var dateAdd = Convert.ToDateTime(serviceTest.RunTemporalDateAdd(), CultureInfo.InvariantCulture);
        var whereCount = Convert.ToInt32(serviceTest.RunTemporalNowWhere(users), CultureInfo.InvariantCulture);
        var orderedName = Convert.ToString(serviceTest.RunTemporalNowOrderBy(users), CultureInfo.InvariantCulture) ?? string.Empty;

        Assert.NotEqual(default, dateScalar);
        Assert.NotEqual(default, currentTimestamp);
        Assert.NotEqual(default, dateAdd);

        return (dateScalar, currentTimestamp, dateAdd, whereCount, orderedName);
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
