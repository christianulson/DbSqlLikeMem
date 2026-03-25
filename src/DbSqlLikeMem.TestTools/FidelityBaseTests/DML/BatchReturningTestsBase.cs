using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared batch RETURNING fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de batch RETURNING compartilhados entre execucoes mock e container.
/// </summary>
public abstract class BatchReturningTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that an INSERT RETURNING batch persists one row and returns one reader row for the current provider.
    /// PT: Verifica se um batch INSERT RETURNING persiste uma linha e retorna uma linha no reader para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchReturningInsertTest()
        => Assert.Equal(
            1,
            RunReturningComparison(
                (service, tableName) => service.RunReturningInsert(tableName),
                (service, tableName) => service.RunReturningInsert(tableName)));

    private TResult RunReturningComparison<TResult>(
        Func<BatchServiceTest<T>, string, TResult> runMock,
        Func<BatchServiceTest<T2>, string, TResult> runContainer)
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunReturningScenario(connMock, users, uId, runMock);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunReturningScenario(connContainer, users, uId, runContainer);
            Assert.Equal(resultMock, resultContainer);
        }

        return resultMock;
    }

    private TResult RunReturningScenario<TConnection, TResult>(
        TConnection connection,
        string users,
        string uId,
        Func<BatchServiceTest<TConnection>, string, TResult> runner)
        where TConnection : DbConnection
    {
        var testScenario = new InsertUsersScenario<TConnection>(dialect);
        var serviceTest = new BatchServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = BuildTableName(users, uId);
            return runner(serviceTest, tableName);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private string BuildTableName(string users, string uId)
        => dialect.Provider == ProviderId.Oracle
            ? users.ToLowerInvariant()
            : $"{users}_{uId}";

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
